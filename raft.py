from random import randint, random
import sys
import os
import re
import socket
import select
import utils
import traceback
import zmq
import signal

from requests import delete
from hashtable import HashTable
from threading import Thread, Lock
import time
from queue import Queue
from random import shuffle
from commit_log import CommitLog
from pathlib import Path
from consistent_hashing import ConsistentHashing
from queue import Queue
import mmh3

signal.signal(signal.SIGINT, signal.SIG_DFL)


class Raft:
    def __init__(self, ip, port, partitions):
        self.ip = ip
        self.port = port
        self.partiitons = eval(partitions)
        self.database = HashTable()
        self.commit_log = CommitLog()
        self.conns = [[None]*len(self.partiitons[i]) for i in range(len(self.partiitons))]
        self.cluster_index = -1
        self.server_index = -1

        for i in range(len(self.partiitons)):
            cluster = self.partiitons[i]
            for j in range(len(cluster)):
                ip, port = cluster[j].split(':')
                port = int(port)
                if(ip == self.ip and port == self.port):
                    self.cluster_index = i
                    self.server_index = j
                else:
                    self.conns[i][j] = (ip, port)

        self.current_term = 1
        self.voted_for = -1
        self.votes = set()

        u = len(self.partiitons[self.cluster_index])
        self.state = 'FOLLOWER' if len(self.partiitons[self.cluster_index]) > 1 else 'LEADER'
        self.leader_id = -1
        self.commit_index = 0
        self.next_index = [0]*u
        self.match_index = [0]*u
        self.election_period_ms = randint(1000, 5000)
        self.rpc_period_ms = 3000
        self.election_timeout = -1
        self.rpc_timeout = [-1]*u

        print("Ready....")

    def init(self):
        self.set_election_timeout()
        utils.run_thread(func=self.on_election_timeout, args=())
        utils.run_thread(func=self.leader_send_append_entries, args=())
    
    def set_election_timeout(self, timeout=None):
        if timeout:
            self.election_timeout = timeout
        else:
            self.election_timeout = time.time() + randint(self.election_period_ms, 2*self.election_period_ms)/1000
    
    def on_election_timeout(self):
        while True:
            if time.time() > self.election_timeout and (self.state == 'FOLLOWER' or self.state == 'CANDIDATE'):

                print("Timeout....")
                self.set_election_timeout()
                self.start_election()
    
    def start_election(self):
        print("Starting election....")
        self.state = 'CANDIDATE'
        self.voted_for = self.server_index
        self.current_term += 1
        self.votes.add(self.server_index)

        threads = []
        for i in range(len(self.partiitons[self.cluster_index])):
            if i != self.server_index:
                t = utils.run_thread(func=self.request_vote, args=(i,))
                threads+=[t]
        
        for t in threads:
            t.join()

        return True

    def request_vote(self, server):
        last_index, last_term = self.commit_log.get_last_index_term()

        while True:
            print(f"Requesting vote from {server}....")

            if(self.state == 'CANDIDATE' and time.time() < self.election_timeout):
                ip, port = self.conns[self.cluster_index][server]
                msg = f"VOTE-REQ {self.server_index} {self.current_term} {last_term} {last_index}"
                resp = utils.send_and_recv_no_retry(msg, ip, port, timeout=self.rpc_period_ms/1000)

                if resp:
                    vote_rep = re.match('^VOTE-REP ([0-9]+) ([0-9\-]+) ([0-9\-]+)$', resp)
                    if vote_rep:
                        server, curr_term, voted_for = vote_rep.groups()
                        server = int(server)
                        curr_term = int(curr_term)
                        voted_for = int(voted_for)
                        self.process_vote_reply(server, curr_term, voted_for)
                        break
            else:
                break

    def process_vote_request(self, server, term, last_term, last_index):
        print(f"Processing vote request from {server} {term}....")

        if (term > self.current_term):
            self.step_down(term)
        
        self_last_index, self_last_term = self.commit_log.get_last_index_term()

        if(term == self.current_term) and (self.voted_for == server or self.voted_for == -1) and (last_term > self_last_term or (last_term == self_last_term and last_index >= self_last_index)):
            self.voted_for = server
            self.state = 'FOLLOWER'
            self.set_election_timeout()

        return f"VOTE-REP {self.server_index} {self.current_term} {self.voted_for}"
    
    def step_down(self, term):
        print(f"Stepping down....")
        self.state = 'FOLLOWER'
        self.current_term = term
        self.voted_for = -1
        self.set_election_timeout()

    def process_vote_reply(self, server, term, voted_for):
        print(f"Processing vote reply from {server} {term}....")

        if term > self.current_term:
            self.step_down(term)
        
        if term == self.current_term and self.state == 'CANDIDATE':
            if voted_for == self.server_index:
                self.votes.add(server)
        
        if len(self.votes) > len(self.partiitons[self.cluster_index])//2:
            self.state = 'LEADER'
            self.leader_id = self.server_index
            print(f"{self.cluster_index}-{self.server_index} is the leader....")
            print(f"{self.votes}-{self.current_term}")

    def leader_send_append_entries(self):
        print("Sending append entries....")
        while True:
            if self.state == 'LEADER':
                self.append_entries()
                last_index, _ = self.commit_log.get_last_index_term()
                self.commit_index = last_index

    def append_entries(self):
        res = Queue()
        for j in range(len(self.partiitons[self.cluster_index])):
            if j != self.server_index:
                utils.run_thread(func=self.send_append_entries_request, args=(j, res,))

        if len(self.partiitons[self.cluster_index]) > 1:
            cnts = 0
            while True:
                res.get(block=True)
                cnts += 1
                if cnts > (len(self.partiitons[self.cluster_index])/2) - 1:
                    return
        else:
            return
    
    def send_append_entries_request(self, server, res=None):
        print(f"Sending append entries to {server}....")
        prev_idx = self.next_index[server] - 1
        log_slice = self.commit_log.read_logs_start_end(prev_idx)

        if prev_idx == -1:
            prev_term = 0
        else:
            if(len(log_slice) > 0):
                prev_term = log_slice[0][0]
                log_slice = log_slice[1:] if len(log_slice) > 1 else []
            else:
                prev_term = 0
                log_slice = []
        
        msg = f"APPEND-REQ {self.server_index} {self.current_term} {prev_idx} {prev_term} {str(log_slice)} {self.commit_index}"

        while True:
            if self.state == 'LEADER':
                ip, port = self.conns[self.cluster_index][server]
                resp = utils.send_and_recv_no_retry(msg, ip, port, timeout=self.rpc_period_ms/1000)
                if resp:
                    append_rep = re.match('^APPEND-REP ([0-9]+) ([0-9\-]+) ([0-9]+) ([0-9\-]+)$', resp)
                    if append_rep:
                        server, curr_term, flag, index = append_rep.groups()
                        server = int(server)
                        curr_term = int(curr_term)
                        flag = int(flag)
                        success = True if flag == 1 else False
                        index = int(index)
                        self.process_appned_reply(server, curr_term, success, index)
                        break
            else:
                break
        
        if res:
            res.put('OK')
        
    def process_append_requests(self, server, term, prev_idx, prev_term, logs, commit_index):
        print(f"Processing append entries from {server} {term}....")
        self.set_election_timeout()
        flag, index = 0, 0
        if term > self.current_term:
            self.step_down(term)
        
        if term == self.current_term:
            self.leader_id = server
            self_logs = self.commit_log.read_logs_start_end(prev_idx, prev_idx) if prev_idx != -1 else []

            success = prev_idx == -1 or (len(self_logs) > 0 and self_logs[0][0] == prev_term)

            if success:
                last_index, last_term = self.commit_log.get_last_index_term()
                if len(logs)>0 and last_term == logs[-1][0] and last_index == self.commit_index:
                    index = self.commit_index
                else:
                    index = self.store_enries(prev_idx,logs)
            
            flag = 1 if success else 0
        
        return f"APPEND-REP {self.server_index} {self.current_term} {flag} {index}"
    
    def process_append_reply(self, server, term, success, index):
        print(f"Processing append reply from {server} {term}....")
        if term > self.current_term:
            self.step_down(term)
        
        if self.state == 'LEADER' and term == self.current_term:
            if success:
                self.next_index[server] = index + 1
            else:
                self.next_index[server] = max(0, self.next_index[server] - 1)
                self.send_append_entries_request(server)

    def store_entries(self, prev_idx, leader_logs):
        commands = [f"{leader_logs[i][1]}" for i in range(len(leader_logs))]
        last_index, _ = self.commit_log.log_replace(self.current_term, commands, prev_idx+1)
        self.commit_index = last_index

        for command in commands:
            self.update_state_machine(command)
        
        return last_index
    

    def update_state_machine(self, command):
        set_ht = re.match('^SET ([^\s]+) ([^\s]+) ([0-9]+)$', command)
        if set_ht:
            key, value, req_id = set_ht.groups()
            req_id = int(req_id)
            self.ht.set(key=key, value=value, req_id=req_id)
    
    def handle_commands(self, msg, conn):
        set_ht = re.match('^SET ([^\s]+) ([^\s]+) ([0-9]+)$', msg)
        get_ht = re.match('^GET ([^\s]+) ([0-9]+)$', msg)
        vote_req = re.match('^VOTE-REQ ([0-9]+) ([0-9\-]+) ([0-9\-]+) ([0-9\-]+)$', msg)
        append_req = re.match('^APPEND-REQ ([0-9]+) ([0-9\-]+) ([0-9\-]+) ([0-9\-]+) (\[.*?\]) ([0-9\-]+)$', msg)
        if set_ht:
            output = "ko"
            try:
                key, value, req_id = set_ht.groups()
                req_id = int(req_id)
                node = mmh3.hash(key, signed=False) % len(self.partiitons)
                if self.cluster_index == node:
                    while True:
                        if self.state == 'LEADER':
                            last_index, _ = self.commit_log.log(self.current_term, msg)
                            while True:
                                if last_index == self.commit_index:
                                    break
                            
                            self.ht.set(key=key, value=value, req_id=req_id)
                            output = "ok"
                            break
                        else:
                            if self.leader_id != -1 and self.leader_id != self.server_index:
                                output = utils.send_and_recv_no_retry(msg, self.conns[node][self.leader_id][0], self.conns[node][self.leader_id][1], timeout=self.rpc_period_ms)

                                if output is not None:
                                    break
                            else:
                                output = "ko"
                                break
                else:
                    output = utils.send_and_recv(msg, self.conns[node][0][0], self.conns[node][0][1])

                    if output is None:
                        output = "ko"
            except Exception as e:
                traceback.print_exc(limit=1000)
        
        elif get_ht:
            output = "ko"
            try:
                key, _ = get_ht.groups()
                node = mmh3.hash(key, signed=False) % len(self.partiitons)
                if self.cluster_index == node:
                    while True:
                        if self.state == 'LEADER':
                            output = self.ht.get_value(key)
                            if output:
                                output = str(output)
                            else:
                                output = 'Error: Non existent key'
                            break
                        else:
                            if self.leader_id != -1 and self.leader_id != self.server_index:
                                output = utils.send_and_recv_no_retry(msg,
                                                                      self.conns[node][self.leader_id][0],
                                                                      self.conns[node][self.leader_id][1],
                                                                      timeout=self.rpc_period_ms)
                                if output is not None:
                                    break
                            else:
                                output = 'ko'
                                break
                else:
                    output = utils.send_and_recv(msg, self.conns[node][0][0], self.conns[node][0][1])
                    if output is None:
                        output = 'ko'
            except Exception as e:
                traceback.print_exc(limit=1000)
        
        elif vote_req:
            try:
                server, term, last_term, last_index = vote_req.groups()
                server = int(server)
                term = int(term)
                last_term = int(last_term)
                last_index = int(last_index)
                output = self.process_vote_request(server, term, last_term, last_index)
            except Exception as e:
                traceback.print_exc(limit=1000)

        elif append_req:
            try:
                server, curr_term, prev_idx, prev_term, logs, commit_index = append_req.groups()
                server = int(server)
                curr_term = int(curr_term)
                prev_idx = int(prev_idx)
                prev_term = int(prev_term)
                logs = eval(logs)
                commit_index = int(commit_index)
                output = self.process_append_requests(server, curr_term, prev_idx, prev_term, logs, commit_index)
            except Exception as e:
                traceback.print_exc(limit=1000)

        else:
            print("Hello1 - " + msg + " - Hello2")
            output = "Error: Invalid command"

        return output
    
    def process_request(self, conn):
        while True:
            try:
                msg = conn.recv(2048).decode('utf-8')
                if msg:
                    msg = msg.decode('utf-8')
                    print(f"Received {msg}....")
                    output = self.handle_commands(msg, conn)
                    conn.sendall(output.encode('utf-8'))
            except Exception as e:
                traceback.print_exc(limit=1000)
                print("Error processing request....")
                conn.close()
                break
    
    def listen_to_client(self):
        context = zmq.Context()
        socket = context.socket(zmq.REP)
        socket.bind(f"tcp://{self.ip}:{self.port}")
        print(f"Listening on {self.ip}:{self.port}....")
        while True:
            try:
                conn, _ = socket.recv_multipart()
                print(f"Received {conn}....")
                my_thread = Thread(target=self.process_request, args=(conn,))
                my_thread.daemon = True
                my_thread.start()
            except:
                print("Error listening to client....")
    
if __name__ == "__main__":
    ip, port, partitions = sys.argv[1], int(sys.argv[2]), sys.argv[3]
    raft = Raft(ip, port, partitions)
    utils.run_thread(func=raft.init, args=())
    raft.listen_to_client()

        



            
        


