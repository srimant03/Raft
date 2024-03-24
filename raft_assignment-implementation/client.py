import threading
import time
from random import randint, random
import zmq
import signal
import sys
import time
from threading import Thread, Lock
import os
import utils
import datetime
from queue import Queue
import traceback

signal.signal(signal.SIGINT, signal.SIG_DFL)

#ip = sys.argv[1]
#port = sys.argv[2]
#ip_address = ip + ":" + port
partitions = "[['127.0.0.1:5001', '127.0.0.1:5002', '127.0.0.1:5003'], ['127.0.0.1:5004', '127.0.0.1:5005', '127.0.0.1:5006']]"
partitions = eval(partitions)

leader_info = None

def connect_to_node(ip, port):
    context = zmq.Context()
    socket = context.socket(zmq.REQ)
    socket.connect(f"tcp://localhost:{port}")
    return socket

def get_leader_info():
    if leader_info is None:
        port = partitions[0][0].split(":")[1]
    else:
        port = leader_info[1]
    socket = connect_to_node("localhost", port)
    socket.send_multipart([b'LEADER', b'INFO'])
    message = socket.recv_multipart()
    leader_index = int(message[0].decode('utf-8'))
    leader_ip = partitions[0][leader_index].split(":")[0]
    leader_port = partitions[0][leader_index].split(":")[1]
    leader_info = (leader_ip, leader_port)
    return leader_ip, leader_port

while True:
    print("1) Get Leader")
    print("2) SET Operation")
    print("3) GET Operation")
    print("4) Exit")
    choice = int(input("Enter choice: "))
    if choice == 1:
        leader_ip, leader_port = get_leader_info()
        print(f"Leader IP: {leader_ip}")
        print(f"Leader Port: {leader_port}")
        
    elif choice == 2:
        key = input("Enter key: ")
        value = input("Enter value: ")
        leader_ip, leader_port = get_leader_info()
        socket = connect_to_node(leader_ip, leader_port)
        socket.send_multipart([b'SET', key.encode('utf-8'), value.encode('utf-8')])
        message = socket.recv()
        print(f"Received: {message}")
        
    elif choice == 3:
        key = input("Enter key: ")
        leader_ip, leader_port = get_leader_info()
        socket = connect_to_node(leader_ip, leader_port)
        socket.send_multipart([b'GET', key.encode('utf-8')])
        message = socket.recv()
        print(f"Received: {message}")
    elif choice == 4:
        break
    else:
        print("Invalid Choice")
        continue

