import socket
import json
import time

from multiprocessing.connection import Listener
from threading import Thread, Lock


class WorkerState:
    def __init__(self, conn, addr):
        self.conn = conn
        self.addr = addr
        self.cpu_usage = None # cpu
        self.memory_usage = None # memory
        self.last_seen = time.time()  # timestamp of last seen worker
        
    def update_state(self, cpu, memory, timestamp):
        self.cpu_usage = cpu
        self.memory_usage = memory
        self.last_seen = timestamp or time.time()
        
    def is_idle(self, threshold=10):
        return (time.time() - self.last_seen) > threshold
        
class WorkerInfo:
    def __init__(self, host, port=6000):
        self.host = host
        self.port = port
        self.address = (self.host, self.port)
        self.listener = Listener(self.address, authkey=b'peekaboo')
        self.workers = {}
        self.lock = Lock()
        self.worker_id = 0

    def register_worker(self, state):
        with self.lock:
            worker_id = self.worker_id
            self.workers[worker_id] = WorkerState(conn, addr)
            print(f"Worker {worker_id} registered from {addr}")
            self.worker_id += 1
                        
class RPCHandler:
    def __init__(self, worker_info):
        
        
        
    

