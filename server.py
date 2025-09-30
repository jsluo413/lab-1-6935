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
            
    def remove_worker(self, worker_id):
        with self.lock:
            if worker_id in self.workers:
                del self.workers[worker_id]
                print(f"Worker {worker_id} removed because of idle timeout") # delete idling workers
                
    def get_worker_states(self):
        with self.lock:
            w = self.workers.get(worker_id)
            if w:
            return {wid: (state.cpu_usage, state.memory_usage, state.last_seen) for wid, state in self.workers.items()}
            
class RPCHandler:
    def __init__(self, worker_info):
        
        
        
    

