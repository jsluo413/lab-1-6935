import socket
import json
import time
import os
import sys
import json

from multiprocessing.connection import Client, Listener
from threading import Thread, Lock
from monitor_lib import get_cpu_status, get_memory_status
from calc_pi import calculate_pi



# --- Configuration ---
MANAGER_ADDRESS = ('10.128.0.2', 9999)
AUTH_KEY = b'secret-key'  # Shared secret for authentication


# Worker Function
def get_system_status():
    cpu_status = get_cpu_status()
    mem_status = get_memory_status()

    cpu_usage = float(cpu_status['lavg_1']) * 100  # Simplified CPU usage approximation
    mem_total = mem_status.get('MemTotal', 1)
    mem_free = mem_status.get('MemFree', 0) + mem_status.get('Buffers', 0) + mem_status.get('Cached', 0)
    mem_usage = (1 - mem_free / mem_total) * 100 if mem_total > 0 else 0

    return {'cpu': cpu_usage, 'mem': mem_usage}


def get_pi(num_terms):
    return calculate_pi(num_terms)

FUNCTION_MAP = {
    'get_system_status': get_system_status,
    'calculate_pi': get_pi,
}

# --- Worker's RPC Server for Tasks ---
def run_task_server(address):
    with Listener(address, authkey=AUTH_KEY) as listener:
        print(f"Worker listening for tasks on {listener.address}")
        while True:
            try:
                with listener.accept() as conn:
                    request = conn.recv()
                    function_name = request.get('function_name')
                    args = request.get('args', [])
                    kwargs = request.get('kwargs', {})

                    if function_name in FUNCTION_MAP:
                        try:
                            result = FUNCTION_MAP[function_name](*args, **kwargs)
                            response = {'status': 'SUCCESS', 'result': result}
                        except Exception as e:
                            response = {'status': 'ERROR', 'message': str(e)}
                    else:
                        response = {'status': 'ERROR', 'message': f"Unknown function: {function_name}"}

                    conn.send(response)
            except Exception as e:
                print(f"WORKER: Error during task processing: {e}")
                
if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python worker.py <host> <port>")
        sys.exit(1)

    host = sys.argv[1]
    port = int(sys.argv[2])
    worker_address = (host, port)

    # Register with Manager
    try:
        with Client(MANAGER_ADDRESS, authkey=AUTH_KEY) as conn:
            reg_data = {'host': host, 'port': port}
            conn.send(reg_data)
            response = conn.recv()
            if response.get('status') == 'SUCCESS':
                print(f"WORKER: Successfully registered with manager at {MANAGER_ADDRESS}")
            else:
                print(f"WORKER: Registration failed: {response.get('message')}")
                sys.exit(1)
    except Exception as e:
        print(f"WORKER: Could not connect to manager at {MANAGER_ADDRESS}: {e}")
        sys.exit(1)

    # Start Task Server
    run_task_server(worker_address)