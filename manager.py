import json
import time

from multiprocessing.connection import Listener, Client
from threading import Thread, Lock

# --- Configuration ---
MANAGER_ADDRESS = ('10.128.0.2', 9999)
WORKER_TIMEOUT_SECONDS = 60
AUTH_KEY = b'secret-key'  # Shared secret for authentication

WORKER_REGISTRY = {}
REGISTRY_LOCK = Lock()

# --- RPC Client for Calling Workers ---
def call_rpc(address, function_name, *args, **kwargs):
    """Sends an RPC request using multiprocessing.connection.Client."""
    request = {'function_name': function_name, 'args': args, 'kwargs': kwargs}
    try:
        with Client(address, authkey=AUTH_KEY) as conn:
            conn.send(request)
            response = conn.recv()
            return response
    except Exception as e:
        return {'status': 'ERROR', 'message': f"Communication failed: {e}"}
    
# --- Worker Registration ---
def run_registry_server():
    """Listens for worker registrations."""
    with Listener(MANAGER_ADDRESS, authkey=AUTH_KEY) as listener:
        print(f"Manager listening for registrations on {listener.address}")
        while True:
            try:
                with listener.accept() as conn:
                    reg_data = conn.recv()

                    with REGISTRY_LOCK:
                        if not hasattr(run_registry_server, "_next_worker_id"):
                            run_registry_server._next_worker_id = 1
                        worker_id = f"worker {run_registry_server._next_worker_id}"
                        run_registry_server._next_worker_id += 1

                        print(f"REGISTRY: Registering {worker_id}")
                        WORKER_REGISTRY[worker_id] = {
                            'address': (reg_data['host'], reg_data['port']),
                            'last_seen': time.time()
                        }
                    conn.send({'status': 'SUCCESS'})
            except Exception as e:
                print(f"REGISTRY: Error during registration: {e}")
                
# --- Main Manager Logic ---
def manage_workers():
    while True:
        print("\n--- Monitoring Active Workers ---")
        worker_statuses = []
        with REGISTRY_LOCK:
            current_workers = list(WORKER_REGISTRY.items())
            
        if not current_workers:
            print("MONITOR: No active workers found.")
            time.sleep(10)
            continue
        
        for worker_id, info in current_workers:
            response = call_rpc(info['address'], 'get_system_status')
            if response['status'] == 'SUCCESS':
                with REGISTRY_LOCK:
                    if worker_id in WORKER_REGISTRY:
                        WORKER_REGISTRY[worker_id]['last_seen'] = time.time()
                status = response['result']
                worker_statuses.append({**status, 'address': info['address']})
                print(f"✅ {worker_id} | CPU Load: {status['cpu']:.2f} | Memory: {status['mem']:.1f}%")
            else:
                print(f"❌ {worker_id} | Status Check Failed: {response['message']}")

        # 3. Load Balance & Assign Task
        if worker_statuses:
            best_worker = min(worker_statuses, key=lambda w: w['cpu'])
            print(f"\n--- Load Balancing ---")
            print(f"Idle worker found: {best_worker['address'][0]}:{best_worker['address'][1]} with {best_worker['cpu']:.1f}% CPU.")
            result = call_rpc(best_worker['address'], 'calculate_pi', digits=10000)
            print(f"ASSIGNMENT: Response -> {result.get('result') or result.get('message')}")
        
        time.sleep(10)
        
if __name__ == "__main__":
    registry_thread = Thread(target=run_registry_server, daemon=True)
    registry_thread.start()
    manage_workers()
    

