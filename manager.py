import json
import time
import sys
import argparse

from multiprocessing.connection import Listener, Client
from threading import Thread, Lock

# --- Configuration ---
MANAGER_ADDRESS = ('10.128.0.2', 9999)
WORKER_TIMEOUT_SECONDS = 60
AUTH_KEY = b'secret-key'  # Shared secret for authentication

IDLE_TIMEOUT_SECONDS = 120  # 2 minutes

WORKER_REGISTRY = {}
REGISTRY_LOCK = Lock()

# --- Job Tracking ---
JOB_HISTORY = []
JOB_LOCK = Lock()

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
                            'last_seen': time.time(),
                            'busy': False,
                            'last_task_start': None,
                            'last_task_end': time.time()
                        }
                    conn.send({'status': 'SUCCESS', 'worker_id': worker_id})
            except Exception as e:
                print(f"REGISTRY: Error during registration: {e}")
# --- Worker Selection Strategies ---
def select_lowest_cpu(workers):
    if not workers:
        return None
    return min(workers, key=lambda w: w['cpu'])

def select_round_robin(workers):
    if not workers:
        return None
    if not hasattr(manage_workers, "_rr_index"):
        manage_workers._rr_index = 0
    idx = manage_workers._rr_index % len(workers)
    manage_workers._rr_index += 1
    return workers[idx]


def assign_task(job_id, worker_id, addr, meta=None):
    with REGISTRY_LOCK:
        if worker_id in WORKER_REGISTRY:
            WORKER_REGISTRY[worker_id]['busy'] = True
            WORKER_REGISTRY[worker_id]['last_task_start'] = time.time()
    start_ts = time.time()
    response = call_rpc(addr, 'calculate_pi', num_terms=20_000_000)
    end_ts = time.time()
    duration = end_ts - start_ts
    print(f"ASSIGNMENT #{job_id}: Response -> {response.get('result') or response.get('message')} in {duration:.2f}s")
    with REGISTRY_LOCK:
        if worker_id in WORKER_REGISTRY:
            WORKER_REGISTRY[worker_id]['busy'] = False
            WORKER_REGISTRY[worker_id]['last_task_end'] = time.time()
    job_record = {
        'job_id': job_id,
        'worker_id': worker_id,
        'worker_addr': addr,
        'dispatched_cpu': (meta or {}).get('cpu'),
        'dispatched_mem': (meta or {}).get('mem'),
        'start_ts': start_ts,
        'end_ts': end_ts,
        'duration': duration,
        'status': response.get('status'),
        'error': response.get('message') if response.get('status') != 'SUCCESS' else None,
    }
    with JOB_LOCK:
        JOB_HISTORY.append(job_record)
    return response

# --- Main Manager Logic ---
def manage_workers(strategy, jobs=None, interval=5.0):
    """Manage workers and optionally launch a fixed number of jobs at a given interval.

    - strategy: 'lowest_cpu' or 'round_robin'
    - jobs: if None, runs continuously; otherwise launches `jobs` tasks and summarizes
    - interval: seconds between consecutive job launches
    """
    last_launch = 0.0
    launched = 0
    job_threads = []
    print_interval_header_done = False

    while True:
        worker_statuses = []
        with REGISTRY_LOCK:
            current_workers = list(WORKER_REGISTRY.items())

        if not current_workers:
            if not print_interval_header_done:
                print("\n--- Waiting For Workers ---")
            print("MONITOR: No active workers found.")
            time.sleep(2)
            continue

        print("\n--- Monitoring Active Workers ---")
        for worker_id, info in current_workers:
            response = call_rpc(info['address'], 'get_system_status')
            if response['status'] == 'SUCCESS':
                with REGISTRY_LOCK:
                    if worker_id in WORKER_REGISTRY:
                        WORKER_REGISTRY[worker_id]['last_seen'] = time.time()
                status = response['result']
                worker_statuses.append({**status, 'address': info['address'], 'worker_id': worker_id})
                print(f"{worker_id} | CPU Load: {status['cpu']:.2f} | Memory: {status['mem']:.1f}%")
            else:
                print(f"{worker_id} | Status Check Failed: {response['message']}")

        # idle detection
        now = time.time()
        with REGISTRY_LOCK:
            for wid, info in WORKER_REGISTRY.items():
                if not info['busy'] and info['last_task_end'] and (now - info['last_task_end'] > IDLE_TIMEOUT_SECONDS):
                    print(f"IDLE DETECTION: {wid} has been idle for over 2 minutes.")

        # Launch logic (continuous or batch)
        should_launch = (now - last_launch) >= float(interval)
        batch_mode = jobs is not None
        can_launch = True if not batch_mode else (launched < jobs)

        if worker_statuses and should_launch and can_launch:
            best_worker = (
                select_lowest_cpu(worker_statuses)
                if strategy == 'lowest_cpu'
                else select_round_robin(worker_statuses)
            )

            print("\n--- Load Balancing ---")
            worker_id = best_worker['worker_id']
            print(
                f"Selected worker: {worker_id} ({best_worker['address'][0]}:{best_worker['address'][1]}) with {best_worker['cpu']:.1f}% CPU."
            )

            launched += 1
            job_id = launched
            last_launch = now
            t = Thread(
                target=assign_task,
                args=(job_id, worker_id, best_worker['address'], {'cpu': best_worker['cpu'], 'mem': best_worker['mem']}),
                daemon=True,
            )
            t.start()
            job_threads.append(t)

        # Batch completion and summary
        if batch_mode and launched >= jobs:
            with JOB_LOCK:
                completed = len(JOB_HISTORY)
            if completed >= jobs:
                summarize_batch(strategy=strategy, jobs=jobs, interval=interval)
                return

        time.sleep(1)


def summarize_batch(strategy, jobs, interval):
    with JOB_LOCK:
        records = list(JOB_HISTORY)

    if not records:
        print("No job records to summarize.")
        return

    durations = [r['duration'] for r in records]
    start_times = [r['start_ts'] for r in records]
    end_times = [r['end_ts'] for r in records]
    makespan = max(end_times) - min(start_times)

    per_worker = {}
    for r in records:
        per_worker[r['worker_id']] = per_worker.get(r['worker_id'], 0) + 1

    errors = [r for r in records if r['status'] != 'SUCCESS']

    print("\n=== Batch Summary ===")
    print(f"Strategy: {strategy}")
    print(f"Jobs Launched: {jobs}")
    print(f"Interval: {interval}s")
    print(f"Average Duration: {sum(durations)/len(durations):.2f}s")
    print(f"Min Duration: {min(durations):.2f}s | Max Duration: {max(durations):.2f}s")
    print(f"Makespan (first start to last end): {makespan:.2f}s")
    print("Jobs per worker:")
    
    for wid, count in sorted(per_worker.items()):
        print(f"  - {wid}: {count}")
    if errors:
        print("Errors:")
        for e in errors:
            print(f"  - Job {e['job_id']} on {e['worker_id']}: {e['error']}")
        
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Distributed Manager")
    parser.add_argument('strategy', nargs='?', choices=['lowest_cpu', 'round_robin'], default='lowest_cpu',
                        help="Worker selection strategy")
    parser.add_argument('-n', '--jobs', type=int, default=None,
                        help="Number of jobs to launch (batch mode); default is continuous")
    parser.add_argument('-i', '--interval', type=float, default=5.0,
                        help="Seconds between job launches (default 5.0)")

    args = parser.parse_args()
    user_strategy = args.strategy

    if args.jobs is not None:
        print(f"Manager starting in BATCH mode: strategy={user_strategy}, jobs={args.jobs}, interval={args.interval}s")
    else:
        print(f"Manager starting in CONTINUOUS mode: strategy={user_strategy}, interval={args.interval}s")

    registry_thread = Thread(target=run_registry_server, daemon=True)
    registry_thread.start()
    manage_workers(strategy=user_strategy, jobs=args.jobs, interval=args.interval)
    
