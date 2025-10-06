import json
import time
import sys
import argparse
import os
from collections import deque
from multiprocessing.connection import Listener, Client
from threading import Thread, Lock

# --- Configuration ---
MANAGER_ADDRESS = ('10.128.0.2', 9999)
AUTH_KEY = b'secret-key'  # Shared secret for authentication

# IDLE_TIMEOUT_SECONDS = 120  # 2 minutes

WORKER_REGISTRY = {}
REGISTRY_LOCK = Lock()

# --- Job Tracking ---
JOB_HISTORY = []
JOB_LOCK = Lock()
JOB_QUEUE_LOCK = Lock()



def call_rpc(address, function_name, *args, **kwargs):
    """Sends an RPC request"""
    request = {'function_name': function_name, 'args': args, 'kwargs': kwargs}
    try:
        with Client(address, authkey=AUTH_KEY) as conn:
            conn.send(request)
            response = conn.recv()
            return response
    except Exception as e:
        return {'status': 'ERROR', 'message': f"Communication failed: {e}"}


def handle_worker_registration(conn):
    """Handle a single worker registration on its own thread."""
    try:
        reg_data = conn.recv()
        with REGISTRY_LOCK:
            # Assign a unique worker ID
            if not hasattr(run_registry_server, "_next_worker_id"):
                run_registry_server._next_worker_id = 1
            worker_id = f"worker {run_registry_server._next_worker_id}"
            run_registry_server._next_worker_id += 1

            print(f"REGISTRY: Registering {worker_id}")
            WORKER_REGISTRY[worker_id] = {
                'address': (reg_data['host'], reg_data['port']),
                'last_seen': time.time(),
                'busy': False
            }
        conn.send({'status': 'SUCCESS', 'worker_id': worker_id})
    except Exception as e:
        # Try to inform the client if possible
        try:
            conn.send({'status': 'ERROR', 'message': str(e)})
        except Exception:
            pass
        print(f"REGISTRY: Error during registration: {e}")
    finally:
        try:
            conn.close()
        except Exception:
            pass


def run_registry_server():
    """Listens for worker registrations concurrently."""
    with Listener(MANAGER_ADDRESS, authkey=AUTH_KEY) as listener:
        print(f"Manager listening for registrations on {listener.address}")
        while True:
            try:
                conn = listener.accept()
                # Hand off to a dedicated thread so multiple workers can register concurrently
                Thread(target=handle_worker_registration, args=(conn,), daemon=True).start()
            except Exception as e:
                print(f"REGISTRY: Error accepting registration: {e}")

def select_lowest_cpu(workers):
    """Selects the worker with the lowest CPU usage."""
    if not workers:
        return None
    return min(workers, key=lambda w: w['cpu'])


def select_round_robin(workers):
    """Selects workers in a round-robin fashion."""
    if not workers:
        return None
    # Maintain a static index
    if not hasattr(select_round_robin, "_rr_index"):
        select_round_robin._rr_index = 0
    idx = select_round_robin._rr_index % len(workers)
    select_round_robin._rr_index += 1
    return workers[idx]


def assign_task(job_id, worker_id, addr, meta=None, job_queue=None, job_queue_lock=None):
    print(f"JOB {job_id}: Assignning to {worker_id} ({addr[0]}:{addr[1]})")
    with REGISTRY_LOCK:
        if worker_id in WORKER_REGISTRY:
            # Mark worker as busy
            WORKER_REGISTRY[worker_id]['busy'] = True
            
    start_ts = time.time()
    response = call_rpc(addr, 'calculate_pi', num_terms=20_000_000)
    end_ts = time.time()
    duration = end_ts - start_ts
    # result
    result_text = (response.get('result')if response.get(
        'status') == 'SUCCESS'else response.get('message'))
    print(
        f"JOB {job_id}: Completed by {worker_id} -> {result_text} in {duration:.2f}s")
    # Mark worker as free
    with REGISTRY_LOCK:
        if worker_id in WORKER_REGISTRY:
            WORKER_REGISTRY[worker_id]['busy'] = False

    # Record job
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
    if response.get('status') != 'SUCCESS':
        error_message = response.get('message')
        should_remove_worker = isinstance(error_message, str) and (
            'Communication failed' in error_message or 'timeout' in error_message.lower())
        if job_queue is not None and job_queue_lock is not None:
            with job_queue_lock:
                job_queue.appendleft(job_id)
            print(f"JOB {job_id}: Re-queued due to failure: {error_message}")
        if should_remove_worker:
            with REGISTRY_LOCK:
                removed = WORKER_REGISTRY.pop(worker_id, None) is not None
            if removed:
                print(
                    f"JOB {job_id}: Removed {worker_id} after communication failure.")
    return response

def manage_workers(strategy, jobs, interval=5.0):
    """Manage workers and launch a fixed number of jobs at a given interval.

    - strategy: 'lowest_cpu' or 'round_robin'
    - jobs: total number of jobs to run
    - interval: seconds between consecutive job launches
    """
    with JOB_LOCK:
        JOB_HISTORY.clear()
    # last launch timestamp
    last_launch = 0.0
    job_queue = deque(range(1, jobs + 1))
    job_threads = []
    print_interval_header_done = False

    while True:
        worker_statuses = []
        with REGISTRY_LOCK:
            current_workers = list(WORKER_REGISTRY.items())

        if not current_workers:
            if not print_interval_header_done:
                print("\n--- Waiting For Workers ---")
                print_interval_header_done = True
            print("MONITOR: No active workers found.")
            time.sleep(2)
            continue

        print_interval_header_done = False
        print("\n--- Monitoring Active Workers ---")
        for worker_id, info in current_workers:
            # system status monitoring
            response = call_rpc(info['address'], 'get_system_status')
            if response['status'] == 'SUCCESS':
                with REGISTRY_LOCK:
                    if worker_id in WORKER_REGISTRY:
                        WORKER_REGISTRY[worker_id]['last_seen'] = time.time()
                status = response['result']
                busy_status = "BUSY" if WORKER_REGISTRY.get(worker_id, {}).get('busy', False) else "IDLE"
                status['status'] = busy_status
                worker_statuses.append(
                    {**status, 'address': info['address'], 'worker_id': worker_id})
                print(
                    f"{busy_status}  | {worker_id} | CPU Load: {status['cpu']:.2f}% | Memory: {status['mem']:.1f}%")
            else:
                with REGISTRY_LOCK:
                    removed = WORKER_REGISTRY.pop(worker_id, None) is not None
                if removed:
                    print(f"{worker_id} | Status Check Failed: {response['message']} (removed from registry)")



        # Launch new jobs until number of jobs reached
        now = time.time()
        should_launch = (now - last_launch) >= float(interval)

        if worker_statuses and should_launch:
            with JOB_QUEUE_LOCK:
                job_id = job_queue.popleft() if job_queue else None
            if job_id is None:
                time.sleep(1)
                continue
            # idle detection
            # select only idle workers
            idle_workers = [ w for w in worker_statuses if not WORKER_REGISTRY.get(w['worker_id'], {}).get('busy', False)]
            available_workers = idle_workers if idle_workers else worker_statuses
            best_worker = (select_lowest_cpu(available_workers)if strategy ==
                           'lowest_cpu'else select_round_robin(worker_statuses)) # for round robin, consider all workers (not just idle)

            print("\n--- Load Balancing ---")
            worker_id = best_worker['worker_id']
            print(
                f"Selected worker: {worker_id} with {best_worker['cpu']:.1f}% CPU."
            )

            last_launch = now
            t = Thread(
                target=assign_task,
                args=(job_id, worker_id, best_worker['address'], {
                      'cpu': best_worker['cpu'], 'mem': best_worker['mem']}, job_queue, JOB_QUEUE_LOCK),
            )
            t.start()
            job_threads.append(t)

        with JOB_LOCK:
            completed = sum(1 for r in JOB_HISTORY if r['status'] == 'SUCCESS')

        if completed >= jobs:
            break

        time.sleep(1)
    # Wait for all job threads to finish
    for thread in job_threads:
        thread.join()

    summarize_batch(strategy=strategy, jobs=jobs, interval=interval)
    # print("All jobs completed. Manager will shut down in 10 seconds.")
    # time.sleep(10)
    # os._exit(0)
    # print("Manager exiting.")


def summarize_batch(strategy, jobs, interval):
    with JOB_LOCK:
        records = list(JOB_HISTORY)
    success_records = [r for r in records if r['status'] == 'SUCCESS']
    error_records = [r for r in records if r['status'] != 'SUCCESS']

    durations = [r['duration'] for r in success_records]
    if durations:
        start_times = [r['start_ts'] for r in success_records]
        end_times = [r['end_ts'] for r in success_records]
        makespan = max(end_times) - min(start_times)
    else:
        start_times = []
        end_times = []
        makespan = 0.0

    per_worker = {}
    for r in success_records:
        per_worker[r['worker_id']] = per_worker.get(r['worker_id'], 0) + 1

    print("\n=== Summary ===")
    print(f"Strategy: {strategy}")
    print(f"Jobs Launched: {jobs}")
    print(f"Jobs Completed: {len(success_records)}")
    print(f"Interval: {interval}s")
    if durations:
        print(f"Average Duration: {sum(durations)/len(durations):.2f}s")
        print(
            f"Min Duration: {min(durations):.2f}s | Max Duration: {max(durations):.2f}s")
        print(f"Makespan (first start to last end): {makespan:.2f}s")
    else:
        print("No successful jobs to compute timing statistics.")
    print("Jobs per worker:")

    for wid, count in sorted(per_worker.items()):
        print(f"  - {wid}: {count}")
    # print errors
    if error_records:
        print("Errors:")
        for e in error_records:
            print(f"  - Job {e['job_id']} on {e['worker_id']}: {e['error']}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Distributed Manager")
    parser.add_argument('strategy', nargs='?', choices=['lowest_cpu', 'round_robin'], default='lowest_cpu',
                        help="Worker selection strategy")
    parser.add_argument('-n', '--jobs', type=int, required=True,
                        help="Total number of jobs to launch before shutdown")
    parser.add_argument('-i', '--interval', type=float, default=5.0,
                        help="Seconds between job launches (default 5.0)")

    args = parser.parse_args()
    user_strategy = args.strategy

    if args.jobs <= 0:
        print("Jobs must be a positive integer.")
        sys.exit(1)

    print(
        f"Manager starting: strategy={user_strategy}, jobs={args.jobs}, interval={args.interval}s")

    registry_thread = Thread(target=run_registry_server, daemon=True)
    registry_thread.start()
    manage_workers(strategy=user_strategy,
                   jobs=args.jobs, interval=args.interval)
