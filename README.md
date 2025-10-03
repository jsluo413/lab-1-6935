# lab-1-6935

Distributed manager/worker experiment for job scheduling.

Usage:

- Start the manager in batch mode to launch 20 jobs at 10s intervals:
  - `python3 manager.py lowest_cpu --jobs 20 --interval 10`
  - `python3 manager.py round_robin --jobs 20 --interval 10`

Compare the printed "Batch Summary" sections from both runs to evaluate distribution, average durations, and makespan.

Workers:
- Start workers with: `python3 worker.py <host> <port>`
