import socket
import json
import time
import os
import sys
import json

from multiprocessing.connection import Client
from threading import Thread, Lock
from monitor_lib import get_cpu_status, get_memory_status
from calculate_pi import calculate_pi


