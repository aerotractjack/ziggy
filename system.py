from runner.ziggy_poller import ZiggyQueuePoller
from runner.job import ZiggyJobRegistry
from runner.manager import ZiggyGPUManager
from util.multiqueue import NamedMultiQueue
from dask.distributed import Client

''' 
This is the main entrypopint to kicking off the job polling/execution system

Usage example:
    (env_name) $ python3 system.py
'''

if __name__ == "__main__":

    # Define system configuration
    devices = ["cuda0", "cuda1"]
    inprocess_q_path = "/home/aerotract/.ziggyq_inprocess"
    q_path = "/home/aerotract/.ziggyq"

    # Setup dask connection (dask scheduler must be running on Ziggy)
    client = Client()

    # Create Ziggy objects needed to run system
    inprocess_q = NamedMultiQueue(inprocess_q_path, devices)
    mngr = ZiggyGPUManager(devices, inprocess_q, client)
    poller = ZiggyQueuePoller(q_path, mngr, ZiggyJobRegistry)

    # Load any previously failed jobs into the queue
    poller.push_inprocess_queue(mngr.inprocess_q)

    # Wait for new jobs and submit them as they come, forever
    for foo in poller.poll_forever():
        pass