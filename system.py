from runner.ziggy_poller import ZiggyQueuePoller
from runner.job import ZiggyJobRegistry
from runner.manager import ZiggyGPUManager
from util.multiqueue import NamedMultiQueue
from dask.distributed import Client

if __name__ == "__main__":
    client = Client()

    devices = ["cuda0", "cuda1"]
    inprocess_q_path = "/home/aerotract/.ziggyq_inprocess"
    q_path = "/home/aerotract/.ziggyq"

    inprocess_q = NamedMultiQueue(inprocess_q_path, devices)
    mngr = ZiggyGPUManager(devices, inprocess_q, client)
    poller = ZiggyQueuePoller(q_path, mngr, ZiggyJobRegistry)

    poller.push_inprocess_queue(mngr.inprocess_q)

    for foo in poller.poll_forever():
        pass