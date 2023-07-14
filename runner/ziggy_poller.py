from util.multiqueue import NamedMultiQueue, QueuePoller
from runner.ziggy_submitter import ZiggyQueueSubmitter
from datetime import datetime

class ZiggyQueuePoller(QueuePoller):
    ''' `ZiggyQueuePoller` is responsible for sending jobs from the pending process
    queue to the active, managed process queues in the `runner.manager.ZiggyGPUManager` object.
    This class also makes sure that any jobs which were cut short (such as if the server crashes)
    are added back into the process queue. '''

    def __init__(self, base, manager, registry):
        super().__init__(NamedMultiQueue(base, []))
        self.manager = manager
        self.registry = registry
        for job_name in self.registry.keys():
            self.q.add_queue(job_name)

    def push_inprocess_queue(self, inprocess_q):
        # Check the inprocess queue for old entries from crashed jobs. Add any 
        # into the current pending process queue
        for prev_job in inprocess_q.pop_all():
            entry = prev_job["entry"]
            job_name = entry["job_name"]
            self.q.get_queues()[job_name].push(entry)

    def submit_job_from_entry(self, entry):
        # Send the queue entry and registered job function to the manager to be run
        job_name = entry["job_name"]
        job_cls = self.registry[job_name]
        self.manager.run_job_managed(entry, job_cls)
        print("managed job submitted...")

    def poll_forever(self):
        # Poll the process queue and submit jobs as they come when the manager
        # has available devices
        for popped in super().poll_forever(self.manager.device_available_callback):
            now = str(datetime.now())
            print(now)
            if popped is None:
                continue
            self.submit_job_from_entry(popped["entry"])

