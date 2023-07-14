from util.multiqueue import NamedMultiQueue

class ZiggyGPUManager:
    ''' `ZiggyGPUManager` is responsible for managing our system, ensuring:
        - 1 process per GPU is running at a time
        - in-process jobs are recorded so they are not lost if the system crashes
        - previously failed jobs are pushed into the job queue 
        - busy/available status of GPUs is recorded '''

    def __init__(self, devices, inprocess_q, client):
        self.devices = {d: None for d in devices}
        self.inprocess_q = inprocess_q
        self.client = client

    def check_devices_finished(self):
        # For each device, check the `future` object's status. If it is
        # finished, clear its entry in the dict and grab the result
        for d in self.devices.keys():
            fut = self.devices[d]
            if fut is None:
                continue
            if fut.status == "finished":
                print(fut.result())
                self.devices[d] = None
                self.inprocess_q.get_queues()[d].pop()

    def get_available_device(self):
        # Check all devices, and return the key of an available device, or None if
        # no devices are available
        ret = None
        for d in self.devices.keys():
            if self.devices[d] is None:
                ret = d
        return ret

    def device_available_callback(self):
        # Used by the `runner.ziggy_poller.ZiggyQueuePoller` to pause its polling if no GPUs
        # are available right now. It performs a check of all GPUs and returns whether or not
        # there is an available GPU
        self.check_devices_finished()
        return self.get_available_device() is not None

    def run_job_managed(self, entry, job_cls):
        # Submit a job to a dask worker
        device = self.get_available_device()
        job_runner_fn = job_cls.build_runner(device, entry["config"])
        fut = self.client.submit(job_runner_fn)
        self.devices[device] = fut
        self.inprocess_q.get_queues()[device].push(entry)
        print(f"submitted to manager on device {device}...")
    