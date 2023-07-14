from util.multiqueue import NamedMultiQueue

class ZiggyQueueSubmitter:
    ''' `ZiggyQueueSubmitter` sends incoming job config files to the proper
    queue for their experiment. We have different queues for different registered
    experiments to allow us to prioritize different experiments at different
    times. By default, the longer queue is popped from first, not FIFO. '''

    def __init__(self, base, registry):
        self.q = NamedMultiQueue(base, [])
        self.registry = registry
        for k in self.registry.keys():
            self.q.add_queue(k)

    def submit(self, entry):
        job_name = entry["job_name"]
        self.q.get_queues()[job_name].push(entry)