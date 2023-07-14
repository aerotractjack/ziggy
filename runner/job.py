import subprocess
import json

# This is the system mapping of human-friendly job names to their respective
# python `Job` objects which Ziggy knows how to execute
ZiggyJobRegistry = {}

# Decorator function to register a new `Job` sublcass within Ziggy
def RegisterJob(job_name):
    def decorator(job_cls):
        ZiggyJobRegistry[job_name] = job_cls
        return job_cls
    return decorator

class Job:
    ''' `Job` interface defines steps for Ziggy to build and run a 
    job, such as training or predicting '''

    def __init__(self, device, config):
        self.device = device
        self.config = config

    def run(self): pass
    
    @classmethod
    def build_runner(cls, device, config):
        # Utility method to return the run function of a new instance
        # of this class. Used to submit jobs to dask
        return cls(device, config).run

@RegisterJob("example_pipeline")
class ExamplePipelineJob(Job):
    ''' This is a proof of concept pipeline to show we can use Ziggy to execute python code
    within our dev environment, input a config file, import necessary libraries, and record output '''

    def __init__(self, device, config):
        super().__init__(device, config)
        self.python = "/home/aerotract/miniconda3/envs/rv/bin/python3"
        self.src_code_path = "/home/aerotract/software/ziggy_pipelines/test_pipeline/test_pipeline.py"

    def run(self):
        cmd = [self.python, self.src_code_path, json.dumps(self.config)]
        result = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)
        return [result.stdout, result.stderr]

    