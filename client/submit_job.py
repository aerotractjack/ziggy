import requests
from pathlib import Path
import yaml
import json

'''
Simple utility script to submit a job or jobs (as .json or .yaml files) to
 Ziggy over the local network. It must be run from within the office.

This is the main/only entrypoint for scientists to submit jobs to Ziggy

The input config file(s) must all contain a top-level key-value pair `job_name`
 which is known by Ziggy and registered to the runner.job.ZiggyJobRegistry object

Usage example: submitting a single file
    (env_name) $ python3 /path/to/submit_job.py /path/to/input_1.json 

Usage example: submitting multiple files
    (env_name) $ python3 /path/to/submit_job.py /path/to/input_1.yaml /path/to/input_2.json 
'''

url = "http://127.0.0.1:5000/submit"

class ZiggyClient:

    def __init__(self, submit_url=url):
        self.submit_url = url

    def read_yaml(self, path):
        with open(path, 'r') as yaml_file:
            return yaml.safe_load(yaml_file)

    def read_json(self, path):
        with open(path, 'r') as json_file:
            return json.loads(json_file.read())

    def ziggy_submit_from_dict(self, data):
        resp = requests.post(self.submit_url, json=data)
        return resp

    def ziggy_submit_from_path(self, path):
        fn = self.read_yaml
        if path.suffix == ".json":
            fn = self.read_json
        contents = fn(path)
        return self.ziggy_submit_from_dict(contents)

    def submit(self, data_or_path):
        if not isinstance(data_or_path, list):
            data_or_path = [data_or_path]
        for dop in data_or_path:
            if isinstance(dop, dict):
                self.ziggy_submit_from_dict(dop)
            else:
                dop = Path(dop).absolute()
                if dop.is_file():
                    self.ziggy_submit_from_path(dop)
                    continue
                for f in dop.iterdir():
                    self.ziggy_submit_from_path(f)

    @classmethod
    def Submit(cls, config):
        obj = cls()
        obj.submit(config)

if __name__ == "__main__":
    import sys
    from pathlib import Path
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--config", required=True, type=str, nargs="+",
        help="Path(s) to job config file(s) to submit, or path(s) to directory(s) containing files")
    args = parser.parse_args()

    ZiggyClient.Submit(args.config)

    sys.exit(0)