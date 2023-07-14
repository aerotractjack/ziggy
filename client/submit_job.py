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

def read_yaml(path):
    with open(path, 'r') as yaml_file:
        return yaml.safe_load(yaml_file)

def read_json(path):
    with open(path, 'r') as json_file:
        return json.loads(json_file.read())

def ziggy_submit_from_dict(data):
    resp = requests.post(url, json=data)
    return resp

def ziggy_submit_from_path(path):
    path = Path(path)
    fn = read_yaml
    if path.suffix == ".json":
        fn = read_json
    contents = fn(path)
    return ziggy_submit_from_dict(contents)

if __name__ == "__main__":
    import sys
    from pathlib import Path
    if len(sys.argv) == 1 or sys.argv[1] == "-h" or sys.argv[1] == "help":
        print("\nUSAGE:\n$ python3 submit_job.py /path/to/config/file/or/dir[.json|.yaml]")
        sys.exit(1)
    for i in range(1, len(sys.argv)):
        p = Path(sys.argv[i])
        p = p.absolute()
        if not p.exists():
            raise ValueError("submission file does not exist")
        if p.is_dir():
            for f in p.iterdir():
                print(ziggy_submit_from_path(f.as_posix()))
        elif p.is_file():
            print(ziggy_submit_from_path(p.as_posix()))
    sys.exit(0)