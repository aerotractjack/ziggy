from runner.ziggy_submitter import ZiggyQueueSubmitter
from runner.job import ZiggyJobRegistry
from flask import Flask, request, jsonify
from datetime import datetime

'''
Simple Flask app to intake job config files over network and add them
to Ziggy's job queue for future processing

Usage example:
    (env_name) $ python3 server_submit_app
'''

app = Flask(__name__)

ziggy_queue_submitter = ZiggyQueueSubmitter("/home/aerotract/.ziggyq", ZiggyJobRegistry)

def log(submission, response):
    print("==============================")
    print(str(datetime.now()))
    print(submission)
    print(response)
    print("==============================")

@app.route("/submit", methods=["POST"])
def submit():
    submission = request.json
    response = ziggy_queue_submitter.submit(submission)
    log(submission, response)
    return jsonify(response)

if __name__ == "__main__":
    app.run(debug=True)