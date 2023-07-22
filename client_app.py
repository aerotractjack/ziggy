from client.submit_job import ZiggyClient
from flask import Flask, request, jsonify
import json
import yaml
from datetime import datetime

'''
Usage example:
    (env_name) $ python3 client_app.py
'''

app = Flask(__name__)

html = '''
<!DOCTYPE html>
<html>
<head>
  <title>Upload File or Paste Data</title>
</head>
<body>
  <h1>Upload File or Paste Data</h1>
  <form action="/" method="post" enctype="multipart/form-data">
    <label for="file">Upload File:</label>
    <input type="file" name="file" id="file">
    <br><br>
    <label for="data">Paste Data:</label>
    <textarea name="data" id="data" rows="5" cols="40"></textarea>
    <br><br>
    <input type="submit" value="Submit">
  </form>
</body>
</html>

'''

@app.route("/", methods=["GET", "POST"])
def submit():
    if request.method == "POST":
        uploaded_file = request.files["file"]
        data = request.form["data"]
        if uploaded_file:
            ext = uploaded_file.filename.split(".")[-1]
            contents = uploaded_file.read().decode("utf-8")
            if ext == "yaml":
                contents = yaml.safe_load(contents)
            else:
                contents = json.loads(contents)
            return jsonify({"message": "File uploaded successfully", **contents})
        elif data:
            try:
                data = json.loads(data)
            except:
                data = yaml.safe_load(data)
            return jsonify({"message": "Data received successfully", "data": data})
        else:
            return jsonify({"error": "No file or data provided"})
    return html


if __name__ == "__main__":
    app.run(port=6011, debug=True, host="0.0.0.0")