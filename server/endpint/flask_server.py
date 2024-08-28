from flask import Flask, request, jsonify, send_file
import os
import time

app = Flask(__name__)

# TODO: Request UUID list from Elasticsearch
valid_uuids = {}
target_ip_filepath = "target_generation/targets"
last_modified_time = os.path.getmtime(target_ip_filepath)

@app.route('/validate', methods=['POST'])
def validate_uuid():
    data = request.get_json()
    uuid = data.get('uuid')

    if uuid in valid_uuids:
        return jsonify({"valid": True}), 200
    else:
        return jsonify({"valid": False}), 400
    

@app.route('/update', methods=['GET'])
def get_updated_target():
    current_modified_time = os.path.getmtime(target_ip_filepath)
    if current_modified_time > last_modified_time:
        last_modified_time = current_modified_time
        return send_file(target_ip_filepath, as_attachment=True)
    else:
        return jsonify({"message": "No update"}), 200

if __name__ == "__main__":
    app.run(host='0.0.0.0', port=5000)