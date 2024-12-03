import os
import paramiko
import json
import sys

def load_data_from_json(filename):
    with open(filename, 'r') as file:
        data = json.load(file)
    return data

def fetch_files(remote_host, remote_port, username, key_path, remote_base_dir, local_base_dir, files):
    # Create SSH client
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

    # Set up the SSH key for authentication
    mykey = paramiko.RSAKey.from_private_key_file(key_path)

    # Connect to the server
    ssh.connect(remote_host, port=remote_port, username=username, pkey=mykey)

    # Create SFTP session
    sftp = ssh.open_sftp()

    # Loop through each directory and fetch files
    for dir in remote_base_dir:
        # Extract only the last directory name to use in local path
        last_dir_name = os.path.basename(dir)
        local_path = os.path.join(local_base_dir, last_dir_name)
        
        # Create the directory locally if it doesn't exist
        if not os.path.exists(local_path):
            os.makedirs(local_path)

        # Fetch each specified file
        for file in files:
            remote_file_path = os.path.join(dir, file)
            local_file_path = os.path.join(local_path, file)
            sftp.get(remote_file_path, local_file_path)

    # Close connections
    sftp.close()
    ssh.close()

if __name__ == "__main__":
    # Load directory data from JSON
    file_data = load_data_from_json('source_data/count.json')
    target_dirs = [entry["folder"] for entry in file_data]

    # Connection settings
    remote_host = "traversa.cc.gatech.edu"
    remote_port = 3412  # Custom SSH port
    username = "weili"
    key_path = "/Users/weili/.ssh/weili"
    local_base_dir = "result/vene"
    files_to_fetch = ['8048.json', '27889.json']  # Example filenames

    # Expand the environment variable for key_path
    key_path = os.path.expanduser(key_path)

    # Fetch files
    fetch_files(remote_host, remote_port, username, key_path, target_dirs, local_base_dir, files_to_fetch)

    print("Files have been fetched successfully.")