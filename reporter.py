import json
import subprocess
import logging
import os


def get_pending_files(conf):
    return [os.path.join(conf['prober']['tmp_dir'],f)
            for f in os.listdir(conf['prober']['tmp_dir']) 
            if os.path.isfile(os.path.join(conf['prober']['tmp_dir'],f))]

async def report_data(conf):
    pending_files = get_pending_files(conf)
    if len(pending_files) == 0:
        return
    rsync_cmd = [
            'rsync',
            '-avHP',
            '--remove-source-files',
            '-e',
            'ssh  -i ' +conf['ssh_identity_file']+' -p 3412 -o "StrictHostKeyChecking no"'
            ] + pending_files + [
            'ioda-ud@traversa.cc.gatech.edu:/traversa-pool/upstream-delay/incoming']
    logging.info(f"Executing command: {rsync_cmd}")
    process = subprocess.Popen(rsync_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdout, stderr = process.communicate()
    logging.info("Execution completed")
    if process.returncode == 0:
        logging.info(f'Command succeeded: {stdout.decode()}')
    else:
        logging.info(f'Command returned unexpected code: {stderr.decode()}')
