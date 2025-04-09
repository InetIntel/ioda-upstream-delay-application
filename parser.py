import pyipmeta
from datetime import datetime
import subprocess
import os
import json
from collections import defaultdict
import ipaddress
import logging

def parse_yarrp_txt(input_path, output_path, max_ttl, dt):
    with open(input_path) as f:
        current_trace = {}
        for line in f:
            try:
                if line.startswith('Trace to:'):
                    current_trace['dst'] = {"ip" : line.strip().split(':')[-1]}
                    current_trace["measurement_info"] = {} 
                    current_trace["hops"] = []
                    #current_trace["raw"] = []
                elif line.startswith('END'):
                    file_path = f"{output_path}/traceroutes.json"
                    with open(file_path, "a") as json_file:
                        json.dump(current_trace, json_file)
                        json_file.write("\n")
                    current_trace = {}
                    
                else:
                    #current_trace['raw'].append(line)
                    toks = [x.strip() for x in line.strip().split(',')]
                    current_trace['hops'].append({"ip" : toks[0],
                                                  "sec" : toks[1],
                                                  "usec" : toks[2],
                                                  "rtt" : toks[3],
                                                  "ipid" : toks[4],
                                                  "psize" : toks[5],
                                                  "rsize" : toks[6],
                                                  "ttl" : toks[7],
                                                  "rttl" : toks[8],
                                                  "rtos" : toks[9],
                                                  "icmp_type" : toks[10],
                                                  "icmp_code" : toks[11],
                                                  "yrp_counter" : toks[12]})
            except Exception as e:
                print(f"error during line: {line} - error: {e}")
                pass


def process_yarrp_result(folder_path, filename):
    curr = datetime.now()
    dt = curr.strftime("%Y-%m-%dT%H:%M:%S")
    
    src_path = os.path.join(folder_path, filename)
    parsed_file_path = os.path.join(folder_path, "parsed.txt")
    dst_path = os.path.join(folder_path, "result")
    try:
        process = subprocess.run(["yrp2text/yrp2text", "-i", src_path, "-o", parsed_file_path])
    except subprocess.CalledProcessError as e:
        logging.error(f"yrp2text error - {e.stderr}")
    os.makedirs(dst_path, exist_ok=True)

    parse_yarrp_txt(parsed_file_path, dst_path, 32, dt)
        
