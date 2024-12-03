import pyipmeta
from datetime import datetime
import subprocess
import os
import json
from collections import defaultdict
import logging
from concurrent.futures import ProcessPoolExecutor
#import _pytimeseries
import numpy as np

"""
Input: path towards raw data folder, 
such as "/traversa-pool/weili-yarrp-testing/result/venezuela"
Output: Kafka stream

Stage 1: Raw Yarrp data -> parsed yarrp file (single text file containing all traceroute objects)
Stage 2: Parsed yarrp file -> Fake ES storage (list of prefix data, groupded by ASes)
Stage 3: List of prefix data, group by ASes -> Kafka Stream

"""


"""
Stage 1
Raw Yarrp data -> parsed yarrp file (single text file containing all traceroute objects)
input: base dir
output: {base_dir}/parsed, containing a list of txt file, each is the parsed yarrp data for a timestamp
"""
def stage1_yarrp_parsing(base_dir):
    print("Stage 1 starting...")
    raw_data_dir = os.path.join(base_dir, "raw")

    # For all dates involved - can add a filter here such as if dir.startwith("XX")
    # !! dir name is the timestamp of the measurement and all resulting files or dirs are named using this timestamp
    tasks = [(base_dir, dir) for dir in os.listdir(raw_data_dir) 
             if os.path.isdir(os.path.join(raw_data_dir, dir))]

    with ProcessPoolExecutor(max_workers=1) as executor:
        executor.map(process_yarrp_result, tasks)

    print("Stage 1 complete.")

def process_yarrp_result(args):
    base_dir, ts = args
    src_file_path = os.path.join(base_dir, "raw", ts, f"{ts}-test.yrp")
    # Stage result
    parsed_file_path = os.path.join(base_dir, "parsed", f"{ts}-parsed.txt")
    
    try:
        process = subprocess.run(["input/parser/yrp2text", "-i", src_file_path, "-o", parsed_file_path])
        process.check_returncode()
    except Exception as e:
        logging.error(f"Unexpected error - {str(e)}")




"""
Stage 2
Parsed yarrp file -> Fake ES storage (list of prefix data, groupded by ASes)
input: base dir
output: {base_dir}/es, containing a list of folders, named by timestamp
Each timestamp folder contains ~75K files (ASes).
"""
def stage2_processing_and_grouping(base_dir):
    print("Stage 2 starting...")
    raw_data_dir = os.path.join(base_dir, "raw")

    # Assume all files are successfully parsed, reuse raw data folder to generate tasks
    tasks = [(base_dir, dir) for dir in os.listdir(raw_data_dir) 
             if os.path.isdir(os.path.join(raw_data_dir, dir))]

    with ProcessPoolExecutor(max_workers=1) as executor:
        executor.map(process_parsed_result, tasks)

    print("Stage 2 complete.")

def process_parsed_result(args):
    base_dir, ts = args
    # input
    parsed_file_path = os.path.join(base_dir, "parsed", f"{ts}-parsed.txt")
    # stage output
    result_files_folder_path = os.path.join(base_dir, "es", ts)
    os.makedirs(result_files_folder_path, exist_ok=True)

    ipm = pyipmeta.IpMeta(providers=["pfx2as "
                                    "-f input/routeviews-rv2-20240422-1200.pfx2as.gz"])
    

    datetime_str = datetime.strptime(f"2024 {ts}", "%Y %B-%d-%H-%M").strftime("%Y-%m-%dT%H:%M:%S")

    group_by_as(parsed_file_path, result_files_folder_path, 32, ipm, datetime_str)


"""
each json represents an AS; this json is not a regular Dict obj!!
it contains a list of dict, each represents a prefix, rather than a big dict
"""
def group_by_as(input_path, output_path, max_ttl, ipm, dt):
    with open(input_path) as file:
        while True:
            line = file.readline()
            if not line:
                break
            try:
                if line.startswith('Trace to:'):
                    dest_ip = line.strip().split(':')[-1]
                    dest_as = ipm.lookup(dest_ip)
                    dest_as = dest_as[0]["asns"][0] if dest_as else ""
                    full_tr_result = []
                    as_path = []
                    rtts = []

                elif line.startswith('END'):
                    # Ideally penultimate as and latency should be here, but since we need to do extra analysis before inserting into kafka, all calculation is placed in stage 3
                    # here we store full traceroute, as path, and latency path.

                    # If want to recover original packet order, one can sort full_traceroute by yrp_counter (hop_info[12].strip()), the smaller the counter, the earlier the packet, skip here
                    result_dict = {
                        "timestamp": dt,
                        "dest": {"ip": dest_ip, "asn": dest_as},
                        "full_traceroute": full_tr_result,
                        "as_path": as_path,
                        "rtts": rtts
                    }

                    file_path = f"{output_path}/{dest_as}.json"
                    with open(file_path, "a") as json_file:
                        json.dump(result_dict, json_file)
                        json_file.write("\n")

                    
                else:
                    hop_info = line.strip().split(',')
                    addr = hop_info[0].strip()
                    sec = hop_info[1].strip()
                    usec = hop_info[2].strip()
                    rtt = hop_info[3].strip()
                    ipid = hop_info[4].strip()
                    psize = hop_info[5].strip()
                    rsize = hop_info[6].strip()
                    ttl = hop_info[7].strip()
                    rttl = hop_info[8].strip()
                    rtos = hop_info[9].strip()
                    icmp_type = hop_info[10].strip()
                    icmp_code = hop_info[11].strip()
                    yrp_counter = hop_info[12].strip()
                    full_tr_result.append(line)
                    rtts.append(rtt)

                    if addr:
                        hop_as = ipm.lookup(addr)
                        as_path.append(str(hop_as[0]["asns"][0]) if hop_as else "N/A")
                    else:
                        as_path.append("N/A")

            except Exception as e:
                print(f"error during line: {line} - error: {e}")
                pass
    return 




def stage3_analyzing_and_reporting(base_dir):
    print("Stage 3 starting...")
    es_data_dir = os.path.join(base_dir, "es")

    vpid = "FAKEVPID"
    # This is the dict that we send to Kafka:
    # Key is (vantage_point, as_number, penultimate_as)
    # Value is latencies and penultimate_count
    aggregation_data = {}
    for timestamp in os.listdir(es_data_dir):
        timestamp_dir = os.path.join(es_data_dir, timestamp)
        if os.path.isdir(timestamp_dir):
            for as_file in os.listdir(timestamp_dir):
                file_path = os.path.join(timestamp_dir, as_file)
                as_number = as_file.replace('.json', '') 
                results = analyzing(file_path, as_number)
                for penultimate_as, latencies in results.items():
                    key = (vpid, as_number, penultimate_as)
                    aggregation_data[key] = {'latencies': latencies, 'penultimate_count': len(latencies), 'timestamp': timestamp}


    print(aggregation_data)

    """
    adjust broker, channel, measurement for kafka
    """
    #flush_to_kafka(aggregation_data, broker, channel, measurement)
    print("Stage 3 complete.")


"""
Given an AS number (at timestamp X), 
Update aggregation_data

return a dict:
Key: penultimate AS
Value: list of latencies (each latency is for one prefix that with this AS as penultimate AS)
"""
def analyzing(file_path, origin_as):
    result = defaultdict(list)
    
    try:
        with open(file_path, 'r') as file:
            for line in file:
                    entry = json.loads(line)
                    if 'as_path' in entry and len(entry['as_path']) >= 2:
                        as_path = entry['as_path']
                        rtts = entry['rtts']
                        # Find the index of the last occurrence of the origin AS
                        # Find the previous AS which is not the origin AS
                        # Calculate penultimate latency
                        if origin_as in as_path:
                            origin_as_index = len(as_path) - 1 - as_path[::-1].index(origin_as)
                            
                            for i in range(origin_as_index - 1, -1, -1):
                                if as_path[i] != origin_as:
                                    penultimate_as = as_path[i]
                                    if penultimate_as == 'N/A':
                                        continue
                                    latency = int(rtts[origin_as_index]) - int(rtts[i])
                                    result[penultimate_as].append(latency)
                                    break
    except Exception as e:
        logging.error(f"Stage 3 - analyzing - {str(e)}")
    return result



def flush_to_kafka(aggregation_data, broker, channel, measurement):
    # Boiler-plate libtimeseries setup for a kafka output
    pyts = _pytimeseries.Timeseries()
    be = pyts.get_backend_by_name('kafka')
    if not be:
        raise Exception('Unable to find pytimeseries Kafka backend')
    if not pyts.enable_backend(be, "-b %s -c %s -f ascii -p %s" % ( \
            broker, channel, measurement)):
        raise Exception('Unable to initialize pytimeseries Kafka backend')

    kp = pyts.new_keypackage(reset=False, disable=True)
    # Boiler-plate ends


    # Aggregate and write each field to Kafka
    for tag_key, data in aggregation_data.items():
        vantage_point, as_number, penultimate_as = tag_key
        latencies = data['latencies']
        penultimate_count = data['penultimate_count']
        # TODO: should check in ES if timestamp is correct
        timestamp = data['timestamps']

        # Compute different aggregations of latencies
        mean_latency = np.mean(latencies)
        max_latency = np.max(latencies)
        min_latency = np.min(latencies)
        p90_latency = np.percentile(latencies, 90)
        p10_latency = np.percentile(latencies, 10)


        # TODO: whether pyts can simply add multiple fields for the same set of tags?
        fields = {
            "mean_latency": mean_latency,
            "max_latency": max_latency,
            "min_latency": min_latency,
            "p90_latency": p90_latency,
            "p10_latency": p10_latency,
            "penultimate_as_count": penultimate_count
        }

        for metric_name, value in fields.items():
            key = f"{measurement}.{vantage_point}.{as_number}.{penultimate_as}.{metric_name}"
            key = key.encode()
            idx = kp.get_key(key)
            if idx is None:
                idx = kp.add_key(key)


            if metric_name == "penultimate_as_count":
                kp.set(idx, int(value))
            else:
                kp.set(idx, int(value * 1000))

        kp.flush(timestamp)


"""
Input: path towards raw data folder, 
such as "/traversa-pool/weili-yarrp-testing/result/venezuela"
Output: Kafka stream

Stage 1: Raw Yarrp data -> parsed yarrp file (single text file containing all traceroute objects)
Stage 2: Parsed yarrp file -> Fake ES storage (list of prefix data, groupded by ASes)
Stage 3: List of prefix data, group by ASes -> Kafka Stream

"""
if __name__ == "__main__":
    base_dir = "/traversa-pool/weili-yarrp-testing/result/testFront"
    #base_dir = "result/testFront"

    os.makedirs(os.path.join(base_dir, "parsed"), exist_ok=True)
    os.makedirs(os.path.join(base_dir, "es"), exist_ok=True)
    
    # cp all files under {base_dir}/raw
    #stage1_yarrp_parsing(base_dir)
    #stage2_processing_and_grouping(base_dir)
    stage3_analyzing_and_reporting(base_dir)