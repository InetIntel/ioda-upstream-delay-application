import pyipmeta
from datetime import datetime
import subprocess
import os
import json
from collections import defaultdict
import ipaddress
import logging

# Longest prefix match
# Dict with first octet as key, as the shortest prefix length is 8
# sort prefix by prefix length in descending order, so that once found, it is the LPM.
def compute_prefix_set():
    grouped_prefixes = defaultdict(list)
    file_path = "intermediate_result/targets/routeviews-rv2-20240422-1200.pfx2as"

    with open(file_path, 'r') as file:
        for line in file:
            parts = line.strip().split()
            
            if len(parts) >= 2:
                prefix = f"{parts[0]}/{parts[1]}"
                first_octet = prefix.split('.')[0]
                network = ipaddress.IPv4Network(prefix)
                grouped_prefixes[first_octet].append(network)

    for first_octet in grouped_prefixes:
        grouped_prefixes[first_octet].sort(key=lambda x: x.prefixlen, reverse=True)
    return grouped_prefixes

def LPM(ip, grouped_prefixes):
    ip = ipaddress.IPv4Address(ip)
    first_octet = int(str(ip).split('.')[0])
    
    if first_octet in grouped_prefixes:
        for network in grouped_prefixes[first_octet]:
            if ip in network:
                return str(network)
    
    return None

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
                    if len(rtts) >=2:
                        lat = (int(rtts[-1])-int(rtts[-2]))/1000
                    else:
                        lat = -1
                    result_dict = {
                        "timestamp": dt,
                        "dest": {"ip": dest_ip, "asn": dest_as},
                        "latency": lat,
                        "penultimate_asn": as_path[-1],
                        "full_traceroute": full_tr_result,
                        "as_path": as_path
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


def process_yarrp_result(folder_path, filename):
    curr = datetime.now()
    dt = curr.strftime("%Y-%m-%dT%H:%M:%S")
    ipm = pyipmeta.IpMeta(providers=["pfx2as "
                                    "-f input/dataset/routeviews-rv2-20240422-1200.pfx2as.gz"])

    
    src_path = os.path.join(folder_path, filename)
    parsed_file_path = os.path.join(folder_path, "parsed.txt")
    dst_path = os.path.join(folder_path, "result")
    try:
        process = subprocess.run(["parser/yrp2text", "-i", src_path, "-o", parsed_file_path])
    except subprocess.CalledProcessError as e:
        logging.error(f"yrp2text error - {e.stderr}")
    os.makedirs(dst_path, exist_ok=True)

    group_by_as(parsed_file_path, dst_path, 32, ipm, dt)
        