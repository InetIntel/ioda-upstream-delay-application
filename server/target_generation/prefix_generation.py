import csv
import os
from collections import defaultdict
import ipaddress
from multiprocessing import Pool

"""
Input - pfx2as, processed ip historical data
Output - most responsive IP per BGP prefix
"""


def compute_prefix_set():
    grouped_prefixes = defaultdict(set)
    file_path = "intermediate_result/targets/routeviews-rv2-20240422-1200.pfx2as"

    with open(file_path, 'r') as file:
        for line in file:
            parts = line.strip().split()
            
            if len(parts) >= 2:
                prefix = f"{parts[0]}/{parts[1]}"
                first_octet = prefix.split('.')[0]
                grouped_prefixes[first_octet].add(prefix)
    return grouped_prefixes

def find_most_responsive_ip(args):
    first_octet, prefixes, csv_dir = args
    most_responsive_ips = {}
    csv_file = os.path.join(csv_dir, f"ip_{first_octet}.csv")
    
    print(f"START {csv_file}")

    # test if a ip is within a prefix, if yes, update the best score for that prefix
    prefixes = {prefix: ipaddress.ip_network(prefix) for prefix in prefixes}
    best_scores = {prefix: (None, float('-inf')) for prefix in prefixes}
    if os.path.isfile(csv_file):
        with open(csv_file, 'r') as csvfile:
            reader = csv.reader(csvfile, delimiter=',')
            headers = next(reader)

            for row in reader:
                ip = row[0]
                score = float(row[2])
                ip_obj = ipaddress.ip_address(ip)
                for prefix, network in prefixes.items():
                    if ip_obj in network:
                        if score > best_scores[prefix][1]:
                            best_scores[prefix] = (ip, score)
                        break

    for prefix, (ip, score) in best_scores.items():
        if ip is not None and score != 0:
            most_responsive_ips[prefix] = ip
        else:
            network = prefixes[prefix]
            default_ip = str(network.network_address + 1) # gateway by default
            most_responsive_ips[prefix] = default_ip

    print(f"FINISHED {csv_file}")
    return most_responsive_ips

def save_ip_list_to_file(most_responsive_ips, output_file):
    with open(output_file, 'w') as f:
        for ip in most_responsive_ips.values():
            f.write(f"{ip}\n")

def main():
    csv_dir = "intermediate_result/targets/parsed"
    output_file = "intermediate_result/targets/test.txt"

    grouped_prefixes = compute_prefix_set()

    with Pool() as pool:
        results = pool.map(find_most_responsive_ip, [(first_octet, prefixes, csv_dir) for first_octet, prefixes in grouped_prefixes.items()])

    most_responsive_ips = {}
    for result in results:
        most_responsive_ips.update(result)

    save_ip_list_to_file(most_responsive_ips, output_file)
    print(f"most responsive IPs computed - {output_file}")

if __name__ == "__main__":
    main()