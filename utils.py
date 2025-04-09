import socket
import subprocess
import logging

def get_vp_ip():
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.connect(("8.8.8.8", 80))
    vp_ip = s.getsockname()[0]
    s.close()
    return vp_ip

def get_vp_network_interface():
    ip = get_vp_ip()
    cmd1 = ['ifconfig']
    cmd2 = ['grep', '-B1', ip]
    cmd3 = ['grep', '-o', r'^\w*']
    
    process1 = subprocess.Popen(cmd1, stdout=subprocess.PIPE)
    process2 = subprocess.Popen(cmd2, stdin=process1.stdout, stdout=subprocess.PIPE)
    process1.stdout.close()
    process3 = subprocess.Popen(cmd3, stdin=process2.stdout, stdout=subprocess.PIPE)
    process2.stdout.close()
    stdout, stderr = process3.communicate()
    return stdout.strip()

def get_interface_io_stats(interface):
    cmd = ['ifconfig', 'eno1']
    p = subprocess.Popen(cmd, stdout=subprocess.PIPE)
    stdout, stderr = p.communicate()
    d = {}
    for line in stdout.split(b"\n"):
        if line.strip().startswith(b"RX packets"):
            toks = line.strip().split()
            d["rx_packets"] = int(toks[2])
            d["rx_bytes"] = int(toks[4])
        elif line.strip().startswith(b"RX errors"):
            toks = line.strip().split()
            d["rx_errors"] = int(toks[2])
            d["rx_dropped"] = int(toks[4])
            d["rx_overruns"] = int(toks[6])
            d["rx_frame"] = int(toks[8])
        elif line.strip().startswith(b"TX packets"):
            toks = line.strip().split()
            d["tx_packets"] = int(toks[2])
            d["tx_bytes"] = int(toks[4])
        elif line.strip().startswith(b"TX errors"):
            toks = line.strip().split()
            d["tx_errors"] = int(toks[2])
            d["tx_dropped"] = int(toks[4])
            d["tx_overruns"] = int(toks[6])
            d["tx_carrier"] = int(toks[8])
            d["tx_collisions"] = int(toks[10])
    return d


def get_network_stats():
    interface = get_vp_network_interface()
    return get_interface_io_stats(interface)

def compare_stats(t1, t2):
    diff = {}
    for key in t1:
        diff[key] = t2[key] - t1[key]
    return diff
        
    
