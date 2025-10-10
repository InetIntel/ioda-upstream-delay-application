# yrp2text

Convert Yarrp binary output files to human-readable text format.

## Build

```bash
make
```

## Usage

**From file:**
```bash
./yrp2text -i input.yrp -o output.txt
```

**From stdin (compressed):**
```bash
bzip2 -dc input.yrp.bz2 | ./yrp2text -s -o output.txt
```

**With custom delimiter:**
```bash
./yrp2text -i input.yrp -o output.txt -d "\t"
```

## Options

- `-i, --input`   Input Yarrp file (.yrp format)
- `-o, --output`  Output text file
- `-d, --delimiter` Field delimiter (default: comma)
- `-s, --stdin`   Read piped input from STDIN
- `-h, --help`    Show help message

## Output Format

Groups traceroute results by target IP address. Each hop is a CSV line with the following fields:

```
hop_addr,sec,usec,rtt,ipid,psize,rsize,ttl,rttl,rtos,"mpls",icmp_type,icmp_code,yrp_counter
```

### Field Descriptions

| Field | Description |
|-------|-------------|
| `hop_addr` | IP address of the responding hop |
| `sec` | Timestamp (seconds) |
| `usec` | Timestamp (microseconds) |
| `rtt` | Round-trip time |
| `ipid` | IP identification field |
| `psize` | Probe packet size |
| `rsize` | Response packet size |
| `ttl` | TTL value sent in probe |
| `rttl` | TTL in response packet |
| `rtos` | Type of service field |
| `mpls` | MPLS label stack (quoted, see below) |
| `icmp_type` | ICMP response type |
| `icmp_code` | ICMP response code |
| `yrp_counter` | Sequence number for preserving probe order |

### MPLS Format

The MPLS field is **quoted when using comma delimiter** to handle comma-separated label stacks:

- No MPLS: `"0"` (with comma) or `0` (with other delimiters)
- Single label: `"16:255"` (label:ttl)
- Multiple labels: `"16:255,17:254,18:253"` (comma-separated stack)

Note: Quotes are automatically omitted when using non-comma delimiters (e.g., tab)

### Example Output

```
Trace to:8.8.8.8
192.168.1.1,1760034038,71327,3147,19841,40,68,1,64,192,"0",11,0,2
10.246.255.61,1760034038,272059,3951,15470,40,96,3,252,0,"25:253",11,0,4
217.164.200.1,1760034038,771974,3830,181,40,56,2,254,0,"0",11,0,9
195.229.4.64,1760034038,876054,7922,26570,40,96,5,251,0,"50:252,60:251,70:250",11,0,10
END
```
