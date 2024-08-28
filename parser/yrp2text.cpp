/****************************************************************************
 * Copyright (c) 2016-2019 Justin P. Rohrer <jprohrer@tancad.org> 
 * All rights reserved.  
 *
 * Program:     $Id: yrp2warts.cpp $
 * Description: Convert Yarrp output files (https://www.cmand.org/yarrp) to 
 *              plain text files.
 *              indent -i4 -nfbs -sob -nut -ldi1 yrp2warts.cpp
 *
 * Attribution: R. Beverly, "Yarrp'ing the Internet: Randomized High-Speed 
 *              Active Topology Discovery", Proceedings of the ACM SIGCOMM 
 *              Internet Measurement Conference, November, 2016
 * 
 * 
 * Modified: 	Weili Wu, yrp2text.cpp, 2024.08.05
 * 				Resolve edge cases where IPID (TTL encoded) field being modified
 * 
 ***************************************************************************/
 
#include <unordered_map>
#include <sys/time.h>
#include <iomanip>
#include <memory>
#include <fstream>
#include "ipaddress.hpp"
#include "yarrpfile.hpp"

using namespace std;
using namespace ip;

string infile_name = "";
string outfile_name = "";
bool read_stdin = false;

void usage(char *prog) {
	cout << "Usage:" << endl
		 << " $ " << prog << " -i input.yrp -o output.txt" << endl
		 << " $ bzip2 -dc input.yrp.bz2 | " << prog << " -s -o output.txt" << endl
		 << "  -i, --input             Input Yarrp file" << endl
		 << "  -o, --output            Output text file" << endl
		 << "  -s, --stdin             Read piped input from STIDN" << endl
		 << "  -h, --help              Show this message" << endl
		 << endl;
	exit(-1);
}

void parse_opts(int argc, char **argv) {
	if (argc <= 3) {
		usage(argv[0]);
	}
	char c;
	int opt_index = 1;
	while (opt_index < argc-1) {
		c = argv[opt_index][1];
		switch (c) {
		  case 'i':
			infile_name = argv[++opt_index];
			break;
		  case 'o':
			outfile_name = argv[++opt_index];
			break;
		  case 's':
			read_stdin = true;
			break;
		  case 'h':
		  default:
			usage(argv[0]);
		}
		opt_index++;
	}
}

struct yrpStats {
	ipaddress vantage_point;
	uint8_t tracetype;
	uint16_t maxttl;
	double t_min;
	double t_max;
};

struct hop {
	ipaddress addr;
	uint32_t sec;
	uint32_t usec;
	uint32_t rtt;
	uint16_t ipid;
	uint16_t psize;
	uint16_t rsize;
	uint8_t ttl;
	uint8_t rttl;
	uint8_t rtos;
	uint8_t icmp_type;
	uint8_t icmp_code;
	// for reconstructing the original order
	uint32_t yrp_counter;
	hop& operator= (const yarrpRecord &r)
	{
		addr = r.hop;
		sec = r.sec;
		usec = r.usec;
		rtt = r.rtt;
		ipid = r.ipid;
		psize = r.psize;
		rsize = r.rsize;
		ttl = r.ttl;
		rttl = r.rttl;
		rtos = r.rtos;
		icmp_type = r.typ;
		icmp_code = r.code;
		return *this;
	}
};

ostream& operator<< (ostream& os, const hop& h)
{
    return os << uint16_t(h.ttl) << " " << h.rtt << " " << h.addr;
}

bool operator<(const hop& h1, const hop& h2) {
	return h1.ttl < h2.ttl;
}

bool operator==(const hop& h1, const hop& h2) {
	return h1.ttl == h2.ttl;
}

yrpStats yarrp_proc(string yarrpfile, unordered_map<ipaddress, vector<hop> > &traces) {
	yarrpFile yrp;
	yarrpRecord r;
	yrpStats s;
	s.t_min = 0;
	s.t_max = 0;
	if (read_stdin) {
		if (!yrp.open(std::cin)) {
			cerr << "Failed to open input stream" << endl;
			exit(1);
		}
		std::cin.tie(nullptr);
	}
	else {
		if (!yrp.open(yarrpfile)) {
			cerr << "Failed to open input file: " << yarrpfile << endl;
			exit(1);
		}
	}
	double timestamp = 0.0;
	uint64_t yrp_counter = 0;
	while (yrp.nextRecord(r)) {
		hop this_hop;
		this_hop = r;
		this_hop.yrp_counter = yrp_counter;
		traces[r.target].push_back(this_hop);	// Each trace must be <= 255 hops long
		timestamp = r.sec + (r.usec / 1000000.0);
		if (s.t_min <= 0) { s.t_min = timestamp; }
	    if (timestamp < s.t_min) { s.t_min = timestamp; }
	    if (timestamp > s.t_max) { s.t_max = timestamp; }
		yrp_counter++;
	}
	s.vantage_point = yrp.getSource();
	s.tracetype = yrp.getType();
	s.maxttl = yrp.getMaxTtl();
	cout << "Processed " << yrp_counter << " Yarrp records" << endl;
	return s;
}


/**
 * We add additional info for each hop - the original order appeared in the raw data
 * Each traceroute packets has a order number and the larger the later, 
 * we can then sort hops based on this number for each target to recover the orginal order during probing
*/

void write_traces_to_text(unordered_map<ipaddress, vector<hop> > &traces, string outfile_name) {
	ofstream outfile(outfile_name);
	if (!outfile.is_open()) {
		cerr << "Failed to open output file: " << outfile_name << endl;
		exit(1);
	}

	for (auto &trace : traces) {
		ipaddress target = trace.first;
		vector<hop> &hops = trace.second;
		
		outfile << "Trace to:" << target << endl;
		sort(hops.begin(), hops.end());
		for (auto &this_hop : hops) {
			outfile << this_hop.addr << ","
                    << this_hop.sec << ","
                    << this_hop.usec << ","
                    << this_hop.rtt << ","
                    << this_hop.ipid << ","
                    << this_hop.psize << ","
                    << this_hop.rsize << ","
                    << uint16_t(this_hop.ttl) << ","
                    << uint16_t(this_hop.rttl) << ","
                    << uint16_t(this_hop.rtos) << ","
                    << uint16_t(this_hop.icmp_type) << ","
                    << uint16_t(this_hop.icmp_code) << ","
					<< this_hop.yrp_counter << endl;
		}
		outfile << "END" << endl;
	}
	outfile.close();
	cout << "Successfully wrote traces to " << outfile_name << endl;
}

int main(int argc, char* argv[])
{
	ios_base::sync_with_stdio(false);
	parse_opts(argc, argv);
	unordered_map<ipaddress, vector<hop> > traces;
	yrpStats stats = yarrp_proc(infile_name, traces);
	cout << "Created " << traces.size() << " traces" << endl;

	write_traces_to_text(traces, outfile_name);

	return 0;
}