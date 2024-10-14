import re
from datetime import datetime, timedelta, tzinfo
import subprocess
import ipaddress

class UTC(tzinfo):
    def utcoffset(self, dt):
        return timedelta(0)
    def tzname(self, dt):
        return "UTC"
    def dst(self, dt):
        return timedelta(0)

def convert_to_utc(log_time, timezone_offset):
    # Convert the timezone offset from '-0500' to a timedelta
    sign = 1 if timezone_offset[0] == '+' else -1
    hours_offset = int(timezone_offset[1:3])
    minutes_offset = int(timezone_offset[3:])

    offset = timedelta(hours=sign * hours_offset, minutes=sign * minutes_offset)
    
    utc_time = log_time - offset
    return utc_time.replace(tzinfo=UTC())

size_regex = r'size:\s*(\d+)'
hostname_regex = r'\[([a-zA-Z0-9.-]+):\d+\]'

def get_ordered_subnets(unique_subnets):
    # Sort the unique subnets based on their numerical value
    ordered_subnets = sorted(unique_subnets, key=lambda x: ipaddress.IPv6Network(x))
    return ordered_subnets

# Function to extract unique hostnames, sizes, and run nslookup
def extract_successful_ips_and_hostnames(logfile_path):
    unique_hostnames = set()
    total_size = 0
    line_count = 0  # both 'size' and 'SUCCESS'
    utc = UTC()
    start_time = None
    end_time = None

    with open(logfile_path, 'r') as logfile:
        for line in logfile:
            timestamp_match = re.match(r'\[(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{6}) ([+-]\d{4})\]', line)
            if timestamp_match:
                log_time = datetime.strptime(timestamp_match.group(1), '%Y-%m-%d %H:%M:%S.%f')
                
                timezone_offset = timestamp_match.group(2)

                utc_time = convert_to_utc(log_time, timezone_offset)

                if start_time is None:
                    start_time = utc_time

                end_time = utc_time

            if 'SUCCESS' in line:
                size_match = re.search(size_regex, line)
                hostname_match = re.search(hostname_regex, line)

                if size_match and hostname_match:
                    size_value = int(size_match.group(1))
                    hostname = hostname_match.group(1)
                    
                    unique_hostnames.add(hostname)
                    
                    total_size += size_value
                    line_count += 1  

    return unique_hostnames, total_size, start_time, end_time, line_count

# Function to get only IPv6 subnets from nslookup results
def get_ipv6_from_hostname(hostname):
    # Run nslookup to get AAAA records
    result = subprocess.run(['nslookup', '-type=AAAA', hostname], capture_output=True, text=True)
    
    # Extract IPv6 addresses from the output (valid IPv6 has colons and follows "AAAA address")
    ipv6_regex = r'AAAA address\s*([A-Fa-f0-9:]+)'
    ipv6_matches = re.findall(ipv6_regex, result.stdout)
    
    # Filter out any non-IPv6 addresses
    ipv6_addresses = [ip for ip in ipv6_matches if ':' in ip]
    
    # Handle case when no IPv6 addresses are found
    if not ipv6_addresses:
        print(f"No IPv6 addresses found for {hostname}")
        return set()
    
    # Convert IPv6 addresses to subnets (using /64 by default)
    ipv6_subnets = set()
    for ipv6 in ipv6_addresses:
        try:
            # Parse the IPv6 address and get the /64 subnet
            ipv6_obj = ipaddress.IPv6Address(ipv6)
            subnet = ipaddress.IPv6Network(f'{ipv6}/64', strict=False)
            ipv6_subnets.add(str(subnet))
        except ipaddress.AddressValueError:
            # Skip invalid addresses
            continue
    
    return ipv6_subnets

logfile_path = '/Users/agiannakou/Downloads/xrdlog_ft_7.txt'

unique_hostnames, total_size, start_time, end_time, line_count = extract_successful_ips_and_hostnames(logfile_path)



print(f"Start Time (UTC): {start_time}")
print(f"End Time (UTC): {end_time}")
print(f"Total Bytes Transferred (with SUCCESS): {total_size}")
print(f"Number of lines containing both 'size' and 'SUCCESS': {line_count}")

all_ipv6_subnets = set()

print("\nNSLOOKUP Results:")
for hostname in unique_hostnames:
    print(f"\nHostname: {hostname}")
    ipv6_subnets = get_ipv6_from_hostname(hostname)
    all_ipv6_subnets.update(ipv6_subnets)
    print(f"IPv6 Subnets: {ipv6_subnets}")

# Print unique IPv6 subnets
print("\nUnique IPv6 Subnets:")
ordered_subnets = get_ordered_subnets(all_ipv6_subnets)
for subnet in ordered_subnets:
    print(subnet)
