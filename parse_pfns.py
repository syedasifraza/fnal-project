import re
import subprocess
import ipaddress

def extract_hostnames(file_path):
    hostname_regex = r'root://([^/:]+)'
    hostnames = set()

    with open(file_path, 'r') as file:
        for line in file:
            match = re.search(hostname_regex, line)
            if match:
                hostnames.add(match.group(1))

    return hostnames

def get_ordered_subnets(unique_subnets):
    # Sort the unique subnets based on their numerical value
    ordered_subnets = sorted(unique_subnets, key=lambda x: ipaddress.IPv6Network(x))
    return ordered_subnets


def get_ipv6_from_hostname(hostname):
    try:
        result = subprocess.run(['nslookup', '-type=AAAA', hostname], capture_output=True, text=True)
    except Exception as e:
        print(f"Error running nslookup for {hostname}: {e}")
        return set()

    ipv6_regex = r'AAAA address\s*([A-Fa-f0-9:]+)'
    ipv6_matches = re.findall(ipv6_regex, result.stdout)
    
    ipv6_addresses = {ip for ip in ipv6_matches if ':' in ip}
    
    return ipv6_addresses

def get_unique_subnets(ipv6_addresses):
    subnets = set()
    for ipv6 in ipv6_addresses:
        try:
            ipv6_obj = ipaddress.IPv6Address(ipv6)
            subnet = ipaddress.IPv6Network(f'{ipv6}/64', strict=False)
            subnets.add(str(subnet))
        except ipaddress.AddressValueError:
            # Skip invalid addresses
            continue
    return subnets


file_path = '/Users/agiannakou/Downloads/pfns_new.txt'  

hostnames = extract_hostnames(file_path)
print("Extracted Hostnames:")
for hostname in hostnames:
    print(hostname)

all_ipv6_addresses = set()
for hostname in hostnames:
    ipv6_addresses = get_ipv6_from_hostname(hostname)
    all_ipv6_addresses.update(ipv6_addresses)

print("\nExtracted IPv6 Addresses:")
for ipv6 in all_ipv6_addresses:
    print(ipv6)

unique_subnets = get_unique_subnets(all_ipv6_addresses)

ordered_subnets = get_ordered_subnets(unique_subnets)



print("\nOrdered IPv6 Subnets:")
for subnet in ordered_subnets:
    print(subnet)