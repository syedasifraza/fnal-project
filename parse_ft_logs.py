import re
import pandas as pd
import sys
from ipaddress import ip_network, ip_address

log_pattern = re.compile(r'\[(?P<timestamp>[\d-]+\s[\d:.]+)\s(?P<timezone>[\+\-\d]+)\]\[(?P<level>\w+)\s+\]\[(?P<module>\w+)\s+\]\s(?P<message>.+)')

ip_port_pattern = re.compile(
    r'(?P<ipv4>\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}):(?P<port4>\d{1,5})|'  # IPv4 with port
    r'(?P<ipv6>\[[a-fA-F0-9:]+\]):(?P<port6>\d{1,5})|'                    # IPv6 with port (in brackets)
    r'(?P<ipv6_nobracket>[a-fA-F0-9:]+):(?P<port6_nobracket>\d{1,5})'     # IPv6 with port (no brackets)
)

# Function to parse log file and extract unique IPs (IPv4/IPv6) with port numbers
def parse_log_file(file_path):
    unique_ips_ports = set()
    
    with open(file_path, 'r') as file:
        for line in file:
            if "Attempting connection to" in line:
                match = log_pattern.match(line)
                if match:
                    log_entry = match.groupdict()
                    ip_port_matches = ip_port_pattern.findall(log_entry['message'])
                    
                    # Extract unique IPs with port numbers
                    for match in ip_port_matches:
                        ipv4, port4, ipv6, port6, ipv6_nobracket, port6_nobracket = match
                        if ipv4 and port4:
                            unique_ips_ports.add((ipv4, port4))
                        elif ipv6 and port6:
                            # Remove brackets from IPv6 if present
                            unique_ips_ports.add((ipv6.strip('[]'), port6))
                        elif ipv6_nobracket and port6_nobracket:
                            unique_ips_ports.add((ipv6_nobracket, port6_nobracket))
    
    return list(unique_ips_ports)

# Function to find subnets and associate them with IPs and ports
def find_subnets(unique_ips_ports):
    ip_subnet_mapping = []

    for ip, port in unique_ips_ports:
        try:
            ip_obj = ip_address(ip)
            if ip_obj.version == 4:
                # For IPv4, group by /24 subnets
                subnet = ip_network(ip_obj.exploded + '/24', strict=False)
            else:
                # For IPv6, group by /48 subnets
                subnet = ip_network(ip_obj.exploded + '/48', strict=False)
            ip_subnet_mapping.append((ip, port, str(subnet)))
        except ValueError as e:
            print(f"Skipping invalid IP address: {ip}, Error: {e}")

    return ip_subnet_mapping

def main():
    if len(sys.argv) != 2:
        print("Usage: python parse_logs.py <log_file>")
        sys.exit(1)

    log_file_path = sys.argv[1]
    unique_ips_ports = parse_log_file(log_file_path)
    ip_subnet_mapping = find_subnets(unique_ips_ports)

    # Create a DataFrame with IPs, ports, and associated subnets
    df = pd.DataFrame(ip_subnet_mapping, columns=['IP Address', 'Port', 'Subnet'])

    # Print IPs, ports, and associated subnets
    print(df)

    # Save the DataFrame to a CSV file
    output_file = 'ip_ports_subnets.csv'
    df.to_csv(output_file, index=False)

    print(f"IPs, ports, and associated subnets saved to {output_file}")

if __name__ == "__main__":
    main()
