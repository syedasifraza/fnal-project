import pandas as pd
from ipaddress import ip_network, ip_address

# Function to extract and print unique subnets from a CSV file
def extract_and_print_unique_subnets(csv_file_path):
    try:
        # Read the CSV file
        df = pd.read_csv(csv_file_path)
        
        unique_subnets = set()

        # Iterate over each row to process IP addresses
        for _, row in df.iterrows():
            ip = row['IP Address']
            try:
                ip_obj = ip_address(ip)
                if ip_obj.version == 4:
                    # For IPv4, group by /24 subnets
                    subnet = ip_network(ip_obj.exploded + '/24', strict=False)
                else:
                    # For IPv6, group by /48 subnets
                    subnet = ip_network(ip_obj.exploded + '/48', strict=False)
                unique_subnets.add(str(subnet))
            except ValueError as e:
                print(f"Skipping invalid IP address: {ip}, Error: {e}")
        
        # Print the unique subnets
        print("Unique subnets found:")
        for subnet in unique_subnets:
            print(subnet)
    
    except FileNotFoundError:
        print(f"File not found: {csv_file_path}")

# Main function
def main():
    csv_file_path = 'ip_ports_subnets.csv'  # Adjust the path if needed
    extract_and_print_unique_subnets(csv_file_path)

if __name__ == "__main__":
    main()
