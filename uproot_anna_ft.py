import subprocess

# Check if the file exists
try:
    with open('pfns.txt', 'r') as file:
        lines = file.readlines()
except FileNotFoundError:
    print("File pfns.txt not found!")
    exit(1)

# Run xrdcp command for each line
for line in lines:
    line = line.strip()  # Remove any leading/trailing whitespace
    if line:  # Check if the line is not empty
        print(f"Processing: {line}")
        command = ["xrdcp", "-f", line, "/dev/null"]
        # Check the actual data server
        #dataserver: str = line.file.source.file.get_property("DataServer")
        #logging.info('File was served from {}'.format(dataserver))
        try:
            subprocess.run(command, check=True)
        except subprocess.CalledProcessError as e:
            print(f"Error running command {command}: {e}")
