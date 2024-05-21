#import uproot
import awkward as ak
import logging
import psutil
import time


def monitor(func):
    def wrapper(*args, **kwargs):
        process = psutil.Process()
        cur_func_pid = process.pid
        process = psutil.Process(cur_func_pid)

        func(*args, **kwargs)

        cpu_usage = process.cpu_percent(interval=1)
        memory_usage = process.memory_info().rss / 1024 / 1024 / 1024 # bytes -> GB
        net_counters = process.connections()
        print(f"connections:{net_counters}")
        print(f"CPU usage: {cpu_usage}%")
        print(f"Memory usage: {memory_usage}GB")
        mem = process.memory_percent()
        for child in process.children(recursive=True):
            cpu_usage_child = child.cpu_percent()
            print(f"Child CPU usage: {cpu_usage_child}%")

    return wrapper

@monitor
def workflow():
    stride = 1
    interesting_vars = ["Photon_pt", "Photon_eta", "Photon_phi", "MET_pt", "Jet_pt"]
    ##adding logger for timestamps
    logging.basicConfig(filename='timestamp_logs',filemode='a',
        format='%(asctime)s %(levelname)-8s %(message)s',
        level=logging.INFO,
        datefmt='%Y-%m-%d %H:%M:%S.%f'
    )
    print('la')
    with open("pfns1.txt") as fin:
        file_pfns = list(l.strip() for l in fin)
    for pfn in file_pfns[::stride]:
        print('in')
        logging.info('Opening file {}'.format(pfn))
        #time.sleep(1)
    #exit(0)
        with uproot.open(pfn) as fin:
            some_data = fin["Events"].arrays(interesting_vars)
            for var in interesting_vars:
                mean = ak.mean(some_data[var])
                logging.info("Mean value for {}: {}".format(var,mean))
            logging.info('Read {} bytes from file {}'.format(fin.file.source.num_requested_bytes,pfn))
            logging.info('Opened file {} from remote location'.format(pfn))
            # Check the actual data server
            dataserver: str = fin.file.source.file.get_property("DataServer")
            logging.info('File was served from {}'.format(dataserver))


workflow()