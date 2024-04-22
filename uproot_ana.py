import uproot
import awkward as ak
import logging



stride = 1
interesting_vars = ["Photon_pt", "Photon_eta", "Photon_phi", "MET_pt", "Jet_pt"]
##adding logger for timestamps
logging.basicConfig(filename='timestamp_logs',
                    filemode='a',
    format='%(asctime)s %(levelname)-8s %(message)s',
    level=logging.INFO,
    datefmt='%Y-%m-%d %H:%M:%S.%f'
)

if __name__ == "__main__":
    with open("pfns1.txt") as fin:
        file_pfns = list(l.strip() for l in fin)

    for pfn in file_pfns[::stride]:
        logging.info('Opening file {}'.format(pfn))
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
