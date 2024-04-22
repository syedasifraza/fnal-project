import uproot
import awkward as ak
import logging



stride = 1
interesting_vars = ["Photon_pt", "Photon_eta", "Photon_phi", "MET_pt", "Jet_pt"]
##adding logger for timestamps
logging.basicConfig(
    format='%(asctime)s %(levelname)-8s %(message)s',
    level=logging.INFO,
    datefmt='%Y-%m-%d %H:%M:%S'
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
                print(f"Mean value for {var}: {mean}")
            print(f"Read {fin.file.source.num_requested_bytes} bytes from file {pfn}")
            logging.info('Opened file {} from remote location'.format(pfn))
            # Check the actual data server
            dataserver: str = fin.file.source.file.get_property("DataServer")
            print(f"File was served from {dataserver}")

