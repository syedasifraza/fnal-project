import uproot
import awkward as ak

stride = 1
interesting_vars = ["Photon_pt", "Photon_eta", "Photon_phi", "MET_pt", "Jet_pt"]

if __name__ == "__main__":
    with open("pfns1.txt") as fin:
        file_pfns = list(l.strip() for l in fin)

    for pfn in file_pfns[::stride]:
        with uproot.open(pfn) as fin:
            some_data = fin["Events"].arrays(interesting_vars)
            for var in interesting_vars:
                mean = ak.mean(some_data[var])
                print(f"Mean value for {var}: {mean}")
            print(f"Read {fin.file.source.num_requested_bytes} bytes from file {pfn}")
            # Check the actual data server
            dataserver: str = fin.file.source.file.get_property("DataServer")
            print(f"File was served from {dataserver}")

