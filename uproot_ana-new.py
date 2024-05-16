import uproot
import awkward as ak

stride = 10
chunk_size = 100_000
interesting_vars = ["Photon_pt", "Photon_eta", "Photon_phi", "MET_pt", "Jet_pt"]

if __name__ == "__main__":
    with open("pfns.txt") as fin:
        file_pfns = list(l.strip() for l in fin)

    for pfn in file_pfns[:20:stride]:
        # also uproot.iterate could be used instead of manually iterating through chunks
        with uproot.open(pfn) as fin:
            tree = fin["Events"]
            for start in range(0, tree.num_entries, chunk_size):
                some_data = tree.arrays(interesting_vars, entry_start=start, entry_stop=start + chunk_size)
                for var in interesting_vars:
                    mean = ak.mean(some_data[var])
                    print(f"Mean value for {var}: {mean}")
            print(f"Read {fin.file.source.num_requested_bytes} bytes from file {pfn}")
            # Check the actual data server
            dataserver: str = fin.file.source.file.get_property("DataServer")
            print(f"File was served from {dataserver}")

