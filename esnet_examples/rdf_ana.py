import ROOT


stride = 10
interesting_vars = ["Photon_pt", "Photon_eta", "Photon_phi", "MET_pt", "Jet_pt"]

if __name__ == "__main__":
    # verbosity = ROOT.Experimental.RLogScopedVerbosity(ROOT.Detail.RDF.RDFLogChannel(), ROOT.Experimental.ELogLevel.kInfo)
    with open("pfns.txt") as fin:
        file_pfns = list(l.strip() for l in fin)

    for pfn in file_pfns[::stride]:
        rdf = ROOT.RDataFrame("Events", pfn)
        means = [rdf.Mean(var) for var in interesting_vars]
        # the first access will trigger the compute
        for var, mean in zip(interesting_vars, means):
            print(f"Mean value for {var}: {mean.GetValue()}")
        print(f"Finished reading from file {pfn}")
