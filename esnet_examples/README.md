# Environment
Initial setup:
```
source /cvmfs/cms.cern.ch/cmsset_default.sh
export SCRAM_ARCH=el8_amd64_gcc12
cmsrel CMSSW_14_0_0
pushd CMSSW_14_0_0
cmsenv
scram-venv
cmsenv
popd
```
Then copy the python files from this area

Re-visiting:
```
source /cvmfs/cms.cern.ch/cmsset_default.sh
pushd CMSSW_14_0_0 && cmsenv && popd
```

# Example analysis task with uproot
Run `python3 uproot_ana.py`

Run `XRD_LOGLEVEL=Debug XRD_LOGFILE=xrdlog.txt python3 uproot_ana.py` to store xrootd logs

# Example analysis task with RDataFrame
Run `python3 rdf_ana.py`
Not sure how to detect the data servers other than setting
the environment variable `XRD_LOGLEVEL=Debug` (lots of messages)

# Example production premixing task
Run `./cmsrun.sh`.
The test_premix_job.py was generated using the cmsdriver.sh script, but it takes a bit to
run that and it does not need to be run repeatedly.
