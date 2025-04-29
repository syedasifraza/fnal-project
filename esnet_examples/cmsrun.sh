#!/usr/bin/env bash

export SCRAM_ARCH=el8_amd64_gcc10
source /cvmfs/cms.cern.ch/cmsset_default.sh
pushd /cvmfs/cms.cern.ch/el8_amd64_gcc10/cms/cmssw-patch/CMSSW_12_4_11_patch3
cmsenv
popd

cmsRun -j test_premix_job_report.xml test_premix_job.py 
