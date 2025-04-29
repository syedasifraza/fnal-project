#!/usr/bin/env bash

export SCRAM_ARCH=el8_amd64_gcc10
source /cvmfs/cms.cern.ch/cmsset_default.sh
pushd /cvmfs/cms.cern.ch/el8_amd64_gcc10/cms/cmssw-patch/CMSSW_12_4_11_patch3
cmsenv
popd

# rucio list-file-replicas cms:/Neutrino_E-10_gun/Run3Summer21PrePremix-Summer22_124X_mcRun3_2022_realistic_v11-v2/PREMIX --rses T1_US_FNAL_Disk --pfns > fnal_pileup.txt
# <remove pfn prefix>

# Update: use CERN and leave the full pfn prefix
# rucio list-file-replicas cms:/Neutrino_E-10_gun/Run3Summer21PrePremix-Summer22_124X_mcRun3_2022_realistic_v11-v2/PREMIX --rses T2_CH_CERN --protocol root --pfns > cern_pileup.txt

cmsDriver.py  --eventcontent PREMIXRAW --customise Configuration/DataProcessing/Utils.addMonitoring \
  --datatier GEN-SIM-RAW --conditions 124X_mcRun3_2022_realistic_v12 --step DIGI,DATAMIX,L1,DIGI2RAW,HLT:2022v12 \
  --procModifiers premix_stage2,siPixelQualityRawToDigi --geometry DB:Extended --datamix PreMix --era Run3 \
  --python_filename test_premix_job.py --fileout file:premix_output.root \
  --filein "dbs:/MinBias_TuneCP5_13p6TeV-pythia8/Run3Summer22GS-124X_mcRun3_2022_realistic_v10-v1/GEN-SIM" \
  --pileup_input filelist:cern_pileup.txt  \
  --no_exec --mc -n 1800
