"""
Given a list of OutputModulesLFNBases (from ReqMgr2), list
the total data volume, number of directories and files in
the CERN EOS storage (T2_CH_CERN).
"""
import subprocess

LFNs = [
    "/store/unmerged/RunIIFall17wmLHEGS/Z2JetsToNuNu_M-50_LHEZpT_150-250_TuneCP5_13TeV-amcnloFXFX-pythia8/LHE/93X_mc2017_realistic_v3_ext1-v2",
    "/store/unmerged/RunIIFall17MiniAODv2/Z2JetsToNuNu_M-50_LHEZpT_150-250_TuneCP5_13TeV-amcnloFXFX-pythia8/MINIAODSIM/PU2017_12Apr2018_94X_mc2017_realistic_v14_ext1-v2",
    "/store/unmerged/RunIIFall17wmLHEGS/Z2JetsToNuNu_M-50_LHEZpT_150-250_TuneCP5_13TeV-amcnloFXFX-pythia8/GEN-SIM/93X_mc2017_realistic_v3_ext1-v2",
    "/store/unmerged/RunIIFall17DRPremix/Z2JetsToNuNu_M-50_LHEZpT_150-250_TuneCP5_13TeV-amcnloFXFX-pythia8/AODSIM/PU2017_94X_mc2017_realistic_v11_ext1-v2",
    "/store/unmerged/RunIIFall17DRPremix/Z2JetsToNuNu_M-50_LHEZpT_150-250_TuneCP5_13TeV-amcnloFXFX-pythia8/GEN-SIM-RAW/PU2017_94X_mc2017_realistic_v11_ext1-v2",
    "/store/unmerged/RunIIFall17NanoAOD/Z2JetsToNuNu_M-50_LHEZpT_150-250_TuneCP5_13TeV-amcnloFXFX-pythia8/NANOAODSIM/PU2017_12Apr2018_94X_mc2017_realistic_v14_ext1-v2"
]

for lfnBase in LFNs:
    command = ["eos", "info", lfnBase, "-m"]
    p = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    out, err = p.communicate()
    size = [item for item in out.split() if "treesize" in item]
    size = int(size[0].replace("treesize=", "")) / (1024 * 1024 * 1024)  # GB

    command = ["eos", "find", "--count", lfnBase]
    p = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    out, err = p.communicate()
    result = out
    print "%s\ntotalsize=%d GB  %s" % (lfnBase, size, result)
