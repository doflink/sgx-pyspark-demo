# Initialize /fspf/encrypted-files/
mkdir -p encrypted-files
rm -rf encrypted-files/*

# Encrypt files in /fspf/input and store in /fspf/encrypted-files/
scone fspf create encrypted-files/volume.fspf
scone fspf addr encrypted-files/volume.fspf / --not-protected --kernel /
scone fspf addr encrypted-files/volume.fspf /fspf/encrypted-files/ --encrypted --kernel /fspf/encrypted-files/
scone fspf addf encrypted-files/volume.fspf /fspf/encrypted-files/ /fspf/input/ /fspf/encrypted-files/ 
scone fspf encrypt encrypted-files/volume.fspf > input/keytag

# This meta-data will be obtained from the CAS component (see the paper)
export SCONE_FSPF_KEY=$(cat input/keytag | awk '{print $11}')
export SCONE_FSPF_TAG=$(cat input/keytag | awk '{print $9}')
export SCONE_FSPF=/fspf/encrypted-files/volume.fspf
