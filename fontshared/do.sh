
####
# This is the Condor script that we used to detect layout and fonts from Cuneiform's results.
####

####
# Prepare working directory
####
mkdir rs

####
# Copy Cuneiform's result into the working directory
# Copy NLP result into the working directory
####
cp cune*html rs

####
# Run layout analysis script
####
python cuneiform_fonttype/do.py rs/

####
# Prepare output
####
cp rs/* .
rm cuneiform-page-*

echo ":-)" > SUCCEED.txt
