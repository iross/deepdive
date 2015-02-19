git checkout ians_cde
wget http://chtc.cs.wisc.edu/downloads/ChtcRun.tar.gz
tar xzf ChtcRun.tar.gz
cd ChtcRun
mkdir shared
cp ../URLS shared/
cp ../ocr_pdf.py shared/
cp ../argparse.py shared/
ln -s ../createJobs.sh .
