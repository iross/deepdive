wget http://pages.cs.wisc.edu/~iross/ChtcRun_Merlin.tar.gz
tar xzf ChtcRun_Merlin.tar.gz
cd ChtcRun
mkdir shared
cp ../URLS shared/
cp ../ocr_pdf.py shared/
cp ../argparse.py shared/
ln -s ../createJobs.sh .
