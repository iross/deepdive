#!/bin/sh

####
# We always log which machine does this job run first
####
uname -a

####
# The file cuneiform.zip, ocr2.zip, nlp3.jar, k2pdfopt
# are downloaded from the Squid proxy specified in
# the file ``URLS''.
#   - nlp3.jar: Stanford NLP
####
mv nlp3.jar nlp.jar
tar xvfpz jre-7u51-linux-x64.tar.gz
JAVA_HOME=`pwd`/jre1.7.0_51
export JAVA_HOME

####
# Prepare tmp directory
####
ls
rm -rf tmp
mkdir tmp

cp *hocr.html tmp

####
# Run NLP over Tesseract's result
#   - tmp: folder that contains Tesseract's result
#   - input.text: output file for NLP
####
java -Dfile.encoding=UTF-8 -Xmx8192m -jar nlp.jar tmp input.text

####
# Clean up
#   - bmp file for each page is large, we only maintain png version
#   - remove binary files
####
rm -f tmp/*.bmp
rm -f nlp.jar
rm -rf codes
rm -rf cde-package
rm -f ocr.zip


####
# Prepare the result that will come back
####
mv tmp input
mv input.text input
mv input/* .


####
# The task is labelled as "SUCCEED" if and only if Tesseract runs successfully.
####
if [ -f "input.text" ]
then
    echo "SUCCEED" > SUCCEED.txt
fi


