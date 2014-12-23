# should be run from ChtcRun directory

# clean up journal name, removing commas and spaces
journal=$1
journal_clean=${journal//,/}
journal_clean=${journal_clean// /_}

python ../deepdive/pdf_to_dag.py ~/DeepDive/downloads/$journal $journal_clean

# assume ChtcRun/shared exists and holds all common shared junk
cp -r shared/ $journal_clean/
./mkdag --cmdtorun=ocr_pdf.py --parg=input.pdf --parg="--cuneiform" --parg="--tesseract" --data=$journal_clean --outputdir="$journal_clean"_out --pattern=*.html --type=other                                                                                 
