# should be run from ChtcRun directory

# clean up journal name, removing commas and spaces
journal=$1
journal_clean=${journal//,/}
journal_clean=${journal_clean// /_}

echo "Running pdf_to_dag with args $2 and submit_$journal_clean"
python ../deepdive/pdf_to_dag.py "$2" "submit_$journal_clean"

# assume ChtcRun/shared exists and holds all common shared junk
cp -rL shared/ submit_$journal_clean/
echo ./mkdag --cmdtorun=ocr_pdf.py --parg=input.*f --parg="--cuneiform" --parg="--tesseract" --parg="--pdf" --data=submit_$journal_clean --outputdir="$journal_clean"_out --pattern=*.html --type=other
