# should be run from ChtcRun directory

# clean up journal name, removing commas and spaces
journal=$1
journal_clean=${journal//,/}
journal_clean=${journal_clean// /_}

joblimit=$3

echo "Running pdf_to_dag with args $2 and submit_$journal_clean"
python ../pdf_to_dag.py "$2" "submit_$journal_clean" $3

# assume ChtcRun/shared exists and holds all common shared junk
for f in submit_$journal_clean*/; do
    cp -rL shared/ $f
    f=${f/\//}
    echo ./mkdag --cmdtorun=ocr_pdf.py --parg=input.*f --parg="--cuneiform" --parg="--tesseract" --parg="--pdf" --data=$f --outputdir="${f/submit_/}"_out --pattern=*.html --type=other
done
