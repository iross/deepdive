# should be run from ChtcRun directory
# needs to: 
    # create submit dir
    # set up proper shared/ stuff
    # loop over _out dirs, copying tess*.hocr over
    # run mkdag

# clean up journal name, removing commas and spaces
journal=$1
journal_clean=${journal//,/}
journal_clean=${journal_clean// /_}
journal_out="$journal_clean"_out
journal_new="$journal_clean"_FontType

for job in "$journal_out"/job*/;
do
    if ls $job/*.html 1> /dev/null 2>&1; then
        mkdir -p $journal_new${job/$journal_out/}
        ln -s `pwd`/$job/*.html $journal_new${job/$journal_out/}
    fi
done

#carry the filepath mapping pickle along for easier bookkeeping
cp $journal_clean/filepath_mapping.pickle $journal_new/

# assume ChtcRun/NLPshared exists and holds all common shared junk
cp -r fontshared $journal_new
mv $journal_new/fontshared $journal_new/shared
echo "Submit directories created from requested output! Use mkdag to create DAG files for submission. e.g.:"
./mkdag --cmdtorun=do.sh --data=$journal_new --outputdir="$journal_new"_out_FontType --pattern=SUCCEED.txt --type=other                                                                                 
