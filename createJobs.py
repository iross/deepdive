import pymongo
import os, shutil, sys
import subprocess
import pickle
import argparse
import urllib
import ConfigParser

config = ConfigParser.RawConfigParser()
config.read('./db.cfg')

reader_user = config.get('database', 'reader_user')
reader_password = config.get('database', 'reader_password')
reader_password = urllib.quote_plus(reader_password)

uri = "mongodb://%s:%s@deepdivesubmit.chtc.wisc.edu/?authMechanism=MONGODB-CR" % (reader_user, reader_password)
client = pymongo.MongoClient(uri)
articlesdb = client.articles
articles = articlesdb.articles

def createSymlinks(files, submit_dir, count, type):
    """
    :processingDict: TODO
    :returns: 0 if successful

    """
    # then, gather all the files + processings
    if not os.path.exists(submit_dir+"/job%06d/" % count ):
        os.makedirs(submit_dir+"/job%06d/" % count)
    # todo: make filepath_mapping pickle
    if len(files) > 1 and (type == "ocr" or type=="cuneiform"):
        print "Warning! Exepcted only one input file, but found %s!" % len(files)
    for file in files:
        try:
            if type == "nlp" or type == "fonttype":
                os.symlink(file,submit_dir+"/job%06d/" % (count) + str(os.path.basename(file)) )
            else: # tesseract/cuneiform -- should only be one file
                os.symlink(file,submit_dir+"/job%06d/" % (count) + "input" + os.path.splitext(file)[-1])
        except OSError as e:
            if e.errno == 17:
                print "Error 17!"
                continue
    return 0

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='')
    parser.add_argument('type', type=str, default="ocr", help='Type of processing')
    parser.add_argument('dir', type=str, default="submit_dir", help='Submit directory to create')
    parser.add_argument('tag', type=str, default="elsevier_002", help='Processing tag (used to look up which jobs need to be run).')
    parser.add_argument('--limit', type=int, default=1000, help='Number of jobs to create (default 1000)')

    args = parser.parse_args()

    if args.type not in ["ocr", "nlp", "cuneiform", "fonttype"]:
        print "Please provide a valid processing type (ocr, nlp, cuneiform, fonttype)!"
        sys.exit(1)

    submit_dir = args.dir
    if os.path.exists(submit_dir):
        shutil.rmtree(submit_dir)
    os.mkdir(submit_dir)
    # look for articles that match a query
    count = 1
    filepath_mapping = {}
    query = {}
    # todo: clean up these queries
    type = args.type
    tag = args.tag
    if type == "ocr":
        query = { "ocr_processing.%s" % tag:{"$exists": False} }
    elif type == "cuneiform":
        query = { "cuneiform_processing.%s" % tag:{"$exists": False} }
    elif type == "nlp":
        query = { "ocr_processing.%s.harvested" % tag:True, "ocr_processing.%s.filename" %tag:{"$ne": [] }, "nlp_processing.%s" % tag: {"$exists": False}}
    elif type == "fonttype":
        query = { "cuneiform_processing.%s.harvested" % tag:True, "cuneiform_processing.%s.filename" % tag:{"$ne": []}, "fonttype_processing.%s" % tag: {"$exists": False}}

    articles_list = articles.find(query).limit(args.limit)
    for article in articles_list:
        # todo: create a filepath_mapping.pickle
        if type == "ocr":
            try:
                filelist = [article["filepath"]]
            except KeyError: # happened to grab an article before the 'filepath' was written
                continue
        if type == "cuneiform":
            try:
                filelist = [article["filepath"]]
            except KeyError: # happened to grab an article before the 'filepath' was written
                continue
        elif type == "nlp":
            filelist = article["ocr_processing"][tag]["filename"]
        elif type == "fonttype":
            filelist = article["cuneiform_processing"][tag]["filename"]
        if filelist == []:
            continue
        check = createSymlinks(filelist, submit_dir, count, type)
        filepath_mapping["job%06d" % count] = article["filepath"]
        count += 1
    with open(submit_dir+"/filepath_mapping.pickle","wb") as f:
        pickle.dump(filepath_mapping, f )
    if type == "ocr" or type=="cuneiform":
        shutil.copytree("./shared",submit_dir+"/shared/")
        print "Submit directories prepared! Use mkdag to create the DAGs, passing relevant runtime arguments. e.g.:"
        print "./mkdag --cmdtorun=ocr_pdf.py --parg=input.pdf --parg=\"--cuneiform\" --parg=\"--no-tesseract\" --data=%s --output=%s_out --pattern=*.html --type=other" % (submit_dir, submit_dir)
    elif type == "nlp":
        shutil.copytree("./NLPshared",submit_dir+"/shared/")
        print "Submit directories created from requested output! Use mkdag to create DAG files for submission. e.g.:"
        print "./mkdag --cmdtorun=do.sh --data=%s --outputdir=%s_out_NLP --pattern=SUCCEED.txt --type=other" % (submit_dir, submit_dir)
    elif type == "fonttype":
        shutil.copytree("./fontshared",submit_dir+"/shared/")
        print "Submit directories created from requested output! Use mkdag to create DAG files for submission. e.g.:"
        print "./mkdag --cmdtorun=do.sh --data=%s --outputdir=\"%s\"_out_FontType --pattern=SUCCEED.txt --type=other" % (submit_dir, submit_dir)
