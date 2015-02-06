import pymongo
import pdb
import os, shutil
import subprocess
import pickle

STAGING = "./dev_staging"
TAG = "elsevier_002"
TYPE = "fonttype"
LIMIT = 10

uri = "mongodb://reader:testpass@127.0.0.1/?authMechanism=MONGODB-CR"
client = pymongo.MongoClient(uri)
articlesdb = client.articles
articles = articlesdb.articles

def createSymlinks(files, count, type):
    """
    :processingDict: TODO
    :returns: 0 if successful

    """
    # then, gather all the files + processings
    if not os.path.exists(STAGING+"/job%06d/" % count ):
        os.makedirs(STAGING+"/job%06d/" % count)
    # todo: make filepath_mapping pickle
    if len(files) > 1 and (type == "ocr" or type=="cuneiform"):
        print "Warning! Exepcted only one input file, but found %s!" % len(files)
    for file in files:
        try:
            if type == "nlp" or type == "fonttype":
                os.symlink(file,STAGING+"/job%06d/" % (count) + str(os.path.basename(file)) )
            else: # tesseract/cuneiform -- should only be one file
                os.symlink(file,STAGING+"/job%06d/" % (count) + "input" + os.path.splitext(file)[-1])
        except OSError as e:
            if e.errno == 17:
                continue
    return 0

if __name__ == '__main__':
    if os.path.exists(STAGING):
        shutil.rmtree(STAGING)
    os.mkdir(STAGING)
    # look for articles that match a query
    count = 1
    filepath_mapping = {}
    query = {}
    # todo: clean up these queries
    if TYPE == "ocr":
        query = { "ocr_processing.%s" % TAG:{"$exists": False} }
    elif TYPE == "cuneiform":
        query = { "cuneiform_processing.%s" % TAG:{"$exists": False} }
    elif TYPE == "nlp":
        query = { "ocr_processing.%s.harvested" % TAG:True, "nlp_processing.%s" % TAG: {"$exists": False}}
        query = { "ocr_processing.%s.harvested" % TAG:True, "nlp_processing.%s" % TAG: {"$exists": False}}
    elif TYPE == "fonttype":
        query = { "cuneiform_processing.%s.harvested" % TAG:True, "fonttype_processing.%s" % TAG: {"$exists": False}}

    articles_list = articles.find(query).limit(LIMIT)
    for article in articles_list:
        # todo: create a filepath_mapping.pickle
        if TYPE == "ocr":
            filelist = [article["filepath"]]
        if TYPE == "cuneiform":
            filelist = [article["filepath"]]
        elif TYPE == "nlp":
            filelist = article["ocr_processing"][TAG]["filename"]
        elif TYPE == "fonttype":
            filelist = article["cuneiform_processing"][TAG]["filename"]
        if filelist == []:
            continue
        check = createSymlinks(filelist, count, TYPE)
        count += 1
        filepath_mapping["job%06d" % count] = article["filepath"]
    with open(STAGING+"/filepath_mapping.pickle","wb") as f:
        pickle.dump(filepath_mapping, f )
#    if not os.path.exists(STAGING+"shared/"):
#        os.mkdir(STAGING+"shared/")
    if TYPE == "ocr" or TYPE=="cuneiform":
        shutil.copytree("/home/iaross/elsevier_002_cuneiform/ChtcRun/shared",STAGING+"/shared/")
        print "Submit directories prepared! Use mkdag to create the DAGs, passing relevant runtime arguments. e.g.:"
        print "./mkdag --cmdtorun=ocr_pdf.py --parg=input.pdf --parg=\"--cuneiform\" --parg=\"--no-tesseract\" --data=%s --output=%s_out --pattern=*.html --type=other" % (STAGING, STAGING)
    elif TYPE == "nlp":
        shutil.copytree("/home/iaross/elsevier_002_cuneiform/ChtcRun/NLPshared",STAGING+"/shared/")
        print "Submit directories created from requested output! Use mkdag to create DAG files for submission. e.g.:"
        print "./mkdag --cmdtorun=do.sh --data=%s --outputdir=\"%s\"_out_NLP --pattern=SUCCEED.txt --type=other" % (STAGING, STAGING)
    elif TYPE == "fonttype":
        shutil.copytree("/home/iaross/elsevier_002_cuneiform/ChtcRun/fontshared",STAGING+"/shared/")
        print "Submit directories created from requested output! Use mkdag to create DAG files for submission. e.g.:"
        print "./mkdag --cmdtorun=do.sh --data=%s --outputdir=\"%s\"_out_FontType --pattern=SUCCEED.txt --type=other" % (STAGING, STAGING)

    # todo: create shared directory too
