import pymongo
import pdb
import os
import subprocess

STAGING = "./dev_staging/"
TARGET = "./dev_target/"

client = pymongo.MongoClient()
articlesdb = client.articles_dev
articles = articlesdb.articles

def createSymlinks(processingDict, type):
    """


    :processingDict: TODO
    :returns: 0 if successful

    """

    # then, gather all the files + processings
    for tag in processingDict.keys():
        # todo: dump metadata?
        doclist[tag] = []
        if "harvested" in processingDict[tag]:
            doclist[tag] = processingDict[tag]["filename"]
        if not os.path.exists(STAGING+"%s_%s/%s/" % (type,tag, article["_id"]) ):
            os.makedirs(STAGING+"%s_%s/%s/" % (type,tag, article["_id"]))
        for file in doclist[tag]:
            try:
                os.symlink(file,STAGING+"%s_%s/%s/" % (type, tag, article["_id"]) + file.split("/")[-1])
            except OSError as e:
                if e.errno == 17:
                    continue
    return 0

# first, find results that meet the query
doclist = {}
for article in articles.find( { "pubname" : "Palaeoworld" } ):
    try:
        ocrs = article["ocr_processing"]
    except KeyError:
        ocrs = {}
    try:
        nlps = article["nlp_processing"]
    except KeyError:
        nlps = {}

    createSymlinks(ocrs,"ocr")
    createSymlinks(nlps,"nlp")


    # then put them all into a staging area (symbolic link)
    #./dev_staging/jobid?


# todo: how to deal with new matches?



# then rsync
# rsync avzL (L will resolve symlink)
# ./dev_target
subprocess.call(["rsync", "-ravzL", STAGING, TARGET])

