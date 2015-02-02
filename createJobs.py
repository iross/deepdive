import pymongo
import pdb
import os
import subprocess

STAGING = "./dev_staging/"
TYPE = "ocr"

client = pymongo.MongoClient()
articlesdb = client.articles_dev
articles = articlesdb.articles

def createSymlinks(files, count, type):
    """
    :processingDict: TODO
    :returns: 0 if successful

    """
    # then, gather all the files + processings
    if not os.path.exists(STAGING+"job%06d/" % count ):
        os.makedirs(STAGING+"job%06d/" % count)
    for file in files:
        try:
            pdb.set_trace()
            os.symlink(file,STAGING+"job%06d/" % (count) + "input" + os.path.splitext(file)[-1])
        except OSError as e:
            if e.errno == 17:
                continue
    return 0

# first, find results that meet the query
doclist = {}
# look for articles that match a query
count = 0
for article in articles.find( { "pubname" : "Palaeoworld" } ):
    # move symlinks into a staging area (STAGING/job000xxx/filename.ext)
    filelist = [article["filepath"]]
    pdb.set_trace()
    check = createSymlinks(filelist, count, TYPE)
    count += 1
    # name them input.pdf
    # create a filepath_mapping.pickle

    # but we also want to be able to submit NLP jobs,
    # so add flag (?) to symlink the OCR files
    pass

