import pickle
import pymongo
import glob
import os,sys
import pdb
import argparse
from datetime import datetime
import pprint

DEBUG=False

def cleanPath(path):
    """
    Cleans up raw path of pdf to just the name of the PDF (no extension or directories)

    :path: TODO
    :returns: TODO

    """
    filename = path.split("/")[-1]
    filename = filename.split(".")[0]
    return filename

def getMatch(jobpath, collection):
    """
    Finds the document in the collection that matches the specified jobpath

    :jobpath: Path to job output directory
    :collection: MongoDB collection to look over
    :returns: dict of matching document

    """
    # Elsevier-specific stuff for matching
    pii = cleanPath(filepath_map[jobpath])
    url = "http://api.elsevier.com/content/article/pii/" + pii
    match = collection.find_one({"URL": url})
    return match

def parseTime(line):
    """
    Parse the time of submission/termination/execution from a line
    expected input is of the type:
        "001 (132663.000.000) 12/29 15:16:50 Job executing on host: <128.105.244.247:35219>"

    :line: TODO
    :returns: TODO

    """
    line = line.split(" ")
    timeString = line[2] + " " + line[3]
    date_object = datetime.strptime(timeString, '%m/%d %H:%M:%S')
    # FOR NOW: if datetime.month = 12, use 2014. Otherwise, 2015
    if date_object.month == 12:
        date_object = date_object.replace(year=2014)
    else:
        date_object = date_object.replace(year=2015)
    return date_object

def parseResources(chunk):
    """
    Takes a chunk of log text that includes the "Partitionable Resources" table
    and returns a parsed dict of {"diskUsage": xxx, "memUsage": yyy }

    :chunk: TODO
    :returns: TODO

    """
    usage = {}
    # break the chunk up by \n
    chunk = chunk.split("\n")
    for line in chunk:
        if "Disk" in line:
            numbers = [int(s) for s in line.split() if s.isdigit()]
            usage["diskUsage"] = numbers[0]
        if "Memory" in line:
            numbers = [int(s) for s in line.split() if s.isdigit()]
            usage["memUsage"] = numbers[0]
    return usage

def readLog(jobid, basedir):
    """
    Reads log to determine disk/mem usage, runtime
    For processing time, it will only grab the last execution/evict/terminated times.
    And runTime supercedes evictTime (eg. an exec->evict combination will not be written if
    a later exec-termination combination exists in the log)

    To be appended to processing database, so that structure is:
    ocr_processing["tag"]["jobs"] = [ {startTime: xxx, execTime: yyy, ... }, {reports from other jobs...} ]

    :jobid: id of the job within the submit/output directories
    :basedir: base directory for job output
    :returns: TODO

    """
    try:
        with open(basedir + "/" + jobid+"/process.log") as file:
            chunk = ""
            subTime = None
            execTime = None
            evictTime = None
            termTime = None
            runTime = None
            jobReport = {}
            jobReport["path"] = basedir + "/" + jobid
            # todo: grab article PDF path?
            for line in file:
                if line.startswith("..."):
                    if chunk.startswith("000"): # submitted
                        jobReport["subTime"] = parseTime(chunk.split('\n')[0])
                    elif chunk.startswith("001"): # executing
                        jobReport["execTime"] = parseTime(chunk.split('\n')[0])
                    elif chunk.startswith("004"): # evicted, has partitionable table
                        jobReport["evictTime"] = parseTime(chunk.split('\n')[0])
                        runTime = (jobReport["evictTime"] - jobReport["execTime"])
                        jobReport["runTime"] = runTime.days * 86400 + runTime.seconds
                        jobReport["usage"] = parseResources(chunk)
                    elif chunk.startswith("005"): # termination, has partitionable table
                        jobReport["termTime"] = parseTime(chunk.split('\n')[0])
                        runTime = (jobReport["termTime"] - jobReport["execTime"])
                        jobReport["runTime"] = runTime.days * 86400 + runTime.seconds
                        jobReport["usage"] = parseResources(chunk)
                    elif chunk.startswith("006"):
                        pass
                    elif chunk.startswith("009"):
                        pass
                    else:
                        if DEBUG:
                            print "UNKNOWN CODE"
                            print chunk
                    chunk=""
                else:
                    chunk+=line
        return jobReport
    except IOError:
        print "Couldn't find file at %s/%s/process.log" % (basedir, jobpath)
        return None

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Harvest some condor output')
    parser.add_argument('output_dir', type=str, default="./", help="Output directory. The files you want to harvest are here.")
    parser.add_argument('submit_dir', type=str, default="./", help="Submit directory. Must contain the filepath_mapping.pickle which \
            maps the job id to the original PDFs")
    parser.add_argument('type', type=str, default="ocr", help="OCR or NLP?")
    parser.add_argument('tag', type=str, default="", help="Tag of this processing batch")
    parser.add_argument('file_pattern', type=str, default="*.html", help="Pattern for matching output files. Default: *.html")
    parser.add_argument('--dryrun', type=bool, required=False, default=False, help="Don't actually write to the databases--only show\
            what would have been added/updated.")
    parser.add_argument('--update', type=bool, required=False, default=False, help="Force update to the database.")
    args = parser.parse_args()
    args.type = args.type.lower()
    if args.type!="ocr" and args.type!="nlp":
        print "Please choose a valid processing type! (ocr or nlp)"
        sys.exit(1)

    client = pymongo.MongoClient()
    articlesdb = client.articles_dev
    articles = articlesdb.articles

    procdb = client.processing_dev
    processings = procdb["%s_processing" % args.type]

    # need this for matching to PDF
    temp={}
    runTimes = [] # temp for plots

    # todo: add toggle to show what WOULD have been written to a DB vs actually write it.
    #todo: make sure these exist before we go too far
    output_dir = args.output_dir
    submit_dir = args.submit_dir


    #technically, we can grab this from the path, based on how I'm structuring my job submission
    tag = args.tag

    # todo: get the filepath_map from the original submit directory
    filepath_map = pickle.load(open(submit_dir + "/filepath_mapping.pickle"))
    output_dir = os.getcwd()+"/" + output_dir
    joblist = glob.glob(output_dir + "/job*/")

    # long-term todo: can each job run this (or something similar) when it comes home?
    for jobpath in joblist:
        jobid = jobpath.split("/")[-2]

        # match against the articles collection in the DB
        match = getMatch(jobid, articles)
        if match is None:
            print "No match for the article in %s!" % jobid
            continue
        try:
            if match["%s_processing" % args.type][tag]["harvested"]:
                if DEBUG:
                    print "The information for this job has already been added to the DB!"
                    print "If you want to update it, too bad! Ian hasn't implemented that yet (todo)"
                if not args.update:
                    continue
        except KeyError:
            pass

        tempReport = readLog(jobid, output_dir)
        if tempReport is None:
            continue

        # todo: how to figure out if a result has already been harvested?
        # idea: we just matched. How about we add a check against match["ocr_processing"][tag]["harvested"]?

        # todo: ... maybe I should just link/embed the document, instead of injecting pieces of it?
        tempReport["pubname"] = match["pubname"]
        tempReport["URL"] = match["URL"]

        try:
            with open(jobpath + "/RESULT") as file:
                for line in file:
                    if line.strip()=="0":
                        tempReport["success"] = True
                    else:
                        tempReport["success"] = False
        except IOError:
            tempReport["success"] = False

        # get all files associated with the job
        temp["filename"] = []
        temp["contents"] = []

        # todo: match (or update) against processing collection
        list = glob.glob(jobpath+"/" + args.file_pattern)
        for file in list:
            temp["filename"].append(file)
#            temp["contents"].append("<Placeholder for if we ever want to store the full contents.>")
        temp["harvested"] = True #indicate that this article+tag combination has been handled
        match["%s_processing" % args.type] = { tag: temp }


        # this will re-add the job reports if they're already in the db, since we're just pushing to a list
        # can probably make an index on "path" and check against it.
        if args.dryrun:
            ppr = pprint.PrettyPrinter(indent=4)
            print "This would have been pushed into %s.%s.jobs: " % (processings.name, tag)
            ppr.pprint(tempReport)

            print "And this would be the new article document: "
            ppr.pprint(match)
        else:
            if not args.update: # don't update the processings database.
                # todo: make it possible to update the databases without duplicating the "jobs" array
                processings.update( { "_id": tag }, { "$push": { "jobs" : tempReport } } )
            articles.update( { "_id" : match["_id"] }, {"$set": match}, upsert = False ) # upsert: false won't create a new one. Since we just looked for it, we should never actually run into it..
        # todo: clean up all other stuff in the output directories?

    """
    # temp make plot of runTimes
    binwidth=1800
    plt.hist(runTimes, bins=range(min(runTimes), 300000, binwidth), log=True)
    plt.title("RunTimes for %s" % args.type.upper())
    plt.xlabel("Time (seconds)")
    plt.ylabel("Frequency")
    plt.axvline(259200, color='r', linestyle='solid')
    plt.savefig('failures_%s' %args.type.upper())
    """
