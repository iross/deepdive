#!/usr/bin/env python
# encoding: utf-8

"""
File: harvestResults_postscript.py
Author: Ian Ross
Email: iross@cs.wisc.edu
Github: https://github.com/iross
Description:
    Modifying the cron'ed harvestResults script to run as a postscript on the htcondor jobs
    TODO: changes needed:
        Don't need to loop over dirs
        Read pickle from output dir (it should have been copied over)
        Write RESULT with 0 or 1 based on PATTERN input
        Better handling of --update junk
        no args (or at least fewer)
        Overall cleanup
"""


import cPickle as pickle
import pymongo
import ConfigParser
import glob
import os,sys
import argparse
from datetime import datetime
import time
import pprint
import re
import pdb

DEBUG=False
VERBOSE=False

STRIP_MAP = { "ocr": "_out",
              "nlp": "_out_NLP",
              "cuneiform": "_out",
              "fonttype": "_out_FontType",
              }

PATTERN_MAP = {"ocr": "*.html",
                "nlp": "*.text",
                "cuneiform": "*.html",
                "fonttype": "*.text",
                }


BASE = "/home/iaross/DeepDive/deepdive/"

def patternMatch(pattern, dir='./'):
    """
    Returns list of files matching the desired input. Matches via regex.

    :pattern: TODO
    :returns: TODO

    """
    files = []
    files = glob.glob(dir+pattern)
    return files

def cleanPath(path):
    """
    Cleans up raw path of pdf to just the name of the PDF (no extension or directories)

    :path: TODO
    :returns: TODO

    """
    filename = path.split("/")[-1]
    filename = filename.split(".")[0]
    return filename

def getMatch(jobid, collection, filepath_map):
    """
    Finds the document in the collection that matches the specified jobpath

    :jobid: ID of job output directory (i.e. job000001)
    :collection: MongoDB collection to look over
    :returns: dict of matching document

    """
    # Elsevier-specific stuff for matching
    pii = cleanPath(filepath_map[jobid])
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

def readLog(jobpath):
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
        with open(jobpath + "/process.log") as file:
            chunk = ""
            subTime = None
            execTime = None
            evictTime = None
            termTime = None
            runTime = None
            jobReport = {}
            jobReport["path"] = jobpath
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
        if VERBOSE:
            print "Couldn't find file at %s/process.log" % jobpath
        return None

def processJob(match, jobpath, tag, proctype, articlesColl, filepath_map, file_pattern, dryrun=False, update=False):
    """
    Take an article matched in the DB, its associated job path, and update the relevant article collections with files found.
    """
    # we have output, have we processed this article using this proctype+tag before?

    files = patternMatch(file_pattern, jobpath)
    if files == []: # no matching output. May be overall fail, may have just not run this proctype
        return 1,False

    try:  # if this article + tag have already been harvested, then skip
        if match["%s_processing" % proctype][tag]["harvested"]:
            if DEBUG:
                print "The information for this job has already been added to the DB!"
            if not update: # if UPDATE flag isn't used, move to the next job
                return 2,False
    except KeyError:
        pass

    # get all files associated with the job
    temp = {}
    temp["filename"] = files
    temp["contents"] = []

    temp["harvested"] = True #indicate that this article+tag combination has been handled
    temp["success"] = True # we found files, so it's a success!
    match["%s_processing" % proctype][tag] = temp

    # db updates
    # this will re-add the job reports if they're already in the db, since we're just pushing to a list
    # can probably make an index on "path" and check against it.
    if dryrun:
        ppr = pprint.PrettyPrinter(indent=4)
        ppr.pprint(tempReport)

        print "And this would be the new article document: "
        ppr.pprint(match)
    else:
        articlesColl.update( { "_id" : match["_id"] }, {"$set": match}, upsert = False )

    # todo: clean up all other stuff in the output directories?
    return 0, temp["success"]

def exit(success):
    """
    Exit. If success, write 0 to RESULT and quite with sys.exist(0). Otherwise, write 1 and use non-zero exit.

    :success: TODO
    :returns: TODO

    """
    with open("RESULT","w") as fout:
        if success:
            fout.write("0\n")
            sys.exit(0)
        else:
            fout.write("1\n")
            sys.exit(1)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Harvest some condor output')
    parser.add_argument('tag', type=str, default="", help="Tag of this processing batch")
    parser.add_argument('--basedir', type=str, default=os.getcwd(), help="The base directory")
    parser.add_argument('--dryrun', type=bool, required=False, default=False, help="Don't actually write to the databases--only show\
            what would have been added/updated.")
    parser.add_argument('--update', type=bool, required=False, default=True, help="Force update to the database.")
    args = parser.parse_args()

    config = ConfigParser.RawConfigParser()
    config.read(BASE+'db_conn.cfg')
    harvest_user = config.get('database','harvest_user')
    harvest_password = config.get('database','harvest_password')
    uri = "mongodb://%s:%s@deepdivesubmit.chtc.wisc.edu:27017/?authMechanism=MONGODB-CR" % (harvest_user, harvest_password)
    client = pymongo.MongoClient(uri)
    articlesdb = client.articles_harvesting_overhaul
    articles = articlesdb.articles

    procdb = client.processing_dev

    #technically, we can grab this from the path, based on how I'm structuring my job submission
    tag = args.tag

    processingsColl = procdb[tag]

    # Should be getting run in output/job0000001
    # get filepath_mapping.pickle from output
    try:
        filepath_map = pickle.load(open("../filepath_mapping.pickle"))
    except IOError:
        print "No filepath mapping found! Cannot determine what article this job belongs to! Exiting!"
        exit(False)
    jobpath = os.getcwd()

    # ----- match article here
    # match against the articles collection in the DB
    if not jobpath.endswith("/"): #ensure we have a trailing /
        jobpath = jobpath + "/"
    jobid = jobpath.split("/")[-2]

    match = getMatch(jobid, articles, filepath_map)

    if match is None:
        if VERBOSE:
            print "No match for the article found! Job id: %s\nfilepath_map: %s"% \
                ( jobid, filepath_map[jobid] )
        exit(False)

    check = None
    jobSuccess = None
    job_config = ConfigParser.RawConfigParser()
    job_config.read(BASE+'jobs.cfg')
    overall_success = False
    with open("post.log","w") as fout:
        fout.write('TAG: %s\n' % tag)
        for proctype in job_config.sections():
            pattern = job_config.get(proctype, 'pattern')
            processings = procdb["%s_processing" % proctype]

            check,jobSuccess = processJob(match, jobpath, args.tag, proctype, articles, filepath_map, pattern, args.dryrun, args.update)
            fout.write(proctype+"\n")
            fout.write('\t%s -- %s\n' % (check, jobSuccess))
            if jobSuccess:
                overall_success = True

    # read log file
    tempReport = readLog(jobpath)
    if tempReport is None:
        exit(False)
    try:
        tempReport["runTime"]
    except KeyError: # no runTime reported -- probably still running
        exit(False)

    # need to grab the pubname from somewhere.
    tempReport["pubname"] = match["pubname"]

    if overall_success:
        processingsColl.update({ "tag": tag }, {'$inc': {"pub_totals.%s.success" % tempReport["pubname"] : 1 } }, upsert=True )
        processingsColl.update({ "tag": tag }, {'$inc': {"pub_totals.%s.cpusuccess" % tempReport["pubname"]: tempReport["runTime"]} }, upsert = True )
    else:
        processingsColl.update({ "tag": tag },
                {'$inc': {"pub_totals.%s.failure" % tempReport["pubname"] : 1 } }, upsert = True)
        processingsColl.update({ "tag": tag },
                {'$inc': {"pub_totals.%s.cpufailure" % tempReport["pubname"]: tempReport["runTime"]} } , upsert = True)

        if overall_success:
            exit(True)
        else:
            exit(False)
