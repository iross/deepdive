import pickle
import pymongo
import glob
import os,sys
import pdb
import argparse
from datetime import datetime
import pprint

DEBUG=False

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Harvest some condor output')
#    parser.add_argument('output_dir', type=str, default="./", help="Output directory. The files you want to harvest are here.")
#    parser.add_argument('submit_dir', type=str, default="./", help="Submit directory. Must contain the filepath_mapping.pickle which \
#            maps the job id to the original PDFs")
    parser.add_argument('type', type=str, default="ocr", help="OCR or NLP?")
    parser.add_argument('tag', type=str, default="", help="Tag of this processing batch")
#    parser.add_argument('file_pattern', type=str, default="*.html", help="Pattern for matching output files. Default: *.html")
#    parser.add_argument('--dryrun', type=bool, required=False, default=False, help="Don't actually write to the databases--only show\
#            what would have been added/updated.")
#    parser.add_argument('--update', type=bool, required=False, default=False, help="Force update to the database.")
    args = parser.parse_args()
    args.type = args.type.lower()
    if args.type!="ocr" and args.type!="nlp":
        print "Please choose a valid processing type! (ocr or nlp)"
        sys.exit(1)

    client = pymongo.MongoClient()
    articlesdb = client.articles_dev
    articles = articlesdb.articles
    pubnames = articles.distinct("pubname")

    procdb = client.processing_dev
    processings = procdb["%s_processing" % args.type]
    pprt = pprint.PrettyPrinter(indent=3)
    pubname_totals = processings.aggregate([
          {"$unwind": "$jobs"},
          {"$group":
              { "_id": '$jobs.pubname'  ,
                  'total': { "$sum": 1 }
                  }
              } ])
    pubDict = {}
    for pub in pubnames:
        pdb.set_trace()
        totals = processings.aggregate([
              {"$unwind": "$jobs"},
              {"$match": {"jobs.pubname": pub} },
              {"$group":
                  { "_id": '$jobs.success' ,
                      'total': { "$sum": 1 }
                      }
        } ])

        pubDict["pubname"] = pub
        pubDict["totals"]={}
        for success in totals['result']:
            if success['_id']:
                pubDict["totals"]["success"] = success['total']
            else:
                pubDict["totals"]["failures"] = success['total']

        pubDict["cputotals"]={}
        cputotals = processings.aggregate([
              {"$unwind": "$jobs"},
              {"$match": {"jobs.pubname": pub } },
              {"$group":
                  { "_id": '$jobs.success' ,
                      'total': { "$sum": '$jobs.runTime' }
                      }
        } ])
        for success in cputotals['result']:
            if success['_id']:
                pubDict["cputotals"]["success"] = success['total']
            else:
                pubDict["cputotals"]["failures"] = success['total']
        print "------- %s ------- " % pub
        print "Successes: %s in %0.2f minutes" % (pubDict["totals"]["success"], pubDict["cputotals"]["success"]/60.)
        print "Failures: %s in %0.2f minutes" % (pubDict["totals"]["failures"], pubDict["cputotals"]["failures"]/60.)
        print "Failures were %0.2f%% of computing time." % (100*pubDict["cputotals"]["failures"]/
                float(pubDict["cputotals"]["failures"] + pubDict["cputotals"]["success"]))
      # now totals["result"] is a list of { id: {pubname: xxx, success: true/false}, total: n } dicts
