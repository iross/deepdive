#!/usr/bin/python
import time
import os
import pdb
import pymongo
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from harvestResults import *


TAG = "elsevier_002_dev"

TYPE_MAP = {".html": "ocr",
            ".hocr": "ocr",
            ".text": "nlp",
            }

PATTERN_MAP = {"ocr": "*.html",
                "nlp": "*.text",
                }

STRIP_MAP = { "ocr": "_out",
              "nlp": "_NLP_out_NLP",
              }

def parsePath(filepath):
    """
    TODO: Docstring for parsePath.

    :filepath: TODO
    :returns: TODO

    """
    root,extension = os.path.splitext(filepath)
    return root, extension

def getSubmitDir(jobpath, proctype):
    """
    TODO: Docstring for getSubmitDIr.

    :jobpath: TODO
    :proctype: TODO
    :returns: TODO

    """
    basepath = os.path.dirname(jobpath)
    basepath = basepath.replace(STRIP_MAP[proctype], "")
    return basepath

class FileDealer(FileSystemEventHandler):
    """Docstring for FileDealer. """
    # if change is seen in certain filetype (html/hocr, .text, .font?):
    # Add it to the database
    # how do we prevent 90 *.html files coming back at once from triggering this action 90 times?
    # add the subdir to a queue, which then gets slowly drained over time?
    # can I just keep a connection to the DB open indefinitely?
    def __init__(self):
        """TODO: to be defined1. """
        FileSystemEventHandler.__init__(self)
        self.queue = {}
        for jobtype in STRIP_MAP.keys():
            self.queue[jobtype] = []
        client = pymongo.MongoClient()
        articlesdb = client.articles_dev
        self.articlesColl = articlesdb.articles

        procdb = client.processing_dev
        self.ocrProcessingColl = procdb["ocr_processing"]
        self.nlpProcessingColl = procdb["nlp_processing"]


    def process(self):
        """
        TODO: Docstring for process.
        :returns: TODO

        """
        for job in self.queue["ocr"]:
            # filtering -- if NLP-type path is in the output, remove it and move on
            # (implies condor copied input files back)
            if STRIP_MAP["nlp"] in job:
                self.queue["ocr"].remove(job)
                continue

            # need to get the filepath_map, relevant file_pattern for this job
            orig_submit = getSubmitDir(job, "ocr")
            filepath_map = pickle.load(open(orig_submit+"/filepath_mapping.pickle"))
            file_pattern = PATTERN_MAP["ocr"]
            processJob(job, TAG, "ocr", self.articlesColl, self.ocrProcessingColl, filepath_map, file_pattern, dryrun=True)
            self.queue["ocr"].remove(job)
        for job in self.queue["nlp"]:
            orig_submit = getSubmitDir(job, "nlp")
            filepath_map = pickle.load(open(orig_submit+"/filepath_mapping.pickle"))
            file_pattern = PATTERN_MAP["nlp"]
            processJob(job, TAG, "nlp", self.articlesColl, self.nlpProcessingColl, filepath_map, file_pattern, dryrun=True)
            self.queue["nlp"].remove(job)

    def add_to_queue(self, jobpath, jobtype):
        """
        TODO: Docstring for add_to_queue.

        :jobpath: TODO
        :type: TODO
        :returns: TODO

        """
        # make sure it's not here
        if jobpath not in self.queue[jobtype]:
            self.queue[jobtype].append(jobpath)
        pass

    def on_created(self, event):
        root,extension = parsePath(event.src_path)
        if extension == ".debug":
            pdb.set_trace()
        if extension == ".text" or extension == ".html" or extension == ".hocr":
            self.add_to_queue(os.path.dirname(root), TYPE_MAP[extension])
        else:
            pass

if __name__ == "__main__":
    event_handler = FileDealer()
    observer = Observer()
    observer.schedule(event_handler, path='.', recursive=True)
    observer.start()
    proc_n = 1

    try:
        while True:
            time.sleep(1)
            print "Before: ", event_handler.queue
            event_handler.process()
            print "After: ", event_handler.queue
    except KeyboardInterrupt:
        observer.stop()
    observer.join()
