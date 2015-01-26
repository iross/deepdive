#!/usr/bin/python
import time
import os
import pdb
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from harvestResults import *


TAG = "elsevier_002_dev"

TYPE_MAP = {".html": "ocr",
            ".hocr": "ocr",
            ".text": "nlp",
            }

STRIP_MAP = { "ocr": "_out",
              "nlp": "NLP_out_NLP",
              }

def parsePath(filepath):
    """
    TODO: Docstring for parsePath.

    :filepath: TODO
    :returns: TODO

    """
    root,extension = os.path.splitext(filepath)
    return root, extension

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

    def process(self):
        """
        TODO: Docstring for process.
        :returns: TODO

        """
        for job in self.queue["ocr"]:
            processJob(job, "ocr")
            # filemap path pickle needed to extract PII/URL
            pass
        for job in self.queue["nlp"]:
            pass
        pass

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

    def on_modified(self, event):
        root,extension = parsePath(event.src_path)
        if extension == ".test":
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

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()
    observer.join()
