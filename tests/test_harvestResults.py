from harvestResults import *
import unittest
import hashlib
import datetime
import pickle
import os,shutil
import pymongo
from bson.objectid import ObjectId

class TestDownloadManager(unittest.TestCase):

    """Test the downloadManager class"""

    setUpRun = False

    def setUp(self):
        self.client = pymongo.MongoClient()

        articlesdb = self.client.articles_test
        self.articles = articlesdb.articles

        procdb = self.client.processing_test
        self.processings = procdb["ocr_processing"]

        self.jobpath = "./tests/job000001/"
        self.jobid = self.jobpath.split("/")[-2]
        self.filepath_map = pickle.load(open("./tests/filepath_mapping.pickle"))
        self.file_pattern = "*.text"
        self.tag = "elsevier_002"
        self.proctype = "nlp"


        self.resources = "005 (132795.000.000) 12/29 16:25:49 Job terminated.\n\
    (1) Normal termination (return value 0)\n\
        Usr 0 00:03:02, Sys 0 00:00:22  -  Run Remote Usage\n\
        Usr 0 00:00:00, Sys 0 00:00:00  -  Run Local Usage\n\
        Usr 0 00:03:02, Sys 0 00:00:22  -  Total Remote Usage\n\
        Usr 0 00:00:00, Sys 0 00:00:00  -  Total Local Usage\n\
    753231  -  Run Bytes Sent By Job\n\
    378915  -  Run Bytes Received By Job\n\
    753231  -  Total Bytes Sent By Job\n\
    378915  -  Total Bytes Received By Job\n\
    Partitionable Resources :    Usage  Request Allocated\n\
       Cpus                 :                 1         1\n\
       Disk (KB)            :   140726  1000000  25657681\n\
       Memory (MB)          :       11     1000      1000\n"\



    def tearDown(self):
        pass

    def test_clean_path(self):
        """
        Cleans up raw path of pdf to just the name of the PDF (no extension or directories)
        """
        test = "path/to/name.pdf"
        self.assertTrue("name" == cleanPath(test))

    def test_get_match(self):
        """
        """
        # ensure that we find the expected match (with expected document format)

        filepath_map = pickle.load

        match = getMatch(self.jobid, self.articles, self.filepath_map)

        expectedMatch =  {
            "_id" : ObjectId("54b43250e138239d8684b2dd"),
            "sha1" : "35f07525fcf227ee662a98455f42b62212822ac9",
            "title" : "New materials of Sphinxiocarpon, a seed-like organ of putative lycopsid affinity, from the Late Devonian of Hubei, China",
            "vol" : 21,
            "pubname" : "Palaeoworld",
            "URL" : "http://api.elsevier.com/content/article/pii/S1871174X12000078",
            "startingPage" : 131,
            "authKeywords" : "",
            "source" : "Elsevier",
            "authors" : "Xu, Hong-He; Zong, Rui-Wen; Wang, Qi",
            "endingPage" : 136,
            "issue" : 2,
            "filepath" : "/home/iross/DeepDiveEnv/DeepDive/downloads/Palaeoworld/S1871174X12000078.pdf",
            "ocr_processing" : {
                    "elsevier_002" : {
                            "harvested" : True,
                            "contents" : [ ],
                            "filename" : [
                                    "/Users/iross/DeepDiveEnv/prd_files/Palaeoworld_out/job000001/page-1.hocr.html",
                                    "/Users/iross/DeepDiveEnv/prd_files/Palaeoworld_out/job000001/page-2.hocr.html",
                                    "/Users/iross/DeepDiveEnv/prd_files/Palaeoworld_out/job000001/page-3.hocr.html",
                                    "/Users/iross/DeepDiveEnv/prd_files/Palaeoworld_out/job000001/page-4.hocr.html",
                                    "/Users/iross/DeepDiveEnv/prd_files/Palaeoworld_out/job000001/page-5.hocr.html",
                                    "/Users/iross/DeepDiveEnv/prd_files/Palaeoworld_out/job000001/page-6.hocr.html"
                            ]
                    }
            }
        }
        self.assertDictContainsSubset(match, expectedMatch)

    def test_parse_time_2014(self):
        line = "001 (132663.000.000) 12/29 15:16:50 Job executing on host: <128.105.244.247:35219>"
        ret = parseTime(line)
        exp = datetime.datetime(2014,12,29,15,16,50)
        self.assertTrue(ret == exp)

    def test_parse_time_2015(self):
        line = "001 (132663.000.000) 01/15 15:16:50 Job executing on host: <128.105.244.247:35219>"
        ret = parseTime(line)
        exp = datetime.datetime(2015,01,15,15,16,50)
        self.assertTrue(ret == exp)

    def test_parseResources(self):
        ret = parseResources(self.resources)
        exp = { 'diskUsage': 140726, 'memUsage': 11 }
        self.assertTrue(ret == exp)

    def test_parseResources_junk(self):
        ret = parseResources("some\n junk input \n ")
        self.assertTrue(ret == {})

    def test_readLog(self):
        basedir = './tests'
        ret = readLog(basedir + '/' + self.jobid)
        expRuntime =  (datetime.datetime(2014,12,29,16,25,49) -
                        datetime.datetime(2014,12,29,16,21,42))
        usage = {
                'diskUsage': 140726,
                'memUsage': 11
                }
        exp = {
                'path': '%s/%s' % (basedir, self.jobid),
                'subTime': datetime.datetime(2014,12,29,16,19,49),
                'execTime': datetime.datetime(2014,12,29,16,21,42),
                'termTime': datetime.datetime(2014,12,29,16,25,49),
                'runTime' : expRuntime.days * 86400 + expRuntime.seconds,
                'usage' : usage,
                }
        self.assertTrue(ret == exp)

    def test_processJob(self):
        # should prepare processings, articles insertions
        ret = processJob(self.jobpath, self.tag, self.proctype, self.articles, self.processings, self.filepath_map, self.file_pattern, update=True)

        self.assertTrue(ret == 0)
