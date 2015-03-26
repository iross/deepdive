from harvestResults_postscript import *
import unittest
import hashlib
import datetime
import pickle
import os,shutil
import pymongo
from bson.objectid import ObjectId

def extractDictAFromB(A,B):
    return dict([(k,B[k]) for k in A.keys() if k in B.keys()])

def setUpModule():
    print "setup"
    uri = "mongodb://localhost:27018/"
    mongodb_client = pymongo.MongoClient(uri)
    mongodb_client.copy_database("articles_test", "articles_test_play")
    mongodb_client.close()

def tearDownModule():
    uri = "mongodb://localhost:27018/"
    mongodb_client = pymongo.MongoClient(uri)
    mongodb_client.drop_database("articles_test_play")
    mongodb_client.drop_database("processing_test")
    mongodb_client.close()
    print "tearDown"

class HarvestResultTestManager(unittest.TestCase):

    """Test the results harvester"""

    def setUp(self):
        config = ConfigParser.RawConfigParser()
        config.read(BASE+'db_conn.cfg')
        reader_user = config.get('database','reader_user')
        reader_password = config.get('database','reader_password')
        uri = "mongodb://localhost:27018/"
        self.client = pymongo.MongoClient(uri)

        articlesdb = self.client.articles_test_play

        if "articles" not in articlesdb.collection_names():
            print "Cannot run tests! No valid article_test collection found!"
            sys.exit(1)

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
        # restore original test DB
#        self.client.drop_database("articles_test")
        self.client.close()

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
        self.assertTrue(match, extractDictAFromB(match, expectedMatch))

    def test_parse_time(self):
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
        expRuntime = ( datetime.datetime(2015,12,29,15,23,01) - datetime.datetime(2015,12,29,15,13,52))
        usage = {
                'diskUsage': 678215,
                'memUsage': 220
                }
        exp = {
                'path': '%s/%s' % (basedir, self.jobid),
                'subTime': datetime.datetime(2015,12,29,15,10,43),
                'execTime': datetime.datetime(2015,12,29,15,13,52),
                'termTime': datetime.datetime(2015,12,29,15,23,01),
                'runTime' : expRuntime.days * 86400 + expRuntime.seconds,
                'usage' : usage,
                }
        print ret
        print exp
        self.assertTrue(ret == exp)

    def test_failed_job(self):
        # should write 1 to RESULT
        # should write cpu/fail stats
        # should write article processing info
        self.assertTrue(True)

    def test_success_job(self):
        # should write 0 to RESULT
        # should write cpu/success stats
        # should write article processing info
        self.assertTrue(True)

    def test_fail_to_succeed_transition(self):
        # should add to cpu/success stats
        # should update article processing info
        self.assertTrue(True)

    def test_pattern_matching(self):
        # should be able to match arbitrary output patterns to job types
        # or should look for patterns based on job types?
        files = patternMatch('page*.hocr.html', os.getcwd()+'/tests/job000001/')
        base_files = ["page-1.hocr.html", "page-2.hocr.html", "page-3.hocr.html",\
                "page-4.hocr.html", "page-5.hocr.html", "page-6.hocr.html"]
        base_files = [os.getcwd() + '/tests/job000001/' + i for i in base_files]
        self.assertTrue(base_files == files, msg="Returned list doesn't match expected! %s vs %s" % (str(base_files), str(files)))

    def test_articleUpdate_failure(self):
        # should update the matched article, adding an empty filename list, success: False, and harvested: True to the DB
        jobpath = "./tests/job_failure_example/"
        jobid = "job000009"
        tag = "harvesting_postscript_test"
        proctype = "ocr"
        file_pattern = "page*.html"
        filepath_map = pickle.load(open("./tests/job_failure_example/filepath_mapping.pickle"))
        match_before = getMatch(jobid, self.articles, filepath_map)
        success, temp = updateArticle(match_before, jobpath, tag, proctype, self.articles, filepath_map, file_pattern)
        match_after = self.articles.find_one({"URL": match_before["URL"]})
        self.assertTrue(match_after == match_before and success != 0)

    def test_articleUpdate_success(self):
        # def updateArticle(match, jobpath, tag, proctype, articlesColl, filepath_map, file_pattern, dryrun=False, update=False):
        # should update the database
        # verify that the match before and after are the same, aside from harvest updates
        jobpath = os.getcwd()+"/tests/job_success_example/"
        jobid = "job000002"
        tag = "harvesting_postscript_test"
        proctype = "ocr"
        file_pattern = "page*.html"
        filepath_map = pickle.load(open("./tests/job_success_example/filepath_mapping.pickle"))
        match_before = getMatch(jobid, self.articles, filepath_map)
        success, temp = updateArticle(match_before, jobpath, tag, proctype, self.articles, filepath_map, file_pattern)
        match_after = self.articles.find_one({"URL": match_before["URL"]})
        subset = all((k in match_after and match_after[k] == v) for k,v in match_before.iteritems())
        expected = ['/home/iaross/harvesting_postscript/deepdive/tests/job_success_example/page-1.hocr.html']
        new = (match_after["%s_processing" % proctype][tag]['filename'] == expected)
        print "Success? : ", success
        print "New stuff there? : ", new
        print "Nothing else touched? : ", subset

        self.assertTrue(subset and new and success==0)

    def test_processingUpdate_success(self):
        jobpath = os.getcwd()+"/tests/job_success_example/"
        jobid = "job000002"
        tag = "harvesting_postscript_test"
        proctype = "ocr"
        file_pattern = "page*.html"
        filepath_map = pickle.load(open("./tests/job_success_example/filepath_mapping.pickle"))
        report = readLog(jobpath)
        match_before = getMatch(jobid, self.articles, filepath_map)
        proc_before = self.processings.find({"tag": tag})
        check = updateProcessing(self.processings, tag, match_before["pubname"], report, True)
        proc_after = self.processings.find_one({"tag": tag})
        added_one = (proc_after["pub_totals"][match_before['pubname']]["success"]==1)
        added_cputime = (proc_after["pub_totals"][match_before['pubname']]["cpusuccess"]==43)
        print proc_after

        self.assertTrue(added_one and added_cputime)

    def test_processingUpdate_failure(self):
        overall_success = False
        jobpath = os.getcwd()+"/tests/job_failure_example/"
        jobid = "job000009"
        tag = "harvesting_postscript_test"
        proctype = "ocr"
        file_pattern = "page*.html"
        filepath_map = pickle.load(open("./tests/job_success_example/filepath_mapping.pickle"))
        report = readLog(jobpath)
        match_before = getMatch(jobid, self.articles, filepath_map)
        proc_before = self.processings.find({"tag": tag})
        check = updateProcessing(self.processings, tag, match_before["pubname"], report, overall_success)
        proc_after = self.processings.find_one({"tag": tag})
        added_one = (proc_after["pub_totals"][match_before['pubname']]["failure"]==1)
        added_cputime = (proc_after["pub_totals"][match_before['pubname']]["cpufailure"]==17)
        print proc_after

        self.assertTrue(added_one and added_cputime)

    def test_ocr_nlp_compound_job(self):
        # should be able to parse OCR+NLP combo
        self.assertTrue(True)
    def test_cuneiform_fonttype_job(self):
        # should be able to parse cuneiform+fonttype combo
        self.assertTrue(True)
    def test_compound_job(self):
        # should be able to parse ocr+nlp+cuneiform+fonttype combo
        self.assertTrue(True)
