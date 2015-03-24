#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Brief Summary
Attributes:

Google Python Style Guide:
    http://google-styleguide.googlecode.com/svn/trunk/pyguide.html
"""
__copyright__ = "Zhaoyu Luo"

import os
import pickle
import shutil
import subprocess
import string
import pdb
import re


def organize_htcondor_jobs(origin_input_folder,
        htcondor_input_folder, htcondor_output_folder, output_folder):
    """Reorganize the processed ocr results into tree structure

    Args:
        folder paths

    Returns:
        True
    """
    origin_input_folder = os.path.abspath(origin_input_folder)
    input_info = None
    for f in os.listdir(htcondor_input_folder):
        if f.endswith('pickle'):
            pf = open(os.path.join(htcondor_input_folder, f), 'rb')
            input_info = pickle.load(pf)
            pf.close()

    if not input_info:
        raise Exception("There is no pickle file in %s" % htcondor_input_folder)

    if not os.path.exists(output_folder):
        os.makedirs(output_folder)

    for job_name in os.listdir(htcondor_output_folder):
        job_dir_path = os.path.join(htcondor_output_folder, job_name)
        job_dir_pdfs = []
        if job_name in input_info:
            # base on the original input folder path, cut the prefix
            new_organized_path = os.path.join(output_folder,
                os.path.abspath(input_info[job_name])\
                    [len(origin_input_folder)+1:])
            new_organized_path = os.path.join(output_folder,
                os.path.abspath(input_info[job_name])\
                        [len(origin_input_folder)+1:input_info[job_name].rfind('/')])+"/"
            original_file_name = os.path.abspath(input_info[job_name])\
                    [input_info[job_name].rfind('/')+1:]
            new_base = new_organized_path + original_file_name+"_"
            if not os.path.exists(new_organized_path):
                os.makedirs(new_organized_path)

            for f in os.listdir(job_dir_path):
                if f != "out.txt" and \
                    (f.endswith('.txt') or f.endswith('.html') \
                    or f.endswith('.pdf') or f.endswith('.hocr') ):
                    f_new = re.sub('(_input.+?f)', '', f)
                    shutil.copy(
                        os.path.join(htcondor_output_folder, job_name, f),
                        new_base + f_new)
                if f.endswith('.pdf'):
                    # separate cune/tess pdfs?
                    job_dir_pdfs.append('./'+job_dir_path+'/'+f)

            # smoosh multi-page PDFs into one PDF via gs:
            # gs -dBATCH -dNOPAUSE -q -sDEVICE=pdfwrite -dPDFSETTINGS=/prepress -sOutputFile=merged.pdf mine1.pdf mine2.pdf
            for type in ["tesseract", "cuneiform"]:
                combined_output = "\"" + new_base + "%s_combined.pdf" % type + "\""
                pdfstr = "%s/%s*.pdf" % (job_dir_path, type)
                if not os.path.exists('./cde-package'):
                    print "Missing the pdf-combining stuff! Attempting to install."
                    subprocess.call('curl -O http://pages.cs.wisc.edu/~iross/pdftk.zip', shell=True )
                    subprocess.call('unzip pdftk.zip', shell=True )
                try:
                    subprocess.call('./cde-package/cde-exec pdftk %s cat output %s' % (pdfstr, combined_output), shell=True )
                except:
                    pass

        else:
            if os.path.isdir(job_dir_path):
                raise Exception("Please check %s,"
                    "no information found in pickle"
                    % job_name)


def main():
    """Main function only in command line"""
    from sys import argv
    if len(argv) != 5:
        print "Usage: python organize_htcondor_jobs"\
            "ORIGIN_FOLDER FOLDER_HOLDING_PICKLE"\
            "FOLDER_HOLDING_OCR_RESULT OUTPUT"
        print "e.g. python organize_htcondor_jobs.py "\
            "/home/iaross/merlin/toxic/uniroyal/ "\
            "/home/iaross/merlin_001/test_run/ChtcRun/uniroyal "\
            "/home/iaross/merlin_001/test_run/ChtcRun/uniroyal_out/ myout"
        return False
    organize_htcondor_jobs(argv[1], argv[2], argv[3], argv[4])



if __name__ == '__main__':
    main()
