#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
    pdf_to_dag.py
    ~~~~~~~~~~~~~~

    Script to convert a directory (or directory tree) containing
    pdf and tif files into a condor-ready input directory.

    Usage
    =====

    To run the script from the command line::

        python pdf_to_dag.py input_dir output_dir

    Returns
    =======
    Creates the specified directory from the contents of the input_dir,
    with pdf/tif inputs sorted into job subdirectories.

    Within the output_dir directory, there is a file_mapping.pickle which holds
    the mapping between job directory and input filepath.

    Arguments
    =========
    :input_dir: Directory to crawl for input pdf/tif files
    :output_dir: Output directory.
"""
import os
import pickle

def get_job_directory(input_path, output_path, maxjobs):
    """
    Crawls through input_directory, and creates a output_path/jobxxxxx/
    directory with a renamed copy of any pdf/tif found.

    :input_path: Input directory to search.
    :output_path: Output dir
    :returns: info, dictionary containing the mapping between input file and output path

    """
    info = {}
    count = 0
    batches = 0
    for root, dirs, files in os.walk(input_path):
        fileinfos = {}
        for f in files:
            suffix = f.split('.')[-1].lower()
            if suffix == "pdf" or suffix.startswith("tif"):
                filepath = os.path.realpath(os.path.join(root, f))
                count += 1

                new_job_folder_name = "job%06d" % count
                new_job_folder_path = os.path.join(output_path, new_job_folder_name)
                try:
                    os.makedirs(new_job_folder_path)
                except OSError:
                    # it is acceptable to encounter makedir error before create symbol link
                    pass

                link_name = os.path.join(new_job_folder_path, "input.%s" % suffix)
                try:
                    os.symlink(filepath, link_name)
                    info[new_job_folder_name] = filepath
                except os.error, e:
                    print "Fail to create symbol link %s towards %s" % (link_name, filepath)
                    raise e
                if (count >= maxjobs):
                    try:
                        f = open(os.path.join(output_path, "filepath_mapping.pickle"), 'wb')
                        pickle.dump(info, f)
                        info={}
                        f.close()
                    except pickle.PickleError, e:
                        print "WARNING: Fail to serialize original filepath information"
                        raise e
                    count = 0
                    output_path = output_path.split("_"+str(batches))[0]
                    batches += 1
                    output_path = output_path + "_" + str(batches)
    try:
        f = open(os.path.join(output_path, "filepath_mapping.pickle"), 'wb')
        pickle.dump(info, f)
        f.close()
    except:
        print "WARNING: Fail to serialize original filepath information"
    return info


def main(argv):
    if len(argv) >= 3 and os.path.isdir(argv[1]) and not os.path.exists(argv[2]):
        if len(argv) == 4:
            maxjobs = int(argv[3])
        else:
            maxjobs = 10000
        info = get_job_directory(argv[1], argv[2], maxjobs)
    else:
        print "Please indicate existed input path and non-existed output directory!"
        sys.exit(1)

if __name__ == '__main__':
    import sys
    main(sys.argv)
