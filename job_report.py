import glob
import os

def report(inputs, missing, name):
    print "-- %s --" % name
    print "Found: %s of %s" % (len(inputs) - len(missing), len(inputs))
    if len(missing) > 0:
        print "Missing: "
        for i in missing:
            print "\t%s" % i

def find_missing(inputs, name, output_path):
    temp = []
    for i in inputs:
        output_expected = output_path + "/" + i + "_%s_combined.pdf" % name
        if not os.path.exists(output_expected):
            temp.append(i)
    return temp

def main():
    """Main function only in command line"""
    from sys import argv
    if len(argv) < 3:
        print "Usage: python job_report.py "\
            "ORIGIN_FOLDER ORGANIZED_OUTPUT"
        print "e.g. python job_report.py "\
            "/home/iaross/merlin/toxic/uniroyal/ "\
            "organized_unroyal_output"
    origin_path = argv[1]
    output_path = argv[2]

    if not os.path.exists(argv[1]):
        print "Could not find origin (input) path (%s)" % origin_path
        return False
    if not os.path.exists(argv[2]):
        print "Could not find organized output path (%s)" % output_path

    inputs = []
    for root, dirs, files in os.walk(origin_path):
        for f in files:
            suffix = f.split('.')[-1].lower()
            if suffix == "pdf" or suffix.startswith("tif"):
                rel_path = root + "/" + f
                rel_path = rel_path.replace(origin_path + "/","")
                inputs.append(rel_path)

    temp = inputs[:]
    t_missing = find_missing(inputs, "tesseract", output_path)
    c_missing = find_missing(inputs, "cuneiform", output_path)

    report (inputs, t_missing, "Tesseract")
    print "\n"
    report (inputs, c_missing, "Cuneiform")

if __name__ == '__main__':
    main()
