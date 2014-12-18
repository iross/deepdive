#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
    ocr_pdf.py
    **********

    Script to run the Optical Character Recognition (OCR) process on a specified input

    Usage
    =====

    To run the script from the command line::
        python ocr_pdf.py --tesseract --no-cuneiform --pdf

    This example would run the Tesseract OCR program, outputting an annotated
    pdf.

    Returns
    =======
    By default, the output is an html (hOCR) file for each page of the input document.

    Arguments
    =========
    :filename: File to process (required)
    :--cuneiform: Run cuneiform OCR (--no-cuneiform is default)
    :--tesseract: Run tesseract OCR (--no-tesseract is default)
    :--pdf: Return an annotated PDF (or PDFs) after using the OCR (--no-pdf is default)
    :--k2pdf: Run the Kindle 2 pdf optimizer as intermediate step (--no-k2pdf is default)
"""
import subprocess
import os
import shutil
import argparse

import pdb
def call(cmd, check=True, stdout=None, stderr=None):
    """
    Spawn subprocess for a command, optionally checking its return.

    :cmd: Command to run
    :check: check return code or not (default: True)
    :stdout: Path for standard out
    :stderr: Path for standard err
    :returns: If check is True, returns 0 if success or a CalledProcessError. If check is False, returns 0 if success, otherwise 1"

    """
    if check:
        return subprocess.check_call(cmd, stdout=stdout, stderr=stderr, shell=True)
    else:
        return subprocess.call(cmd, stdout=stdout, stderr=stderr, shell=True)


def unzip(zip_file, func=call):
    """
    Unzip the specified file

    :zip_file: Name of zip file to unpack.

    """
    cmd = "unzip -o '%s'" % zip_file
    try:
        return func(cmd)
    except subprocess.CalledProcessError as e:
        if e.returncode != 2:
            raise e


def k2pdfopt(pdf_file, output_file, func=call):
    """
    Convert multi-column PDF into single column

    K2pdfopt (Kindle 2 PDF Optimizer) is a stand-alone program which optimizes the format of PDF (or DJVU) files for viewing on small (e.g. 6-inch) mobile reader and smartphone screens such as the Kindle's.
    The output from k2pdfopt is a new (optimized) PDF file.
    http://www.willus.com/k2pdfopt/

    :pdf_file: Path to PDF file to optimize
    :output_file: Output file.
    :returns: 0. WARNING, k2pdfopt will always return 0; judge its success by looking at the output_file

    """
    try:
        os.remove(output_file)
    except OSError as e:
        if e.errno != 2:
            raise e
    cmd = "./k2pdfopt -ui- -x -w 2160 -h 3840 -odpi 300 '%s' -o '%s'" % (pdf_file, output_file)
    return func(cmd)


def pdf_to_png(pdf_file, tmp_folder=None, func=call):
    """
    Converts pdf_file to png via gs command. Saves to [tmp_folder]/page-%%d.png (or ./page-%%d.png if no tmp_folder is specified)

    :pdf_file: PDF file to convert.
    :tmp_folder: Path to temporary folder to use (default: None)
    :returns: 0 if successful.

    """
    if tmp_folder:
        cmd = "./cde-package/cde-exec 'gs' -dBATCH -dNOPAUSE -sDEVICE=png16m -dGraphicsAlphaBits=4 -dTextAlphaBits=4 -r600 -sOutputFile='%s/page-%%04d.png' '%s'"\
            % (tmp_folder, pdf_file)
    else:
        cmd = "./cde-package/cde-exec 'gs' -dBATCH -dNOPAUSE -sDEVICE=png16m -dGraphicsAlphaBits=4 -dTextAlphaBits=4 -r600 -sOutputFile=page-%%04d.png '%s'" % pdf_file
    return func(cmd)


def pdf_to_bmp(pdf_file, tmp_folder=None, func=call):
    """
    Converts pdf_file to bitmap via gs command. Saves to [tmp_folder]/page-%%d.bmp (or ./page-%%d.bmp if no tmp_folder is specified)

    :pdf_file: PDF file to convert.
    :tmp_folder: Path to temporary folder to use (default: None)
    :returns: 0 if successful.

    """
    if tmp_folder:
        cmd = "./cde-package/cde-exec 'gs' -SDEVICE=bmpmono -r600x600 -sOutputFile='%s/page-%%04d.bmp' -dNOPAUSE -dBATCH -- '%s'"\
                % (tmp_folder, pdf_file)
    else:
        cmd = "./cde-package/cde-exec 'gs' -SDEVICE=bmpmono -r600x600 -sOutputFile='page-%%04d.bmp' -dNOPAUSE -dBATCH -- '%s'" % pdf_file
    return func(cmd)


def tesseract(png_folder_path, output_folder_path=None, func=call):
    """
    Run Tesseract OCR over the PNG files in the specified path.

    :png_folder_path: Path of the converted PNG files
    :output_folder_path: Target directory for hOCR output (defaults to png_folder_path)
    :returns: 0, always return 0

    """
    png_folder_path = os.path.abspath(png_folder_path)
    if not output_folder_path:
        output_folder_path = png_folder_path
    for i in os.listdir(png_folder_path):
        if i.endswith('.png'):
            png_path = os.path.join(png_folder_path, i)
            ppm_filename = "%s.ppm" % png_path
            ppm_filename = ppm_filename.replace(".png","")
            hocr_filename = os.path.join(output_folder_path, "%s" % "tesseract_"+i)
            cmd = "./cde-package/cde-exec 'convert' -density 750 '%s' '%s'" % (png_path, ppm_filename)
            func(cmd)
            cmd = "./cde-package/cde-exec 'tesseract' '%s' '%s' hocr" % (ppm_filename, hocr_filename)
            func(cmd)
            cmd = "rm -f '%s'" % (ppm_filename)
            func(cmd)
    return 0


def cuneiform(bmp_folder_path, output_folder_path=None, func=call):
    """
    Run Cuneiform OCR over the BMP files in the specified path.

    :bmp_folder_path: Path of the converted BMP files
    :output_folder_path: Target directory for hOCR output (defaults to bmp_folder_path)
    :returns: 0, always return

    """
    bmp_folder_path = os.path.abspath(bmp_folder_path)
    if not output_folder_path:
        output_folder_path = bmp_folder_path
    for i in os.listdir(bmp_folder_path):
        if i.endswith('.bmp'):
            cmd = "CF_DATADIR=/usr/local/share/cuneiform ./cde-package/cde-exec cuneiform -f hocr -o '%s.html' '%s'"\
                % (os.path.join(output_folder_path, "cuneiform_" + i), os.path.join(bmp_folder_path, i))
            func(cmd)
    return 0

def hocr2pdf(input_pattern, prefix, suffix, image_dir="tmp/", func=call):
    """
    Use hocr2pdf to embed hOCR output back into source image, converting it to a PDF

    :input_pattern: Pattern of image files (.bmp or .png)
    :prefix: Prefix for saved output.
    :suffix: Suffix for hOCR input.
    :image_dir: Directory where temporary image files are stored (default: tmp/"
    :returns: 0 if successful

    """
    for i in os.listdir(image_dir):
        if i.endswith(input_pattern):
            html_doc = prefix + "_" + i + suffix
            cmd = "./cde-package/cde-exec hocr2pdf -i %s -o %s < %s" % \
                    (image_dir+i, html_doc.replace(suffix, ".pdf"), html_doc)
    return func(cmd)

def tiff_to_html(tiff_path, output_folder_path=None, func=call):
    """
    Run Tesseract OCR on a TIF file.

    :tiff_path: Path where tiff is stored
    :output_folder_path: Target directory for hOCR output (defaults to .)
    :returns: 0 if successful

    """
    output_folder_path = os.path.abspath(output_folder_path) if output_folder_path else os.path.abspath('.')
    hocr_path = os.path.join(output_folder_path, os.path.basename(tiff_path))
    cmd = "./cde-package/cde-exec 'tesseract' '%s' '%s.hocr' hocr" % (tiff_path, hocr_path)
    return func(cmd)


class OcrPdf(object):

    """Helper class to aid the PDF->OCR process"""

    def __init__(self, pdf_path, stdout_filepath, stderr_filepath, output_folder_path=None, cuneiform=True, tesseract=True, k2pdf = False, pdf=False):
        """
        :pdf_path: Path to PDF file to convert
        :stdout_filepath: Path for standard out
        :stderr_filepath: Path for standard err
        :output_folder_path: Path for output
        :cuneiform: Run Cuneiform toggle
        :tesseract: Run Tesseract toggle
        :k2pdf: Run k2pdf toggle
        :pdf: Output as PDF toggle

        """

        try:
            self.stdout = open(stdout_filepath, 'a')
            self.stderr = open(stderr_filepath, 'a')
            self.pdf_path = pdf_path
            self.k2pdf = k2pdf
            self.pdf = pdf
            self.cuneiform = cuneiform
            self.tesseract = tesseract
            self.output_folder_path = output_folder_path
        except IOError as e:
            print "ERROR\tInvalid filepath %s, %s" % (stdout_filepath, stderr_filepath)
            if self.stdout:
                self.stdout.close()
            if self.stderr:
                self.stderr.close()
            raise e

        shutil.rmtree('tmp', True)
        try:
            os.mkdir('tmp')
        except OSError as e:
            print "ERROR\tCreate tmp folder"
            raise e

        if self.output_folder_path and not os.path.isdir(self.output_folder_path):
            try:
                os.mkdir(self.output_folder_path)
            except OSError as e:
                print "ERROR\tCreate output folder"
                raise e

    def __del__(self):
        shutil.rmtree('tmp', True)

    def call(self, cmd, check=True):
        return call(cmd, check=check, stdout=self.stdout, stderr=self.stderr)

    def do(self):
        """
        Runs the desired commands.

        """
        unzip("ianscde.zip", func=self.call)
        if self.k2pdf:
            output_file = "k2_pdf_%s" % self.pdf_path
            print k2pdfopt(self.pdf_path, output_file, func=self.call)
        else:
            output_file = self.pdf_path
        # todo: is there a reason cuneiform is using bmp while tesseract uses png?
        if self.tesseract:
            print pdf_to_png(output_file, tmp_folder='tmp', func=self.call)
            print tesseract('tmp', self.output_folder_path, self.call)
        if self.cuneiform:
            print pdf_to_bmp(output_file, tmp_folder='tmp', func=self.call)
            print cuneiform('tmp', self.output_folder_path, self.call)
        if self.pdf:
            if self.cuneiform:
                print hocr2pdf(".bmp", "cuneiform", ".html", "tmp/", self.call)
            if self.tesseract:
                print hocr2pdf(".png", "tesseract", ".hocr", "tmp/", self.call)

    def tiffs_to_htmls(self, tiff_folder_path):
        """
        Convert TIFFs to hOCR files. Can be used for .tif OR .tiff files

        :tiff_folder_path: Path to directory containing TIFF files.
        :returns: True or the filepath which failed to be converted
        """
        for i in os.listdir(tiff_folder_path):
            if i.endswith('.tif') or i.endswith('.tiff'):
                tiff_path = os.path.join(tiff_folder_path, i)
                if tiff_to_html(tiff_path, self.output_folder_path, self.call):
                    return tiff_path
        return True


def main(args):
    o = OcrPdf(args.file, 'out.txt', 'out.txt', './',args.cuneiform,args.tesseract,args.k2pdf,args.pdf)
    o.do()
#    o.tiffs_to_htmls(argv[1])

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Process some integers.')
    parser.add_argument('file', type=str, default="input.pdf", help='Filename to process')
    parser.add_argument('--cuneiform', dest='cuneiform', action='store_true', help='Run Cuneiform OCR?')
    parser.add_argument('--no-cuneiform', dest='cuneiform', action='store_false', help='Run Cuneiform OCR?')
    parser.add_argument('--tesseract', dest='tesseract', action='store_true', help='Run Tesseract OCR?')
    parser.add_argument('--no-tesseract', dest='tesseract', action='store_false', help='Run Tesseract OCR?')
    parser.add_argument('--pdf', dest='pdf', action='store_true', help='Return annotated PDF?')
    parser.add_argument('--no-pdf', dest='pdf', action='store_false', help='Return annoted PDF?')
    parser.add_argument('--k2pdf', type=bool, required=False, default=False, help='Run k2pdf step?')

    args = parser.parse_args()
    main(args)
