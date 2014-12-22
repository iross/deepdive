deepdive
========

OCR
---

### Getting started

The overall process is:
- Get relevant packages
- Convert PDF/TIF inputs into condor-read job directories.
- Submit jobs

The pieces needed are:
- ocr_pdf.py
- pdf_to_dag.py
- iancsde.zip
- ChtcRun package

Hit-the-ground-running recipe:
```
git clone https://github.com/iross/deepdive
cd deepdive
git checkout ians_cde
wget ianscde.zip
ChtcRun stuff
```


### More documentation
OCR + Job creation scripts: [ReadTheDocs.org] (https://readthedocs.org/projects/deepdive/)

ChtcRun: [Submitting Jobs Using the ChtcRun Package] (http://chtc.cs.wisc.edu/DAGenv.shtml)

### Dealing with different file types
Currently, we made these basic assumptions when dealing with different files

* PDF: one article == one PDF, which has multiple pages
* TIFF: one article == one folder, which contains multiple TIFF files, one TIFF == one page

Reference
---------
* [Tesseract] (http://tesseract-ocr.googlecode.com/svn/trunk/doc/tesseract.1.html)
* [Cuneiform] (https://launchpad.net/cuneiform-linux)

http://tfischernet.wordpress.com/2008/11/26/searchable-pdfs-with-linux/

Known Issues
------------
### Let Cuneiform accept TIFF as its input
You have to compile cuneiform with ImageMagick++

The simplest solution is `apt-get install libmagick++-dev libmagick++1`
Otherwiese you should download ImageMagick and compile it firstly

There is a bug that cmake could not find ImageMagick after the compilation and
installation. (Assuming compile it with `./configure --prefix=$HOME/local`)
One trick hack is to violently modify `$vim cuneiform-linux-1.1.0/builddir/CMakeCache.txt`

    //Path to the ImageMagick include dir.
    ImageMagick_Magick++_INCLUDE_DIR:PATH=/u/z/h/zhaoyu/local/include/ImageMagick-6/

    //Path to the ImageMagick Magick++ library.
    ImageMagick_Magick++_LIBRARY:FILEPATH=/u/z/h/zhaoyu/local/lib/libMagick++-6.Q16.so
