# MAKE SURE TO CHANGE THE FIRST SECTION BELOW FOR EACH NEW SUBMISSION!!!
#
# By default, your job will be submitted to the CHTC's HTCondor 
# Pool only, which is good for jobs that are each less than 24 hours.
#
# If your jobs are less than 4 hours long, "flock" them additionally to 
# other HTCondor pools on campus by uncommenting the below line:
#+WantFlocking = true
#
# If your jobs are less than ~2 hours long, "glide" them to the national
# Open Science Grid (OSG) for access to even more computers and the
# fastest overall throughput. Uncomment the below line:
#+WantGlidein = true
#
# Tell Condor how many CPUs (cores), how much memory (MB) and how much 
# disk space (KB) each job will need:
request_cpus = 1
request_memory = 1000
request_disk = 1000000



# SECOND SECTION: Unlikely that you'll need to change this each time
# The below lines indicate that all files necessary for each job
# need to be transfered to it. Only modify these if you have implemented
# self-checkpointing; change "when_to_transfer_output" to "ON_EXIT_OR_EVICT".
should_transfer_files = YES
when_to_transfer_output = ON_EXIT

# If you want your jobs to go on hold because they are
# running longer then expected,  uncomment this line and
# change from 24 hours to desired limit:
#periodic_hold = (JobStatus == 2) && ((CurrentTime - EnteredCurrentStatus) > (60 * 60 * 24))



# YOU SHOULD NOT NEED TO CHANGE ANYTHING BELOW THIS LINE
#
# This is a "normal" job.
universe = vanilla

# This wrapper script automates setting R or Matlab up.
executable = /home/iaross/harvesting_postscript/ChtcRun/chtcjobwrapper
requirements = (OpSysMajorVer =?= 6)

# If anything is output to standard output or standard error, 
# where should it be saved?
output = process.out
error = process.err

# Where to write a log of your jobs statuses.
log = process.log

# Tell us the version of R or Matlab you are using. Place it
# in the JobAd. Choose one. Or comment both if it is some other
# kind of program.
# Arguments to the wrapper script.  Of note is the last one, --, anything
# after this goes direct to your R, Matlab or Other code.
# This gets augmented for you by mkdag.pl. Choose R or Matlab
arguments =  --type=Other --cmdtorun=ocr_pdf.py --unique=job000009 -- input.pdf --tesseract --no-cuneiform 

# Release a job from being on hold hold after half an hour (1800 seconds), up to 4 times,
# as long as the executable could be started, the input files and initial directory
# were accessible and the user log could be created. This will help your jobs to retry
# if they happen to fail due to a computer issue (not an issue with your job)
periodic_release = (JobStatus == 5) && ((CurrentTime - EnteredCurrentStatus) > 1800) && (JobRunCount < 5) && (HoldReasonCode != 6) && (HoldReasonCode != 14) && (HoldReasonCode != 22)
#

# We don't want email about our jobs. (If you do, let us know,
# there may be some additional configuration necessary.)
notification = never

# This line is completed for you
transfer_input_files = /home/iaross/harvesting_postscript/ChtcRun/submit_1_26Mar/job000009/,/home/iaross/harvesting_postscript/ChtcRun/submit_1_26Mar/shared/

# Leave the below line commented, unless you have a specific need for
# indicating a group that is not your default.
# See: http://monitor.chtc.wisc.edu/uw_condor_usage/usage1.shtml
#+AccountingGroup = "CHTC"


queue
