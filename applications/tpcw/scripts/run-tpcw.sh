#!/bin/bash

#This command line runs the TPC-W Browsing Mix with 30 EB's, output file run1.m
#300 seconds of ramp-up time, 1000 seconds of exection, and 300 seconds of ramp
#down time. http://whitelace.ece.wisc.edu:8085/ is used as the web server
#prefix for all requests. There are 10000 items in the database, and a standard
#Think time of multiplier of 1.0.

#java -mx512M rbe.RBE -EB rbe.EBTPCW1Factory 100 -OUT browsing.m -RU 300 -MI 3000 -RD 300 -WWW http://localhost:8080/tpcw/ -CUST 144000 -ITEM 10000 -TT 1.0

#This command line does the same, using the TPC-W Shopping Mix, and no think
#time.
#java -mx512M rbe.RBE -EB rbe.EBTPCW2Factory 100 -OUT shopping.m -RU 300 -MI 3000 -RD 300 -WWW http://localhost:8080/tpcw/ -CUST 144000 -ITEM 10000 -TT 1.0

#This command line does the same, using the TPC-W Ordering Mix
#java -mx512M rbe.RBE -EB rbe.EBTPCW3Factory 2 -OUT ordering2.m -RU 300 -MI 3000 -RD 300 -WWW
# http://localhost:8080/tpcw/ -CUST 144000 -ITEM 10000 -TT 1.0


#Browsing Mix = rbe.EBTPCW1Factory
#Shopping Mix = rbe.EBTPCW2Factory
#Ordering Mix = rbe.EBTPCW3Factory

# -OUT               Output file               run1.m
# Name of matlab .m output file for results.
# -ST Starting time for ramp-up March 30, 2015 11:36:19 AM PDT (default)
# Time (such as Nov 2, 1999 11:30:00 AM CST) at which to start ramp-up.  Useful for synchronizing multiple RBEs.
# -RU              Ramp-up time                   30
#  Seconds used to warm-up the simulator.
# -MI      Measurement interval                   60
# Seconds used for measuring SUT performance.
# -RD            Ramp-down time                    1
# Seconds of steady-state operation following measurment interval.
# -SLOW          Slow-down factor        1.0 (default)
# 1000 means one thousand real seconds equals one simulated second.  Accepts factional values and E notation.
# -TT Think time multiplication.        1.0 (default)
# Used to increase (>1.0) or decrease (<1.0) think time.  In addition to slow-down factor.
# -KEY      Interactive control.      false (default)
# Require user to hit RETURN before every interaction.  Overrides think time.
# GETIM           Request images.                false
# True will cause RBE to request images.  False suppresses image requests.
# -CON         Image connections          4 (default)
# Maximum number of images downloaded at once.
# -CUST       Number of customers       1000 (default)
# Number of customers in the database.  Used to generated random CIDs.
# -CUSTA              CID NURand A         -1 (default)
# Used to generate random CIDs.  See TPC-W Spec. Clause 2.3.2.  -1 means use TPC-W spec. value.
# -ITEM           Number of items      10000 (default)
# Number of items in the database. Used to generate random searches.
# -ITEMA             Item NURand A         -1 (default)
# Used to generate random searches.  See TPC-W Spec. Clause 2.10.5.1.  -1 means use TPC-W spec. value.
# -DEBUG            Debug message.          0 (default)
# Increase this to see more debug messages ~1 to 10.
# -MAXERROR   Maximum errors allowed.          1 (default)
# RBE will terminate after this many errors.  0 implies no limit.
# -WWW                  Base URL http://localhost:8080/tpcw/
# The root URL for the TPC-W pages.
# -MONITOR Do utilization monitoring      false (default)
# TRUE=do monitoring, FALSE=Don't do monitoring
# -INCREMENTAL   Start EBs Incrementally      false (default)
# TRUE=do them in increments, FALSE=Do them all at once
java rbe.RBE -EB rbe.EBTPCW2Factory 10 -OUT run1.m -GETIM false -RU 1 -MI 60 -TT 0.80 -RD 1 -WWW http://localhost:8080/tpcw/ -ITEM 10000 -CUST 100
