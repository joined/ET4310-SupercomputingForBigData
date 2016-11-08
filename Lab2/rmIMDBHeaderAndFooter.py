#!/usr/bin/env python3
# Removes the header and footer of an IMDB file
# Created by Hamid Mushtaq
# Modified by Lorenzo Gasparini to run on Python3

import sys
import time
import codecs
import os

if len(sys.argv) < 3:
    print("Not enough arguments!")
    print("Example usage: ./removeIMDBHeaderAndFooter.py"
          " actors.list new_actors.list")
    sys.exit(1)

if not os.path.isfile(sys.argv[1]):
    print("File {} does not exist!".format(sys.argv[1]))
    sys.exit(1)

start_time = time.time()

with codecs.open(sys.argv[1], encoding='latin1') as fin,\
        codecs.open(sys.argv[2], 'w', encoding='latin1') as fout:
    # Skip all the header, stop when we reach the magic start line
    while next(fin) != '----\t\t\t------\n':
        continue

    for line in fin:
        # Stop if we get to the magic end line
        if line == '-' * 77 + '\n':
            break
        fout.write(line)

time_in_secs = int(time.time() - start_time)
print("Done!\n|Time taken = " + str(time_in_secs / 60) +
      " mins " + str(time_in_secs % 60) + " secs|")
