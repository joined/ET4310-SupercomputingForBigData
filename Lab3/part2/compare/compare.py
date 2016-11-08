#!/usr/bin/env python3
import sys


def getLineData(line):
    s = line.split()
    if s[0][:3] == 'chr':
        return "{},{},{},{}".format(s[0], s[1], s[3], s[4])
    else:
        return None

if len(sys.argv) < 3:
    print("Insufficient number of arguments!")
    print("Usage: python compare.py ref.vcf your.vcf")
    sys.exit(1)

f1, f2 = sys.argv[1], sys.argv[2]

f1data, f2data = set(), set()

with open(f1) as f1:
    for line in f1:
        line_data = getLineData(line)
        if line_data:
            f1data.add(line_data)

with open(f2) as f2:
    for line in f2:
        line_data = getLineData(line)
        if line_data and line_data in f1data:
            f2data.add(line_data)

print("total = {}".format(len(f1data)))
print("matches = {}".format(len(f2data)))
print("diff = {}".format(len(f1data) - len(f2data)))
