#!/usr/bin/python

import sys
import re
import os

if len(sys.argv) == 1:
	print "Usage: ./demo.py word_to_search"
	exit(0)

outputdir = "../hadoop-1.0.4/output/"
inputdir = "../hadoop-1.0.4/input/"


#Search through files and count matching
docs = {}
for subdir, dirs, files in os.walk(outputdir):
	for file in files:
		run = True
		f = open(outputdir+file, 'r')
		line = f.readline()
		if len(line) == 0:
			run = False
		while run:
			line = re.split('[,\t\n|]',line)
			for i in range(len(sys.argv) - 1):
				if line[0] == sys.argv[i+1]:
					for j in range(len(line)/3):
						doc = line[2+j*3]+line[1+j*3]
						if doc in docs:
							docs[doc] += int(line[3 + j*3])
						else:
							docs[doc] = int(line[3 + j*3])
			line = f.readline()
			if len(line) == 0:
				run = False

#Print out top 4 results
listupto = 4
word = ["" for i in range(listupto)]
val = [0 for i in range(listupto)]
for key, amount in docs.iteritems():
	testing = amount
	testword = key
	for i in range(listupto):
		if val[i] < testing:
			temp = val[i]
			val[i] = testing
			testing = temp
			temp = word[i]
			word[i] = testword
			testword = temp
for i in range(listupto):
	w = word[i]
	if w != '':
		w = w.split(".txt")
		f = open(inputdir+w[0]+".txt")
		f.read(int(w[1]))
		word[i] = f.readline()
	
print " ".join(sys.argv[1:])
for i in range(listupto):
	print str(i+1) + ": " + word[i]
