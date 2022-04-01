#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Apr 01 
@author: Filip Mellgren

The idea is to develop a test environment where we loop over a small list of year months locally, and then loop over the whole list on a different machine. From that machine, we can then export tidy data, and ignore build conditional on observing a tidy data file.
"""
# Packages used:

import tarfile
import os
from bs4 import BeautifulSoup
import itertools

os.chdir(os.getcwd() + '/../..') # Discomment when building in Sublime

PATH = os.getcwd() + '/data/'

RAW = 'raw/nyt_corpus/data/'

YEAR = '2007/'

MONTH = '01'

DEST = 'extracted/'

YEARS = ["2006", "2007"] # Change this to include all years when building main data set

MONTHS = ["01"] # Change this to include all months when building main data set

year_months = itertools.product(YEARS, MONTHS)

# Extract content from compressed TGZ file. Each TGZ file represents a month with the news article from each day in that month #################

def extract_tarfile(path, dest, raw, year, month):
	# Extract file if it has not already been extracted.
	# Write to path/dest/year

	if not os.path.exists(path + dest + year + "/" + month):
	  file = tarfile.open(path + raw + year + "/" + month + '.tgz')
	  file.extractall(path + dest + year + "/")
	  file.close()

for ym in year_months:
	# Loop over cartesian product of years and months to extract TGZ files one by one.
    print(ym[0])
    print(ym[1])
    extract_tarfile(PATH, DEST, RAW, ym[0], ym[1])

# Read sample article from extracted material #################
# TODO: the next steps are
# 	Open extracted files
#		Read them and look for country against a list
#			Country can be found by soup_location = soup.find_all("location"), then it is stored in soup_location.string
# 	If the *.string matches against the list, save file to different directory.
#		From the saved files in the different directory, open them and create features needed for analysis.
#			NOTE: we can skip this step and go directly to creating features, and storing those into an analysis file.

# Reading the data inside the xml file to a variable under the name data
DATAPATH = PATH + 'extracted/' + YEAR + MONTH + '/03/1816067.xml'

with open(DATAPATH, 'r') as f:
    data = f.read()
 
# Passing the stored data inside the beautifulsoup parser, storing the returned object

bs_data = BeautifulSoup(data, "xml")
 
# print(Bs_data.prettify)

# Find all items tagged "location"
bs_location = bs_data.find_all("location")

for location in bs_location:
		print(location.string) # This should print ountry names, if any exists


