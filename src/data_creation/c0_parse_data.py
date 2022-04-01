#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Apr 01 
@author: Filip Mellgren
"""
# Packages used:

import tarfile
import os
from bs4 import BeautifulSoup

#os.chdir(os.getcwd() + '/../..')

PATH = os.getcwd() + '/data/'

RAW = 'raw/nyt_corpus/data/'

YEAR = '2007/'

MONTH = '01'

DEST = 'extracted/'

# Extract content from compressed TGZ file. Each TGZ file represents a month with the news article from each day in that month #################

if not os.path.exists(PATH + DEST + YEAR + MONTH):
    file = tarfile.open(PATH + RAW + YEAR + MONTH + '.tgz')
    file.extractall(PATH + DEST + YEAR)
    file.close()


# Read sample article from extracted material #################

# Reading the data inside the xml file to a variable under the name data
DATAPATH = PATH + 'extracted/' + YEAR + MONTH + '/03/1816067.xml'

with open(DATAPATH, 'r') as f:
    data = f.read()
 
# Passing the stored data inside
# the beautifulsoup parser, storing
# the returned object

bs_data = BeautifulSoup(data, "xml")
 
# print(Bs_data.prettify)

# Find all items tagged "location"
bs_location = bs_data.find_all("location")

#print(b_unique)

for location in bs_location:
		print(location.string) # This should print ountry names, if any exists


