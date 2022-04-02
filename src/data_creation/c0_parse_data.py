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
import csv

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
DATAPATH = PATH + 'extracted/' + YEAR + MONTH + '/03/1816067.xml' # Make this as a list from which I read
DATAPATH2 = PATH + 'extracted/' + YEAR + MONTH + '/03/1816068.xml'

COUNTRIES = ['Thailand']

def article_to_datarow(path_to_article, relevant_countries_list):
	# High level function that reads an article, 
	# Determines whether the location is relevant
	# If the location is relevant, it builds features for the article
	with open(path_to_article, 'r') as f:
		data = f.read()

	bs_data = BeautifulSoup(data, "xml")
	# print(bs_data.prettify)

	bs_location = bs_data.find_all("location")

	if not relevant_location(bs_location, relevant_countries_list):
		f.close()
		return # Return nothing in this case
	
	data = build_features(bs_data)

	return data

def relevant_location(article_locations, relevant_countries_list):
	# Determines whether location is relevant
	
	locations = []

	for location in article_locations:
		locations.append(location.string)

	# Match against country list to determine if relevant
	if set(locations).isdisjoint(relevant_countries_list):
		return False
	return True
 
def build_features(bs_data):
	# TODO: cannot handle when there are several locations
	# TODO: Discuss what set of features we want
	# TODO: important, we may want to classify articles based on content type. How to do that?
		# There are a bunch of potentially relevant tags we may want to explore first
	#print(bs_data.prettify)
	
	# Article id
	id_str = bs_data.find("doc-id").get("id-string")
	# Country	
	loc = bs_data.find("location").string
	
	# Date
	pubdata = bs_data.find(name="pubdata")
	date = pubdata.get("date.publication")

	# Word count
	length = pubdata.get("item-length")
	length_measure = pubdata.get("unit-of-measure")

	# Other complex features
	# TODO: add stuff here that we want to look at. 
	# Bunch together features
	observation = {
		"id": id_str,
		"location" : loc,
		"date": date,
		"length": length,
		"length_measure": length_measure
	}
	#print(observation)
	return(observation)


# TODO: loop over datapaths to all articles and build up the obs_dict_list
DATAPATHS = [DATAPATH, DATAPATH2, DATAPATH]
obs_dict_list = []

for datapath in DATAPATHS:
	obs_dict = article_to_datarow(datapath, COUNTRIES)
	if bool(obs_dict):
		obs_dict_list.append(obs_dict)
		

def write_list_to_table(obs_list, write_to_file):
	field_names= ['id', 'location', 'date', "length", "length_measure"]

	with open(write_to_file, 'w') as csvfile:
		writer = csv.DictWriter(csvfile, fieldnames=field_names)
		writer.writeheader()
		writer.writerows(obs_list)
	return

WRITE_TO_FILE = "data/analysis/test.csv"
write_list_to_table(obs_dict_list, WRITE_TO_FILE)

