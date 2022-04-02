#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Apr 01 
@author: Filip Mellgren

The idea is to develop a test environment where we loop over a small list of year months locally, and then loop over the whole list on a different machine. From that machine, we can then export tidy data, and ignore build conditional on observing a tidy data file.
"""

# TODOS:
# 1. Handle case where an article has several location attributes (if these exist, think they do)
# 2. Learn why there are countries such as Great Britain and France in the output file even as I only consider Sweden and Thailand
# 3. Why are there so few observations? Is it because I so far only considered 3 (or 2) years and 3 months?
# 4. Loop over all years and months. Do this on a different machine as it will take longish time.
# 5. Add more features.
# 6. Structure up the code logic a bit

# Packages used:

import tarfile
import os
from bs4 import BeautifulSoup
import itertools
import csv

os.chdir(os.getcwd() + '/../..') # Discomment when building in Sublime

PATH = os.getcwd() + '/data/'

RAW = 'raw/nyt_corpus/data/'

DEST = 'extracted/'

def read_params(param_path):
	f = open(param_path, "r")
	params = f.read().split(', ')
	f.close()
	return(params)

countries = read_params("parameters/countries.csv")
years = read_params("parameters/years_lite.csv")
months = read_params("parameters/months_lite.csv")
year_months = itertools.product(years, months)

# Extract content from compressed TGZ file. Each TGZ file represents a month with the news article from each day in that month 

def extract_tarfile(path, dest, raw, year, month):
	# Extract file if it has not already been extracted.
	# Write to path/dest/year

	if not os.path.exists(path + dest + year + "/" + month):
		if os.path.exists(path + raw + year + "/" + month + '.tgz'):
			file = tarfile.open(path + raw + year + "/" + month + '.tgz')
			file.extractall(path + dest + year + "/")
			file.close()

for ym in year_months:
	# Loop over cartesian product of years and months to extract TGZ files one by one.
    extract_tarfile(PATH, DEST, RAW, ym[0], ym[1])

# Obtain a list of datapaths to extracted files
datapaths = []

for root, dirs, files in os.walk(PATH + 'extracted/'):
	for file in files:
		if file.endswith(".xml"):
			datapaths.append(root + "/" + file)
			#print(os.path.join(root, file))

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

obs_dict_list = []

for datapath in datapaths:
	obs_dict = article_to_datarow(datapath, countries)
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

