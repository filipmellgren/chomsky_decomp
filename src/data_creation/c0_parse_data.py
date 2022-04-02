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

#os.chdir(os.getcwd() + '/../..') # Discomment when building in Sublime

def parse_data():
	# Main logic for parsing data. 
	# Read from file key arguments re what countries and years to extract from .tgz files.
	# From extracted .tgz files, locate countires that we care about inside articles.
	# For articles with country we care about, save key attributes about the article and store in a list.
	# Finally, write the list to a .csv file that will be used to analyze the data.
	PATH = os.getcwd() + '/data/'
	RAW = 'raw/nyt_corpus/data/'
	DEST = 'extracted/'
	
	countries = read_params("parameters/countries.csv")
	years = read_params("parameters/years_lite.csv")
	months = read_params("parameters/months_lite.csv")
	
	year_months = itertools.product(years, months)

	# Extract content from compressed TGZ file. Each TGZ file represents a month with the news article from each day in that month 

	for ym in year_months:
		# Loop over cartesian product of years and months to extract TGZ files one by one.
		if not os.path.exists(PATH + DEST + ym[0] + "/" + ym[1]):
			# Only extract if destination file does not yet exist
			extract_tarfile(PATH, RAW, ym[0], ym[1])

	# Obtain a list of datapaths to extracted files
	datapaths = []

	for root, dirs, files in os.walk(PATH + 'extracted/'):
		for file in files:
			if file.endswith(".xml"):
				datapaths.append(root + "/" + file)
				#print(os.path.join(root, file))

	obs_dict_list = []
	
	for datapath in datapaths:
		obs_dict = article_to_datarow(datapath, countries)
		if bool(obs_dict):
			obs_dict_list.append(obs_dict)

	write_to_file = "data/analysis/test.csv"
	write_list_to_table(obs_dict_list, write_to_file)
	return

def read_params(param_path):
	# Utility function to read parameter files
	f = open(param_path, "r")
	params = f.read().split(', ')
	f.close()
	return(params)

def extract_tarfile(path, raw, year, month):
	# Extract file if it exists
	# Write to path/dest/year
	if os.path.exists(path + raw + year + "/" + month + '.tgz'):
		file = tarfile.open(path + raw + year + "/" + month + '.tgz')
		file.extractall(path + dest + year + "/")
		file.close()

def article_to_datarow(path_to_article, relevant_countries_list):
	# High level function that reads an article, 
	# Determines whether the location is relevant
	# If the location is relevant, it builds features for the article
	with open(path_to_article, 'r') as f:
		data = f.read()
		f.close()

	bs_article = BeautifulSoup(data, "xml")
	# print(bs_data.prettify)

	bs_location = bs_article.find_all("location")

	if not relevant_location(bs_location, relevant_countries_list):
		return # Return nothing in this case
	
	datarow = build_features(bs_article)

	return datarow

def relevant_location(article_locations, relevant_countries_list):
	# Determines whether the location attribute of an article is relevant to our use case
	
	locations = []

	for location in article_locations:
		locations.append(location.string)

	# Match against country list to determine if relevant
	if set(locations).isdisjoint(relevant_countries_list):
		return False
	return True
 
def build_features(bs_article):
	# Main method for creating features that we care about from opened articles.
	# TODO: cannot handle when there are several locations
	# TODO: Discuss what set of features we want
	# TODO: important, we may want to classify articles based on content type. How to do that?
		# There are a bunch of potentially relevant tags we may want to explore first
	#print(bs_data.prettify)
	
	# Article id
	id_str = bs_article.find("doc-id").get("id-string")
	# Country	
	loc = bs_article.find("location").string
	
	# Date
	pubdata = bs_article.find(name="pubdata")
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

def write_list_to_table(obs_list, write_to_file):
	field_names= ['id', 'location', 'date', "length", "length_measure"] # TODO: make these names depend on the features in order to not hardcode them here

	with open(write_to_file, 'w') as csvfile:
		writer = csv.DictWriter(csvfile, fieldnames=field_names)
		writer.writeheader()
		writer.writerows(obs_list)
		csvfile.close()
	return
