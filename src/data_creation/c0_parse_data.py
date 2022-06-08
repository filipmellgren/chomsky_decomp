#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Apr 01 
@author: Filip Mellgren

The idea is to develop a test environment where we loop over a small list of year months locally, and then loop over the whole list on a different machine. From that machine, we can then export tidy data, and ignore build conditional on observing a tidy data file.
"""

# TODOS:
# 1. Loop over all years and months. Do this on a different machine as it will take longish time.
# 2. Add more features.

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
			extract_tarfile(PATH, DEST, RAW, ym[0], ym[1])

	# Obtain a list of datapaths to extracted files.
	datapaths = []

	if os.path.exists("data/paths.txt"): # If a path to files has already been created
		path_file = open("data/paths.txt", "r")
		datapaths = path_file.read().split(",")
		path_file.close()


	else: # Otherwise go through all files and create the file of paths to relevant data
		for root, dirs, files in os.walk(PATH + 'extracted/'):
			for file in files:
				if file.endswith(".xml"):
					datapaths.append(root + "/" + file)
					#print(os.path.join(root, file))

	obs_dict_list = []
	useful_datapaths = []
	
	for datapath in datapaths:
		obs_dict = article_to_datarow(datapath, countries)
		if bool(obs_dict):
			obs_dict_list.append(obs_dict)
			useful_datapaths.append(datapath)

	write_to_file = "data/analysis/test.csv"
	write_list_to_table(obs_dict_list, write_to_file)

	with open("data/paths.txt", "w") as f:
		f.write(','.join(useful_datapaths))
	f.close()

	return

def read_params(param_path):
	# Utility function to read parameter files
	f = open(param_path, "r")
	params = f.read().split(', ')
	f.close()
	return(params)

def extract_tarfile(path, dest, raw, year, month):
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
	# TODO: Make several location feature nicer by not looping and startin with "-".
	# TODO: Discuss what set of features we want
	# TODO: important, we may want to classify articles based on content type. How to do that?
		# There are a bunch of potentially relevant tags we may want to explore first
	#print(bs_data.prettify)

	#print(bs_article.prettify)
	# Article id
	id_str = bs_article.find("doc-id").get("id-string")
	# Country	
	locations = bs_article.find_all("location")
	locs = ""
	for loc in bs_article.find_all("location"):
		locs = locs + "-" + loc.string

	# Date
	pubdata = bs_article.find(name="pubdata")
	date = pubdata.get("date.publication")

	# Word count
	length = pubdata.get("item-length")
	length_measure = pubdata.get("unit-of-measure")

	# Title 
	title = bs_article.title.string

	# Meta data
	meta = bs_article.find_all('meta')

	# Add to dict
	observation = {}

	for m in meta:
		observation[m.get("name")] = m.get("content")

	#observation = dict(zip(meta.get("name"), meta.get("content")))

	observation["id"] = id_str
	observation["location"] = locs
	observation["date"] = date
	observation["length"] = length
	observation["length_measure"] = length_measure


	# Other complex features
	# TODO: add stuff here that we want to look at. 
	# Bunch together features
	#print(bs_article) ## Prints the .xml

	return(observation)		

def write_list_to_table(obs_list, write_to_file):

	field_names = [*obs_list[0]]
	with open(write_to_file, 'w') as csvfile:
		writer = csv.DictWriter(csvfile, fieldnames=field_names)
		writer.writeheader()
		writer.writerows(obs_list)
		csvfile.close()
	return
