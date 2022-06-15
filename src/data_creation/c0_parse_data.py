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
from itertools import repeat
import csv
from multiprocessing import Pool, cpu_count
from functools import partial
import ipdb
import pandas as pd

def parse_data():
	# Main logic for parsing data. 
	# Read from file key arguments re what countries and years to extract from .tgz files.
	# From extracted .tgz files, locate countires that we care about inside articles.
	# For articles with country we care about, save key attributes about the article and store in a list.
	# Finally, write the list to a .csv file that will be used to analyze the data.
	data_path = os.getcwd() + '/data/'
	RAW = 'raw/nyt_corpus/data/'
	DEST = 'extracted/'
	
	countries = read_params("parameters/countries.csv")
	years = read_params("parameters/years_lite.csv")
	months = read_params("parameters/months_lite.csv")
	
	prepare_tarfiles(years, months, data_path, DEST, RAW)

	pathindex_file = "data/index/" +str(min(years)) + "_" + str(max(years)) +".csv" # TODO: not perfect indicator, lacks month. Not vital as main run will include everything anyway

	datapaths = get_paths(pathindex_file, data_path)
	
	# Parallel processing over datapaths
	pool = Pool(cpu_count()-1)
	obs_dict_list = pool.starmap(article_to_datarow, zip(datapaths, repeat(countries)))

	datarows = [d['datarow'] for d in obs_dict_list]
	datarows = list(filter(None, datarows)) # filters away None entries

	create_path_index(pathindex_file, obs_dict_list) 

	write_to_file = "data/analysis/test.csv"
	write_dict_to_table(datarows, write_to_file)

	return

def create_path_index(pathindex_file, obs_dict_list):
	# Creates a path index for fast referencing of files by get_paths().
	# Only create if it does not already exist.
	if not os.path.exists(pathindex_file):
		path_indices = [d['pathindex'] for d in obs_dict_list]
		write_list_to_table(path_indices, pathindex_file)
		return

def get_paths(pathindex_file, data_path):
		# Returns a list of paths to news articles.
		# First, it checks if there exists an index over paths, and makes a selection based on the index.
		# TODO: So far, the selection is dumb and only filters away articles that do not mention a location. Can make smarter using pd.query.
		# If an index does not exist, the function adds all paths to all files that has been extracted.
		# TODO: We can change behavior so it only returns relevant paths, depending on year and month.

	if os.path.exists(pathindex_file): # If a path to files has already been created
		datapaths_pd = pd.read_csv(pathindex_file)
		datapaths_pd.columns = ['path', 'locations']
		# TODO filter away those with irrelevant locations 
		# TODO: query might work
		# if any("abc" in s for s in countries):
		datapaths_pd = datapaths_pd.dropna() # Quick way to do some filtration on paths (drop all with no location)
		datapaths_pd = datapaths_pd.assign(path=lambda x: os.getcwd() + x.path)
		datapaths = list(datapaths_pd["path"])
	else: # Otherwise go through all files. TODO: this goes through all extracted files. Not always what we want and time consuming. 
		datapaths = []
		for root, dirs, files in os.walk(data_path + 'extracted/'):
			for file in files:
				if file.endswith(".xml"):
					datapaths.append(root + "/" + file)
					#print(os.path.join(root, file))
	return datapaths

def read_params(param_path):
	# Utility function to read parameter files
	f = open(param_path, "r")
	params = f.read().split(', ')
	f.close()
	return(params)

def prepare_tarfiles(years, months, data_path, DEST, RAW):
# Checks whether tarfiles have been extracted, if not, it calls on extract_tarfile()

	year_months = itertools.product(years, months)
	# Extract content from compressed TGZ file. Each TGZ file represents a month with the news article from each day in that month 
	for ym in year_months:
		# Loop over cartesian product of years and months to extract TGZ files one by one.
		if not os.path.exists(data_path + DEST + ym[0] + "/" + ym[1]):
			# Only extract if destination file does not yet exist
			extract_tarfile(data_path, DEST, RAW, ym[0], ym[1])	
	return

def extract_tarfile(path, dest, raw, year, month):
	# Extract file if it exists
	# Write to path/dest/year
	if os.path.exists(path + raw + year + "/" + month + '.tgz'):
		file = tarfile.open(path + raw + year + "/" + month + '.tgz')
		file.extractall(path + dest + year + "/")
		file.close()

def get_location_string(bs_article):
		bs_location = bs_article.find_all("location")
		locs = ""
		for loc in bs_article.find_all("location"):
			locs = locs + "-" + loc.string
		return locs

def article_to_datarow(path_to_article, relevant_countries_list):
	# High level function that reads an article, 
	# Determines whether the location is relevant
	# If the location is relevant, it builds features for the article

	with open(path_to_article, 'r', encoding="utf8") as f:
		data = f.read()
		f.close()

	bs_article = BeautifulSoup(data, "xml")
	
	bs_locations = get_location_string(bs_article)

	pathindex = [path_to_article.replace(os.getcwd(), ""), bs_locations]

	if not relevant_location(bs_locations, relevant_countries_list):
		return {"datarow": None, "pathindex": pathindex}
	datarow = build_features(bs_article)
#	ipdb.set_trace()
	return {"datarow": datarow, "pathindex": pathindex}

def relevant_location(article_locations, relevant_countries_list):
	# Determines whether the location attribute of an article is relevant to our use case
	
	#locations = []

	#for location in article_locations:
	#	locations.append(location.string)

	locations = article_locations.split("-")

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
	locs = get_location_string(bs_article) # TODO: this is actually redundant
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

def write_list_to_table(l, write_to_file):

	field_names = [*l[0]]
	
	with open(write_to_file, 'w') as csvfile:
		writer = csv.writer(csvfile)
		writer.writerow(field_names)
		writer.writerows(l)
		csvfile.close()
	return

def write_dict_to_table(d, write_to_file):

	# Unique key values in all diciotnaries:
	field_names = list(set( val for dic in d for val in dic.keys()))
	
	with open(write_to_file, 'w') as csvfile:
		writer = csv.DictWriter(csvfile, fieldnames = field_names)
		writer.writeheader()
		writer.writerows(d)
		csvfile.close()
	return
