#! /usr/bin/python

# Main logic for the project, from here, everything building the project will be called upon.
import os

# Parameters ####
# Note, parameters related to what NYT data to screen is defined in parameters/
ANALYTICS_FILE = '/data/analysis/test2.csv' # filename containing cleaned data.
#host = os.uname()[1]
host = os.getlogin()

# Load data and store more conveniently
from src.data_creation.c0_parse_data import parse_data
if not os.path.exists(os.getcwd() + ANALYTICS_FILE) and "fime3720" in host :
	parse_data()

# Create output from exported data
from src.analysis.c1_analyze import generate_output
if "fime3720" in host:
	generate_output()


# Compile paper/presentation using new output
if not "fime3720" in host :
	# Conditional to avoid some system error. 
	os.system("pdflatex docs/paper.tex")
	os.system("mv paper.* docs")

print("DONE!")

