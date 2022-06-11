#! /usr/bin/python

# Main logic for the project, from here, everything building the project will be called upon.
import os

# Parameters ####
# Note, parameters related to what NYT data to screen is defined in parameters/
ANALYTICS_FILE = '/data/analysis/test2.csv' # filename containing cleaned data.
host = os.uname()[1]

# Load data and store more conveniently
from src.data_creation.c0_parse_data import parse_data
if not os.path.exists(os.getcwd() + ANALYTICS_FILE) and "Filip" in host :
	parse_data()

# Create output from exported data
from src.analysis.c1_analyze import generate_output
if "Filip" in host:
	generate_output()


# Compile paper/presentation using new output
os.system("pdflatex docs/paper.tex")
os.system("mv paper.* docs")

print("DONE!")

