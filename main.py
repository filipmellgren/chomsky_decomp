#! /usr/bin/python

# Main logic for the project, from here, everything building the project will be called upon.
import os


# Load data and store more conveniently
from src.data_creation.c0_parse_data import parse_data
if not os.path.exists(os.getcwd() + '/data/analysis/test2.csv'):
	parse_data()

# Create output from exported data

# Compile paper/presentation using new output

print("DONE!")