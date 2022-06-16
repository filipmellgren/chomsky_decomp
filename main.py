#! /usr/bin/python
# A year, month takes ca 93 second for one cpu ≈ 13 seconds for 7 cpus. 
# there are ca 20*12 year months. Looping through these should take 20*12*93secs/(60secs/min*60min/hours) = 6.2 hours on one cpu. 

if __name__ == "__main__":
	# Main logic for the project, from here, everything building the project will be called upon.
	import os
	

	# Parameters ####
	# Note, parameters related to what NYT data to screen is defined in parameters/
	ANALYTICS_FILE = '/data/analysis/pilotstudy.csv' # filename containing cleaned data.
	host = os.getlogin()
	
	# Load data and store more conveniently
	from src.data_creation.c0_parse_data import parse_data
	if not os.path.exists(os.getcwd() + ANALYTICS_FILE):
		parse_data()
		
	# Create output from exported data
	from src.analysis.c1_analyze import generate_output
	generate_output()

	# Compile paper/presentation using new output
	if not "fime3720" in host :
		# Conditional to avoid some system error. 
		os.system("pdflatex docs/paper.tex")
		os.system("mv paper.* docs")
	print("DONE!")

