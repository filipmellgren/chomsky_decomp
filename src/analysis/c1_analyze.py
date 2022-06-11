# Main file for analysing data generated from the data creation step.
import pandas as pd
import matplotlib.pyplot as plt

def generate_output():
	# Main function from which plots are made.
	# This function calls are methods that create plots for the paper and presentation. 
	# Import data
	d = pd.read_csv("data/analysis/test.csv")
	# Make plot "test"
	test_plot(d)

def test_plot(data):
	testplot = plt.plot('publication_day_of_month', 'length', data = data)
	plt.savefig('output/plots/test.png')