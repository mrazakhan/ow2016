import graphlab as gl
from preprocessing import *
import argparse
import csv


if __name__=='__main__':
	parser=argparse.ArgumentParser(description='Preprocessing and filtering')
	parser.add_argument('-if','--input_file',help='Input File', required=True)
	parser.add_argument('-of','--output_file',help='Output File', required=True)

	args=parser.parse_args()
	df=gl.SFrame.read_csv(args.input_file, delimiter='\t', header=None)

	df.rename({'X1':'tID','X2':'tText'})

	df=df[['tID','tText']]	
	
	dfEng=detectLanguage(df).filter_by('English','Language')
	print 'Language Detection complete: ', dfEng.shape
	
	dfEng.export_csv(args.output_file, quote_level=csv.QUOTE_NONE, delimiter='\t')
	
