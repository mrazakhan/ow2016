import graphlab as gl
from preprocessing import *
import argparse
import csv


if __name__=='__main__':
	parser=argparse.ArgumentParser(description='Preprocessing and filtering')
	parser.add_argument('-if','--input_file',help='Input File', required=True)
	parser.add_argument('-of','--output_file',help='Output File', required=True)

	args=parser.parse_args()
	df=gl.SFrame.read_csv(parser.input_file, delimiter='\t', header=None)

	df.rename({'X1':'tID','X2':'eName','X3':'eType','X4':'tText','X5':'tUrl','X6':'tTitleForUrl','X7':'tHashtags','X8':'tTotalRetweetCount','X9':'tIsRetweet','X10':'tOriginalID','X11':'tPostTime','X12':'tUtcOffset','X13':'tReceivedTime','X14':'tAdultScore','X15':'tQualityScore','X16':'tAuthorityScore','X17':'uScreenName','X18':'uFollowersCount','X19':'uIsVerified','X20':'tWordBrokenHashtag','X21':'tGeoPoint','X22':'uLocation','X23':'uTimeZone','X24':'tFavoriteCount','X25':'tReceivedHour'})

	
	df_loc=filter_locations(df)
	dfEng=detectLanguage(df_loc)
	dfEng.export_csv(parser.output_file, quote_level=csv.QUOTE_NONE)
	
