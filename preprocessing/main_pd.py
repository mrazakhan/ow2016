import re
import pandas as pd
import string
import nltk
from nltk.corpus import words




from nltk.tokenize import word_tokenize
from nltk.corpus import stopwords
from nltk.stem.porter import PorterStemmer

from joblib import Parallel, delayed
import multiprocessing

import argparse
import csv


districts=["rajanpur","chiniot","bhakkar","ghazi","mianwali","muzaffargarh","khushab","bahawalpur","multan","lahore","sialkot","rawalpindi","faisalabad","attock","rahimyarkhan","pakpattan","khanewal","lodhran","gujranwala","sahiwal","qasur","chakwal"]

def _filter_locations(x):
	
	for each in re.findall(r"\w+",str(x)):
		if each in districts:
			return each
	return 'NotFound'


def filter_locations(df):
	df['uLocation2']=df['uLocation'].apply(lambda x:_filter_locations(x))
	df2=df[df.uLocation2!='NotFound']
	return df2

	
def filter_tweet(df):
	df['tText2']=df['tText'].apply(_filter_locations)
	df2=df.filter_by('NotFound','uLocation2', exclude=True)
	return df2
	


def _isNonEnglish(x):
	if x in words.words() and len(x)>2: # Letting the short words go for the time being
		return 0
	else:
		return 1
		

def _detectLanguage(x):
	tokenized_array=re.findall(r"\w+",x)
	sum=0
	for each in tokenized_array:
		sum+=_isNonEnglish(each.strip())
	if sum<len(tokenized_array)/4.0:
		return 'English'
	else:
		return 'NonEnglish'


def detectLanguage(df):
	df['Language']=df['tText'].apply(_detectLanguage)
	return df
	


PUNCTUATION = set(string.punctuation)
STOPWORDS = set(stopwords.words('english'))
STEMMER = PorterStemmer()


# Function to break text into "tokens", lowercase them, remove punctuation and stopwords, and stem them
def tokenize(text):
    tokens = word_tokenize(text.decode('utf-8'))
    lowercased = [t.lower() for t in tokens]
    no_punctuation = []
    for word in lowercased:
        punct_removed = ''.join([letter for letter in word if not letter in PUNCTUATION])
        no_punctuation.append(punct_removed)
    no_stopwords = [w for w in no_punctuation if not w in STOPWORDS]
    stemmed = [STEMMER.stem(w) for w in no_stopwords]
    return [w for w in stemmed if _isNonEnglish(w)]


	
def getNonEnglish(df):
	df['NonEngTokens']=df['tText'].apply(tokenize)
	return df


	
def applyParallel(dfGrouped, func):
    retLst = Parallel(n_jobs=multiprocessing.cpu_count())(delayed(func)(group) for name, group in dfGrouped)
    return pd.concat(retLst)


if __name__=='__main__':

	parser=argparse.ArgumentParser(description='Preprocessing and filtering')
	parser.add_argument('-if','--input_file',help='Input File', required=True)
	parser.add_argument('-of','--output_file',help='Output File', required=True)

	args=parser.parse_args()
	df=pd.read_csv(args.input_file, header=None, delimiter='\t', error_bad_lines =False)

	df.columns=['tID','eName','eType','tText','tUrl','tTitleForUrl','tHashtags','tTotalRetweetCount','tIsRetweet','tOriginalID','tPostTime','tUtcOffset','tReceivedTime','tAdultScore','tQualityScore','tAuthorityScore','uScreenName','uFollowersCount','uIsVerified','tWordBrokenHashtag','tGeoPoint','uLocation','uTimeZone','tFavoriteCount','tReceivedHour']

	df_loc=filter_locations(df)

	dfEng=detectLanguage(df_loc)


	dfEng2=applyParallel(df_loc[['tID','tText','uLocation']].groupby(df_loc.index), detectLanguage)

	dfEng2.to_csv(args.output_file)



