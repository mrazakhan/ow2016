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

import os


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

def distribute_by_district(df):
	out={}
	for each in districts:
		out[each]=df[df['uLocation']==each]
	
	return out
	
def applyParallel(dfGrouped, func):
    retLst = Parallel(n_jobs=multiprocessing.cpu_count())(delayed(func)(group) for name, group in dfGrouped)
    return pd.concat(retLst)


if __name__=='__main__':
	import glob
	import random
	for name in districts:#'EducationPakistan_201301.tsv','EducationPakistan_201302.tsv', 'EducationPakistan_201303.tsv', 'EducationPakistan_201304.tsv', 'EducationPakistan_201305.tsv', 'EducationPakistan_201306.tsv']:
		print name
		frame = pd.DataFrame()
		list_ = []
		allFiles = glob.glob( name+ "/*loc")
		for file_ in allFiles:
    			#df = pd.read_csv(file_,index_col=None, header=0)
			df=pd.read_csv(file_,  delimiter='\t', error_bad_lines =False,quoting=csv.QUOTE_NONE)
    			list_.append(df)
		frame = pd.concat(list_)
		frame[['uScreenName']].describe().to_csv(name+'/description.csv')
		names_list_complete=list(set(frame['uScreenName'].tolist()))
		len_list=100
		if len(names_list_complete)<len_list:
			len_list=len(names_list_complete)
		names_list=random.sample(names_list_complete,len_list)
		with open(name+'/random_names.txt','w') as frn:
			for each in names_list:
				frn.write(each+' ')
				
		frame.to_csv(name+'/merged.csv',sep='\t')	
