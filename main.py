import graphlab as gl

if __name__=='__main__':
	df=gl.SFrame.read_csv('EducationPakistan_201301.tsv', delimiter='\t', header=None)

	df.rename({'X1':'tID','X2':'eName','X3':'eType','X4':'tText','X5':'tUrl','X6':'tTitleForUrl','X7':'tHashtags','X8':'tTotalRetweetCount','X9':'tIsRetweet','X10':'tOriginalID','X11':'tPostTime','X12':'tUtcOffset','X13':'tReceivedTime','X14':'tAdultScore','X15':'tQualityScore','X16':'tAuthorityScore','X17':'uScreenName','X18':'uFollowersCount','X19':'uIsVerified','X20':'tWordBrokenHashtag','X21':'tGeoPoint','X22':'uLocation','X23':'uTimeZone','X24':'tFavoriteCount','X25':'tReceivedHour'})

	

