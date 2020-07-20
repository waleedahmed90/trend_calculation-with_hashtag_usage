#/Users/WaleedAhmed/Documents/THESIS_DS_CODE/June++/dataReadCode_2/hashtag_Usage_Info(3months)

import gzip
import json
import glob
import itertools
from pandas import DataFrame
import matplotlib.pyplot as plt
import ntpath
import time

def countZeroes(row_vector):
	r = list(row_vector)
	trend_weight = len(r) - r.count(0)
	return trend_weight



def plotSurges(SurgeDict, top_topics, graphName):
	df = DataFrame(SurgeDict, columns = SurgeDict.keys())
	df = df.T
	[rows, cols] = df.shape

	#####print(type(topics_list))
	#sorting by threshold
	#threshold 5 or 10
	
	
	x_axis = df.columns.values.tolist()
	
	#df = df.sort_values(by=x_axis, inplace=True, ascending=False)
	df["count_0"] = (df == 0).sum(axis=1)
	#df.sort_values(by=x_axis, inplace=True, ascending=False)
	df.sort_values(by=["count_0"], inplace=True, ascending=True)
	df = df.drop(["count_0"], axis=1)

	topics_list = df.index.values
	
	# print("top ",top_topics)

	# print()

	# df1 = df.iloc[10]	
# #################################################################################
# 	#plotting code
# 	for i in range(0,top_topics):
# 		plt.plot(x_axis, df.iloc[i], label = topics_list[i]) 
		
# 	# naming the x axis 
# 	plt.xlabel('Time Stamps') 
# 	# naming the y axis 
# 	plt.ylabel('Surge Ratios') 
# 	# giving a title to my graph 
# 	plt.title('Surge Ratios over the day') 
		  
# 	# show a legend on the plot 
# 	plt.legend() 
		  
# 	# function to show the plot 
# 	plt.savefig(graphName)
# 	plt.close()	 
# 	#plt.show()
# #################################################################################
	
	return df.iloc[:top_topics]

def surgeCalculation(stamp_list, Usage_threshold):
	total_stamps = len(stamp_list)
	surgeList = timeseries(tot_stamps)
	surgeList[0] = stamp_list[0]
	for location in range(1, tot_stamps):
		#previous
		t = stamp_list[location-1]
		#current
		t_1 = stamp_list[location]
		
		if t_1>=Usage_threshold:
			if t==0:
				surgeList[location] = t_1
			else:
				surgeList[location] = t_1/t

		#without threshold
		# if t==0 and t_1==0:
		# 	surgeList[location] = 0.0
		# elif t==0 and t_1!=0:
		# 	surgeList[location] = t_1
		# else:
		# 	surgeList[location] = t_1/t
	return surgeList

def timeseries(stamps):
	x = [0]*stamps
	return x

def extractHashtags(usage_Info):
	hashtags_dict = {}
	usage_Info_list = usage_Info.split("\n")
	tot_stamps = len(usage_Info_list)-1

	for s in range(tot_stamps):
		element = usage_Info_list[s].split("\t")
		#timeSt = element[0]
		topics = json.loads(element[1])
		for t in topics.keys():
			hashtags_dict[t] = timeseries(tot_stamps)
	#a dictionary with hashtags as names and an empty list with 96 time stamps with all zeros 
	return hashtags_dict

if __name__== "__main__":
	start_time = time.time()
	###########################################################
	#Path to directory containing files
	path_hashtagsDemo = "/Users/WaleedAhmed/Documents/THESIS_DS_CODE/June++/dataReadCode_2/hashtag_Usage_Info(3months)/*.gz"
	#Path String
	list_of_files = glob.glob(path_hashtagsDemo)
	print("Total Files: ", len(list_of_files))

	#F = list_of_files[1]
	details = {}
	#graphName = ntpath.basename(F)+".png"
	file_counter_temp = 1
	for F in list_of_files:
		with gzip.open(F, 'rt') as f:
			usageInfo = f.read()
			graphName = ntpath.basename(F)+".png"
			fileName_d = ntpath.basename(F)
		f.close()

		#this dictionary just contains hashtags lists and zero for every timestamp 
		hashListingDict = extractHashtags(usageInfo)
		#hashtags_dict = {}
		usage_Info_list = usageInfo.split("\n")
		tot_stamps = len(usage_Info_list)-1
		#timestamp index
		ts_ind = 0

		for s in range(tot_stamps):
			element = usage_Info_list[s].split("\t")
			timeSt = element[0]
			topics = json.loads(element[1])
			#all trending topics in that timestamp
			for t in topics.keys():
				#get the length of the promoter list of that topic
				tot_promoters = len(topics[t])
				#get the previous list available for the time stamps based details of the number of promoters
				temp_stamp_list = hashListingDict[t]
				#put the number of promoters in that list on current timestamp index
				temp_stamp_list[ts_ind] = tot_promoters
				#assign the list back to its hashtag
				hashListingDict[t] = temp_stamp_list

			#increment the ts_index
			ts_ind = ts_ind+1



		SurgeDict = {}
		#setting a threshold in usage (5 or 10)
		usage_threshold = 10

		for h in hashListingDict.keys():
			SurgeDict[h] = surgeCalculation(hashListingDict[h], usage_threshold)


		#print(SurgeDict)

		#this function plots rise in usage (surge) of a hastag over the day
		#top_topics (int) top number of tags which we want to plot
		#returns dictionary of top tags  with their surge ratios
		top_topics=10
		top_topics_frame = plotSurges(SurgeDict, top_topics, graphName)
		print(top_topics_frame)

		trending_topics_10 = top_topics_frame.index.values

		top_10_day_tweets = {}
		#saves a dictionary of end of the day top 10 trends
		for t in trending_topics_10:
			top_10_day_tweets[t] = sum(hashListingDict[t])
#/Users/WaleedAhmed/Documents/THESIS_DS_CODE/June++/dataReadCode_2/Code_HashtagUsage/top_10_daily_tweets/
		daily_counter = '/Users/WaleedAhmed/Documents/THESIS_DS_CODE/June++/dataReadCode_2/Code_HashtagUsage/top_10_daily_tweets/'+fileName_d
		with gzip.open(daily_counter, 'wb') as f3:
			f3.write(json.dumps(top_10_day_tweets).encode('utf-8'))
		f3.close()

		#saves a dictioary of daily top 10 surge ratios
		surge_counter = '/Users/WaleedAhmed/Documents/THESIS_DS_CODE/June++/dataReadCode_2/Code_HashtagUsage/top_10_daily_surges/'+fileName_d
		top_10_day_surges = {}

		df1  =top_topics_frame.T
		top_10_day_surges = df1.to_dict()

		with gzip.open(surge_counter, 'wb') as f4:
			f4.write(json.dumps(top_10_day_surges).encode('utf-8'))
		f4.close()

		print("FILE: ",file_counter_temp," Done")
		file_counter_temp = file_counter_temp+1
		print("Elapsed Time")
		print("--- %s seconds ---" % (time.time() - start_time))

