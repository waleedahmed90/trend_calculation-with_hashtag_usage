#/Users/WaleedAhmed/Documents/THESIS_DS_CODE/June++/dataReadCode_2/Percentage_Demographics

import time
start_time = time.time()

import gzip
import json
import numpy as np

from pandas import DataFrame
import matplotlib.pyplot as plt
from sklearn.cluster import KMeans
from mpl_toolkits import mplot3d 

#This function counts the top_h hashtags assigned to every cluster centroid
def topHashtagsPerCluster(top_h, Cluster_details, Sorted_Dictionary, T_Count):

	keys__1 = list(T_Count.keys())
	for cent in Cluster_details:
		hash_list = Cluster_details[cent]
		print("\nCLUSTER: ",cent, ": ", keys__1[cent])
		TEMP_DICT = {}

		for h in hash_list:
			TEMP_DICT[h] = Sorted_Dictionary[h]

		temp_sort_demo = sorted(TEMP_DICT.items(), key = lambda x: x[1], reverse=True)

		flag_1 = 0
		for i in temp_sort_demo:
			if not flag_1==top_h:
				print(i[0], " ::: ", i[1])
				flag_1 = flag_1+1


def nameClusterRoundOff(list_cent_r):
	x = list_cent_r
	y = [round(i, 3) for i in x]
	clus_name = str(y)
	return clus_name

def printClusterAssignmentDetails(clus_detail):
	for i in clus_detail:
		print(i,": ", clus_detail[i])


def plotRaceClusters(df, labels, centroids):
	ax = plt.axes(projection = '3d')
	ax.scatter(df.iloc[:,0],df.iloc[:,1],df.iloc[:,2] , c=labels.astype(float), cmap='Set2', s=50, alpha=0.4)
		
	ax.set_xlabel('White')
	ax.set_ylabel('Black')
	ax.set_zlabel('Asian');

	ax.scatter(centroids[:,0],centroids[:,1],centroids[:,2] ,c='r', s=200, label='centroid', alpha=1)
	plt.show()


def clusterGrouping (hashtags, labels, centroids, n_clus):
	#has
	clusterDetails = {}

	for i in range(0, n_clus):
		clusterDetails[i]=[]

	#print("Empty_Clusters\n\n", clusterDetails, "\n")
	

	for p in range(len(labels)):

		cluster_number = labels[p]
		hashtagName = hashtags[p]
		#print(hashtagName)
		clusterDetails[cluster_number].append(hashtagName)	

	return clusterDetails



def clustering (dataset, n_clus):
	#dataset: dictionary of stats
	#n_clus: No of cluster

	df = DataFrame(dataset, columns=dataset.keys())
	df = df.T
	#print("DATAFRAME")
	#print(df)
	#print(":::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::")
	

	kmeans = KMeans(n_clusters=n_clus).fit(df)
	
	#cluster centers
	centroids = kmeans.cluster_centers_
	#assined clusters
	labels = kmeans.labels_
	#names of hashtags
	hashtag_names = df.index.values


	#function call
	clusterDetails = clusterGrouping(hashtag_names, labels, centroids, n_clus)

	return df, centroids, clusterDetails, labels, hashtag_names



path_gender = '/Users/WaleedAhmed/Documents/THESIS_DS_CODE/June++/dataReadCode_2/Code_HashtagUsage/trend_perc/gend_perc.gz'
path_race = '/Users/WaleedAhmed/Documents/THESIS_DS_CODE/June++/dataReadCode_2/Code_HashtagUsage/trend_perc/race_perc.gz'
path_age = '/Users/WaleedAhmed/Documents/THESIS_DS_CODE/June++/dataReadCode_2/Code_HashtagUsage/trend_perc/age_perc.gz'



#Loading dictionaries of Promoter percentage Gender
with gzip.open(path_gender,'rt') as T1:
	demo_gender_temp = T1.read()
T1.close()

stats_gender = json.loads(demo_gender_temp)

#Loading dictionaries of Promoter percentage Race
with gzip.open(path_race,'rt') as T2:
	demo_race_temp = T2.read()
T2.close()

stats_race = json.loads(demo_race_temp)

#Loading dictionaries of Promoter percentage Age
with gzip.open(path_age,'rt') as T3:
	demo_age_temp = T3.read()
T3.close()

stats_age = json.loads(demo_age_temp)

dataset_g = stats_gender
n_clus_g = 5

dataset_r = stats_race
n_clus_r = 7

dataset_a = stats_age
n_clus_a = 7


df_g, centroids_g, clusterDetails_g, labels_g, hashtag_names_g = clustering (dataset_g, n_clus_g)
df_r, centroids_r, clusterDetails_r, labels_r, hashtag_names_r = clustering (dataset_r, n_clus_r)
df_a, centroids_a, clusterDetails_a, labels_a, hashtag_names_a = clustering (dataset_a, n_clus_a)


# # print(clusterDetails_g)
# print(centroids_g)
# # print(clusterDetails_r)
# print(centroids_r)
# # print(clusterDetails_a)
# print(centroids_a)

######Trend_counts######
T_Count_Gender = {}
T_Count_Race = {}
T_Count_Age = {}


list_cent_g = centroids_g.tolist()
list_cent_r = centroids_r.tolist()
list_cent_a = centroids_a.tolist()
#print(list_cent_g)


# name = str(centroids_g[0])
# print(name)
# print(type(name))

for c in clusterDetails_g:
	temp_clus_name = nameClusterRoundOff(list_cent_g[c])
	#temp_clus_name = str(centroids_g[c])
	T_Count_Gender[temp_clus_name] = len(clusterDetails_g[c])

for c in clusterDetails_r:
	temp_clus_name = nameClusterRoundOff(list_cent_r[c])
	#temp_clus_name = str(centroids_r[c])
	T_Count_Race[temp_clus_name] = len(clusterDetails_r[c])

for c in clusterDetails_a:
	temp_clus_name = nameClusterRoundOff(list_cent_a[c])
	#temp_clus_name = str(centroids_a[c])
	T_Count_Age[temp_clus_name] = len(clusterDetails_a[c])








print("GENDER_CLUSTERS_HASHTAGS_COUNTS")
printClusterAssignmentDetails(T_Count_Gender)

print("\nRACE_CLUSTERS_HASHTAGS_COUNTS")
printClusterAssignmentDetails(T_Count_Race)

print("\nAGE_CLUSTERS_HASHTAGS_COUNTS")
printClusterAssignmentDetails(T_Count_Age)


#TEMP_DICT = {}
###########################TOPE_TWEETS#########################
#opTweets(ClusterDetails, Sorted_dictionary, )
#########################################################

with gzip.open('Sorted_Dictionary.gz', 'rt') as T4:
	Sorted_Dictionary_temp = T4.read()
T4.close()

Sorted_dictionary = json.loads(Sorted_Dictionary_temp)

#########################################################

#Gender
top_h = 13
Cluster_details = clusterDetails_g
T_Count = T_Count_Gender
print("\n\n#########################################")
print("GENDER [MALE, FEMALE]")
topHashtagsPerCluster(top_h, Cluster_details, Sorted_dictionary, T_Count)

#Race
top_h=13
Cluster_details = clusterDetails_r
T_Count = T_Count_Race
print("\n\n#########################################")
print("RACE [WHITE, BLACK, ASIAN]")
topHashtagsPerCluster(top_h, Cluster_details, Sorted_dictionary, T_Count)

# AGe
top_h = 13
Cluster_details = clusterDetails_a
T_Count = T_Count_Age
print("\n\n#########################################")
print("AGE ['-20', '65+', '20-40', '40-65']")
topHashtagsPerCluster(top_h, Cluster_details, Sorted_dictionary, T_Count)



print("Elapsed Time")
print("--- %s seconds ---" % (time.time() - start_time))

# plt.show()

