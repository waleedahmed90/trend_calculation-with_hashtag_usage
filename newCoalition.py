import gzip
import json
import glob
import itertools
from pandas import DataFrame
#import matplotlib.pyplot as plt
import ntpath
import time
#create month based tweets count and calculate monthly surges OR maybe something else not sure yet what to do in this code




if __name__== "__main__":
	start_time = time.time()

	
	gend_link = '/Users/WaleedAhmed/Documents/THESIS_DS_CODE/June++/dataReadCode_2/Percentage_Demographics/Gender_Percentage_User_Demographics.gz'

	race_link = '/Users/WaleedAhmed/Documents/THESIS_DS_CODE/June++/dataReadCode_2/Percentage_Demographics/Race_Percentage_User_Demographics.gz'

	age_link = '/Users/WaleedAhmed/Documents/THESIS_DS_CODE/June++/dataReadCode_2/Percentage_Demographics/Age_Percentage_User_Demographics.gz'


	with gzip.open(gend_link, 'rt') as g:
		gend_temp = g.read()
	g.close()

	gend_perc_dict = json.loads(gend_temp)
	
	with gzip.open(race_link, 'rt') as r:
		race_temp = r.read()
	r.close()

	race_perc_dict = json.loads(race_temp)
	
	with gzip.open(age_link, 'rt') as a:
		age_temp = a.read()
	a.close()

	age_perc_dict = json.loads(age_temp)


	top_10_daily = '/Users/WaleedAhmed/Documents/THESIS_DS_CODE/June++/dataReadCode_2/Code_HashtagUsage/top_10_daily_tweets/*.gz'
	
	list_of_files = sorted(glob.glob(top_10_daily))
	print("Total Files: ", len(list_of_files))

	#F = list_of_files[0]

	trend_gend = {}
	trend_race = {}
	trend_age = {}

	for F in list_of_files:
		with gzip.open(F, 'rt') as f:
			daily_counts = f.read()
		f.close()

		trending_temp_dict_per_day = json.loads(daily_counts)
		
		for trend in trending_temp_dict_per_day.keys():

			if trend in gend_perc_dict.keys():
				trend_gend[trend] = gend_perc_dict[trend]
				trend_race[trend] = race_perc_dict[trend]
				trend_age[trend] = age_perc_dict[trend]

	print(len(trend_gend))
	print(len(trend_race))
	print(len(trend_age))

	print(trend_gend)

	gend_path = '/Users/WaleedAhmed/Documents/THESIS_DS_CODE/June++/dataReadCode_2/Code_HashtagUsage/trend_perc/gend_perc.gz'
	race_path = '/Users/WaleedAhmed/Documents/THESIS_DS_CODE/June++/dataReadCode_2/Code_HashtagUsage/trend_perc/race_perc.gz'
	age_path = '/Users/WaleedAhmed/Documents/THESIS_DS_CODE/June++/dataReadCode_2/Code_HashtagUsage/trend_perc/age_perc.gz'

	with gzip.open(gend_path, 'wb') as f1:
		f1.write(json.dumps(trend_gend).encode('utf-8'))
	f1.close()


	with gzip.open(race_path, 'wb') as f2:
		f2.write(json.dumps(trend_race).encode('utf-8'))
	f2.close()


	with gzip.open(age_path, 'wb') as f3:
		f3.write(json.dumps(trend_age).encode('utf-8'))
	f3.close()







	print("Elapsed Time")
	print("--- %s seconds ---" % (time.time() - start_time))