def combine_itemsets(df_itemset, master_itemset):
	'''
	Takes the itemsets found in a dataset and adds 
	them to the tally maintained in master_itemsets

	'''
	for item in df_itemset.keys():
		if item not in master_itemset:
			master_itemset[item] = df_itemset[item]
		else: 
			master_itemset[item] += df_itemset[item]
	return master_itemset


def most_frequent_itemsets(master_itemset):
	'''
	Gets the keys with the highest value(s) in 
	master_itemset i.e. across all datasets

	'''
	#print(master_itemset)
	master_itemset = sorted(master_itemset.items(), key = 
             lambda kv:(kv[1]), reverse=True)
	result_set = set()
	curr_max = master_itemset[0][1]
	for item in master_itemset:
		if curr_max == item[1]:
			result_set.add(item[0])
	return result_set