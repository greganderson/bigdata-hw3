page_map = word_counts.map(lambda x: (page_id, list(x))).groupByKey()
