#import pandas as pd
import modin.pandas as pd
import time
cluster_df = pd.read_csv('address_clust.csv')
stats_df = pd.read_csv('address_stats.csv')
#1

cluster_df = cluster_df.merge(stats_df, on='address_id', how='right').fillna(0).astype('int64')
cluster_df.drop_duplicates(inplace=True)
cluster_df.to_csv('out.csv')

transactions = cluster_df.transaction_id.unique()
start = time.time()
for i in range(len(transactions)):
    transaction_df = cluster_df.loc[(cluster_df['transaction_id'] == transactions[i])]

    if (len(transaction_df.cluster_id.unique()) > 1):

        if ((any(transaction_df.cluster_id == 1)) and (any(transaction_df.cluster_id == 2))):

            first = transaction_df.loc[(transaction_df['transaction_id'] == transactions[i]) & (transaction_df['cluster_id'] == 1)]
            second = transaction_df.loc[(transaction_df['transaction_id'] == transactions[i]) & (transaction_df['cluster_id'] == 2)]

            first_sent = first['sent'].sum()
            first_received = first['received'].sum()
            second_sent = second['sent'].sum()
            second_received = second['received'].sum()

            if first_sent != 0:
                print(transaction_df)
                print(first_sent)
                print(second_received)

            if second_sent != 0:
                print(transaction_df)
                print(second_sent)
                print(first_received)

#print(cluster_df.loc[cluster_df['cluster_id'].isin([1,2])])
print('Done',time.time() - start)