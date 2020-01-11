import pandas as pd
import time
from multiprocessing.pool import Pool
from multiprocessing import Manager

#import seaborn as pd

cluster_df = pd.read_csv('address_clust.csv')
stats_df = pd.read_csv('address_stats.csv')
#1

cluster_df = cluster_df.merge(stats_df, on='address_id', how='right').fillna(0).astype('int64')
cluster_df.drop_duplicates(inplace=True)
cluster_df.to_csv('out.csv')

transactions = cluster_df.transaction_id.unique()
start = time.time()

manager = Manager()


first_to_second = manager.list()
second_to_first = manager.list()

first_to_zero = manager.list()
zero_to_first = manager.list()

second_to_zero = manager.list()
zero_to_second = manager.list()

first_fee = manager.list()
second_fee = manager.list()

def check_tr(i):
    global first_to_second,second_to_first,first_to_zero,zero_to_first,second_to_zero,zero_to_second,first_fee,second_fee
    transaction_df = cluster_df.loc[(cluster_df['transaction_id'] == transactions[i])]
    first = None
    second = None
    zero = None
    if (len(transaction_df.cluster_id.unique()) > 1):
        # First task
        if ((any(transaction_df.cluster_id == 1)) and (any(transaction_df.cluster_id == 2))):

            first = transaction_df.loc[(transaction_df['transaction_id'] == transactions[i]) & (transaction_df['cluster_id'] == 1)]
            second = transaction_df.loc[(transaction_df['transaction_id'] == transactions[i]) & (transaction_df['cluster_id'] == 2)]

            first_sent = first['sent'].sum()
            first_received = first['received'].sum()

            second_sent = second['sent'].sum()
            second_received = second['received'].sum()

            if first_sent != 0:
                first_to_second.append(second_received)

            if second_sent != 0:
                second_to_first.append(first_received)

        if ((any(transaction_df.cluster_id == 1)) and (any(transaction_df.cluster_id == 0))):
            if first is None:
                first = transaction_df.loc[(transaction_df['transaction_id'] == transactions[i]) & (transaction_df['cluster_id'] == 1)]

            zero = transaction_df.loc[(transaction_df['transaction_id'] == transactions[i]) & (transaction_df['cluster_id'] == 0)]

            first_sent = first['sent'].sum()
            first_received = first['received'].sum()

            zero_sent = zero['sent'].sum()
            zero_received = zero['received'].sum()

            if first_sent != 0:
                first_to_zero.append(zero_received)

            if zero_sent != 0:
                zero_to_first.append(first_received)

        if ((any(transaction_df.cluster_id == 2)) and (any(transaction_df.cluster_id == 0))):
            if second is None:
                second = transaction_df.loc[(transaction_df['transaction_id'] == transactions[i]) & (transaction_df['cluster_id'] == 2)]
            zero = transaction_df.loc[(transaction_df['transaction_id'] == transactions[i]) & (transaction_df['cluster_id'] == 0)]

            second_sent = second['sent'].sum()
            second_received = second['received'].sum()

            zero_sent = zero['sent'].sum()
            zero_received = zero['received'].sum()

            if second_sent != 0:
                second_to_zero.append(zero_received)

            if zero_sent != 0:
                zero_to_second.append(second_received)

        if ((any(transaction_df.cluster_id == 1)) or (any(transaction_df.cluster_id == 2))):
            if first is None:
                first = transaction_df.loc[(transaction_df['transaction_id'] == transactions[i]) & (transaction_df['cluster_id'] == 1)]
            if second is None:
                second = transaction_df.loc[(transaction_df['transaction_id'] == transactions[i]) & (transaction_df['cluster_id'] == 2)]

            first_sent = first['sent'].sum()
            second_sent = second['sent'].sum()

            if first_sent != 0:
                first_fee.append(first_sent - transaction_df['received'].sum())
            if second_sent != 0:
                second_fee.append(second_sent - transaction_df['received'].sum())

pool = Pool(4)
#
for i in pool.imap_unordered(check_tr, range(0,len(transactions) - 1)):
    pass
print('Done',time.time() - start)

print('From the first to the second cluster', sum(first_to_second)/100000000)
print('From the second to the first cluster', sum(second_to_first)/100000000)

print('From the first to the zero cluster', sum(zero_to_first)/100000000)
print('From the zero to the first cluster', sum(first_to_zero)/100000000)

print('From the second to the zero cluster', sum(second_to_zero)/100000000)
print('From the zero to the second cluster', sum(zero_to_second)/100000000)

print('First cluster fee', sum(first_fee)/100000000)
print('Second cluster fee', sum(second_fee)/100000000)
