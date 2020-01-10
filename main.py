import pandas as pd
import time
cluster_df = pd.read_csv('address_clust.csv')
stats_df = pd.read_csv('address_stats.csv')
#1
def check_transaction(i):
    transaction_df = cluster_df.loc[(cluster_df['transaction_id'] == transactions[i])]

    if (len(transaction_df.cluster_id.unique()) > 1):
        # First task
        if ((any(transaction_df.cluster_id == 1)) and (any(transaction_df.cluster_id == 2))):

            first = transaction_df.loc[
                (transaction_df['transaction_id'] == transactions[i]) & (transaction_df['cluster_id'] == 1)]
            second = transaction_df.loc[
                (transaction_df['transaction_id'] == transactions[i]) & (transaction_df['cluster_id'] == 2)]

            first_sent = first['sent'].sum()
            first_received = first['received'].sum()
            second_sent = second['sent'].sum()
            second_received = second['received'].sum()

            if first_sent != 0:
                first_to_second += second_received

            if second_sent != 0:
                second_to_first += first_received

        if ((any(transaction_df.cluster_id == 1)) and (any(transaction_df.cluster_id == 0))):

            first = transaction_df.loc[
                (transaction_df['transaction_id'] == transactions[i]) & (transaction_df['cluster_id'] == 1)]
            zero = transaction_df.loc[
                (transaction_df['transaction_id'] == transactions[i]) & (transaction_df['cluster_id'] == 0)]

            first_sent = first['sent'].sum()
            first_received = first['received'].sum()

            zero_sent = zero['sent'].sum()
            zero_received = zero['received'].sum()

            if first_sent != 0:
                first_to_zero += zero_received

            if zero_sent != 0:
                zero_to_first += first_received

        if ((any(transaction_df.cluster_id == 2)) and (any(transaction_df.cluster_id == 0))):

            second = transaction_df.loc[
                (transaction_df['transaction_id'] == transactions[i]) & (transaction_df['cluster_id'] == 2)]
            zero = transaction_df.loc[
                (transaction_df['transaction_id'] == transactions[i]) & (transaction_df['cluster_id'] == 0)]

            second_sent = second['sent'].sum()
            second_received = second['received'].sum()

            zero_sent = zero['sent'].sum()
            zero_received = zero['received'].sum()

            if second_sent != 0:
                second_to_zero += zero_received

            if zero_sent != 0:
                zero_to_second += second_received

        if ((any(transaction_df.cluster_id == 1)) or (any(transaction_df.cluster_id == 2))):

            first = transaction_df.loc[
                (transaction_df['transaction_id'] == transactions[i]) & (transaction_df['cluster_id'] == 1)]
            second = transaction_df.loc[
                (transaction_df['transaction_id'] == transactions[i]) & (transaction_df['cluster_id'] == 2)]

            first_sent = first['sent'].sum()
            second_sent = second['sent'].sum()

            if first_sent != 0:
                first_fee += first_sent - transaction_df['received'].sum()

            if second_sent != 0:
                second_fee += second_sent - transaction_df['received'].sum()


cluster_df = cluster_df.merge(stats_df, on='address_id', how='right').fillna(0).astype('int64')
cluster_df.drop_duplicates(inplace=True)
cluster_df.to_csv('out.csv')

transactions = cluster_df.transaction_id.unique()
start = time.time()

first_to_second = 0
second_to_first = 0

first_to_zero = 0
zero_to_first = 0

second_to_zero = 0
zero_to_second = 0

first_fee = 0
second_fee = 0

for i in range(len(transactions)):
    transaction_df = cluster_df.loc[(cluster_df['transaction_id'] == transactions[i])]

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
                first_to_second += second_received

            if second_sent != 0:
                second_to_first += first_received

        if ((any(transaction_df.cluster_id == 1)) and (any(transaction_df.cluster_id == 0))):

            first = transaction_df.loc[(transaction_df['transaction_id'] == transactions[i]) & (transaction_df['cluster_id'] == 1)]
            zero = transaction_df.loc[(transaction_df['transaction_id'] == transactions[i]) & (transaction_df['cluster_id'] == 0)]

            first_sent = first['sent'].sum()
            first_received = first['received'].sum()

            zero_sent = zero['sent'].sum()
            zero_received = zero['received'].sum()

            if first_sent != 0:
                first_to_zero += zero_received

            if zero_sent != 0:
                zero_to_first += first_received

        if ((any(transaction_df.cluster_id == 2)) and (any(transaction_df.cluster_id == 0))):

            second = transaction_df.loc[(transaction_df['transaction_id'] == transactions[i]) & (transaction_df['cluster_id'] == 2)]
            zero = transaction_df.loc[(transaction_df['transaction_id'] == transactions[i]) & (transaction_df['cluster_id'] == 0)]

            second_sent = second['sent'].sum()
            second_received = second['received'].sum()

            zero_sent = zero['sent'].sum()
            zero_received = zero['received'].sum()

            if second_sent != 0:
                second_to_zero += zero_received

            if zero_sent != 0:
                zero_to_second += second_received

        if ((any(transaction_df.cluster_id == 1)) or (any(transaction_df.cluster_id == 2))):

            first = transaction_df.loc[(transaction_df['transaction_id'] == transactions[i]) & (transaction_df['cluster_id'] == 1)]
            second = transaction_df.loc[(transaction_df['transaction_id'] == transactions[i]) & (transaction_df['cluster_id'] == 2)]

            first_sent = first['sent'].sum()
            second_sent = second['sent'].sum()

            if first_sent != 0:
                first_fee += first_sent - transaction_df['received'].sum()

            if second_sent != 0:
                second_fee += second_sent - transaction_df['received'].sum()


print('Done',time.time() - start)

print('From the first to the second cluster', first_to_second/100000000)
print('From the second to the first cluster', second_to_first/100000000)

print('From the first to the zero cluster', zero_to_first/100000000)
print('From the zero to the first cluster', first_to_zero/100000000)

print('From the second to the zero cluster', second_to_zero/100000000)
print('From the zero to the second cluster', zero_to_second/100000000)

print('First cluster fee', first_fee/100000000)
print('Second cluster fee', second_fee/100000000)
