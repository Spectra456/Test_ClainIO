import pandas as pd
import time
from multiprocessing.pool import Pool
from multiprocessing import Manager, cpu_count
import numpy as np

start = time.time()
# Read csv files

cluster_df = pd.read_csv('address_clust.csv')
stats_df = pd.read_csv('address_stats.csv')

# Merge(join) dataframes address id and cluster id
cluster_df = cluster_df.merge(stats_df, on='address_id', how='right').fillna(0).astype('int64')
# Delete duplicate rows
cluster_df.drop_duplicates(inplace=True)
# Convert dataframe to numpy array, because it's faster
cluster_np = cluster_df.to_numpy()
# get list of transactions
transactions = np.unique(cluster_np[:,3])

# Using shared list for getting data from process
manager = Manager()

first_to_second = manager.list()
second_to_first = manager.list()
first_to_zero = manager.list()
zero_to_first = manager.list()
second_to_zero = manager.list()
zero_to_second = manager.list()
first_fee = manager.list()
second_fee = manager.list()

def check_transaction(i):
    """
    Getting id of transaction and handle it
    :param i:
    :return: adding data to shared list
    """
    # Get all records about current transactions
    transaction_np = cluster_np[np.where(cluster_np[:,3] == transactions[i])]

    first = None
    second = None
    zero = None
    # Checking if in transaction is involved more than 1 cluster
    if (len(np.unique(transaction_np[:,1])) > 1):

        # First task
        # Checking if in transaction is involved first and second cluster
        if ((any(transaction_np[:,1] == 1)) and (any(transaction_np[:,1] == 2))):
            # Getting all records from transaction about first cluster
            first = transaction_np[np.where(transaction_np[:,1] == 1)]
            # Getting all records from transaction about second cluster
            second = transaction_np[np.where(transaction_np[:,1] == 2)]

            # Getting sum of sent satoshi for the first cluster
            first_sent = np.sum(first[:,5])
            # Getting sum of received satoshi for the first cluster
            first_received = np.sum(first[:,4])

            # Getting sum of sent satoshi for the first cluster
            second_sent = np.sum(second[:,5])
            # Getting sum of received satoshi for the first cluster
            second_received = np.sum(second[:,4])

            # If first cluster send some money, then add received money for second cluster
            if first_sent != 0:
                first_to_second.append(second_received)

            # If second cluster send some money, then add received money for first cluster
            if second_sent != 0:
                second_to_first.append(first_received)

        # Second task
        # Same as in first task
        if ((any(transaction_np[:,1] == 1)) and (any(transaction_np[:,1] == 0))):
            if first is None:
                first = transaction_np[np.where(transaction_np[:, 1] == 1)]

            zero = transaction_np[np.where(transaction_np[:,1] == 0)]

            first_sent = np.sum(first[:, 5])
            first_received = np.sum(first[:, 4])

            zero_sent = np.sum(zero[:, 5])
            zero_received = np.sum(zero[:, 4])

            if first_sent != 0:
                first_to_zero.append(zero_received)

            if zero_sent != 0:
                zero_to_first.append(first_received)

        # Third task
        # Same as in first task
        if ((any(transaction_np[:,1] == 2)) and (any(transaction_np[:,1] == 0))):
            if second is None:
                second = transaction_np[np.where(transaction_np[:, 1] == 2)]
            if zero is None:
                zero = transaction_np[np.where(transaction_np[:,1] == 0)]

            second_sent = np.sum(second[:, 5])
            second_received = np.sum(second[:, 4])

            zero_sent = np.sum(zero[:, 5])
            zero_received = np.sum(zero[:, 4])

            if second_sent != 0:
                second_to_zero.append(zero_received)

            if zero_sent != 0:
                zero_to_second.append(second_received)

    # Fourth task
    # Same as in first task

    if ((any(transaction_np[:,1] == 1)) or (any(transaction_np[:,1] == 2))):
        if first is None:
            first = transaction_np[np.where(transaction_np[:,1] == 1)]

        if second is None:
            second = transaction_np[np.where(transaction_np[:, 1] == 2)]

        first_sent = np.sum(first[:, 5])
        second_sent = np.sum(second[:, 5])
        # Calculating fee
        if first_sent != 0:
            first_fee.append(first_sent - np.sum(transaction_np[:, 4]))

        if second_sent != 0:
            second_fee.append(second_sent - np.sum(transaction_np[:, 4]))

# Launch pool of process to handle each transaction
pool = Pool(cpu_count())
for i in pool.imap_unordered(check_transaction, range(0, len(transactions) - 1)):
    pass

print('Done',time.time() - start)

print('Amount of sent bitcoins from the first cluster to the second cluster', sum(first_to_second)/100000000)
print('Amount of sent bitcoins from the second cluster to the first cluster', sum(second_to_first)/100000000)

print('Amount of sent bitcoins from the first cluster to the zero cluster', sum(zero_to_first)/100000000)
print('Amount of sent bitcoins from the zero cluster to the first cluster', sum(first_to_zero)/100000000)

print('Amount of sent bitcoins from the second cluster to the zero cluster', sum(second_to_zero)/100000000)
print('Amount of sent bitcoins from the zero cluster to the second cluster', sum(zero_to_second)/100000000)

print('Amount of the first cluster fee', sum(first_fee)/100000000)
print('Amount of the second cluster fee', sum(second_fee)/100000000)
