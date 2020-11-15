# DEPENDENCIES
import pandas as pd
import os as os
import struct

# FUNCTIONS
# read and print bat file to the console
def readBatFile(filename):
    with open(filename, "rb") as binary_file:
        data = binary_file.read()
        print(data)


# Extract header data
def readBatFileHeader(filename):
    with open(filename, "rb") as binary_file:
        binary_file.seek(0, 0)
        name = str(binary_file.read(4), 'utf-8')
        version = int.from_bytes(binary_file.read(1), "big")
        records = int.from_bytes(binary_file.read(4), "big", signed=False)
    return (name,version,records)

# Print Header Data
def PrintBatFileHeaderData(filename):
    name, version, records = readBatFileHeader(filename)
    print("---- start of header ----")
    print(name)
    print('version: ', version)
    print('records: ', records)
    print("---- end of header ----")



# Extract data, export as dataframe
def extractBatFileData(filename):
    with open(filename, "rb") as binary_file:

        MPS7_dataframe = pd.DataFrame(
            columns=['record type', 'unix timestamp', 'user ID', 'amount USD'])

        binary_file.seek(9, 0)
        count = 0

        while count < records:
            record_type = int.from_bytes(binary_file.read(1), "big")
            time = int.from_bytes(binary_file.read(4), "big")
            userID = int.from_bytes(binary_file.read(8), "big")

            if record_type == 0 or record_type == 1:
                amount = struct.unpack('>d', binary_file.read(8))[0]
                MPS7_dataframe = MPS7_dataframe.append({
                    'record type': record_type,
                    'unix timestamp': time,
                    'user ID': userID,
                    'amount USD': amount
                }, ignore_index=True)

            if record_type == 2 or record_type == 3:

                MPS7_dataframe = MPS7_dataframe.append({
                    'record type': record_type,
                    'unix timestamp': time,
                    'user ID': userID
                }, ignore_index=True)
            count = count + 1
        MPS7_dataframe['record type'] = MPS7_dataframe['record type'].replace(
            {0: 'Debit', 1: 'Credit', 2: 'StartAutopay', 3: 'EndAutopay'})
        return MPS7_dataframe


# SCRIPT EXECUTION
filename = "txnlog.dat"
name, version, records = readBatFileHeader(filename)
MPS7_dataframe = extractBatFileData(filename)


# ANSWERS
# 1. What is the total amount in dollars of credits?
print('---- Results ----')
credits_df = MPS7_dataframe[MPS7_dataframe['record type'] == 'Credit']
credit_sum = credits_df['amount USD'].sum()
print('total credit amount=', credit_sum)


# 2. What is the total amount in dollars of debits?
debits_df = MPS7_dataframe[MPS7_dataframe['record type'] == 'Debit']
debits_sum = debits_df['amount USD'].sum()
print('total debit amount=', debits_sum)


# 3. How many autopays were started?
autopay_count_df = MPS7_dataframe[MPS7_dataframe['record type'] == 'StartAutopay']
count = autopay_count_df.shape[0]
print('autopays started=', count)


# 4. How many autopays were ended?
autopay_end_count_df = MPS7_dataframe[MPS7_dataframe['record type'] == 'EndAutopay']
count = autopay_end_count_df.shape[0]
print('autopays ended=', count)


# 5. What is balance of user ID 2456938384156277127?
person = MPS7_dataframe[MPS7_dataframe['user ID'] == 2456938384156277127]
balance = 0
for index, row in person.iterrows():
    if row['record type'] == 'Debit':
        balance = balance - row['amount USD']
    if row['record type'] == 'Credit':
        balance = balance + row['amount USD']

print('balance for user 2456938384156277127=', balance)

print('---- End of Results ----')
