import pandas as pd
import datetime as dt
import numpy as np
import pymongo

# Import moneyball data
moneyball_csv_path = "Resources/moneyball.csv"
moneyball_df = pd.read_csv(moneyball_csv_path)

# Import and clean master_df
master_csv_path = "Resources/master.csv"
master_df = pd.read_csv(master_csv_path, encoding="ISO-8859-1")
master_df = master_df[['lahmanID', 'playerID', 'birthCountry', 'birthState', 'birthCity',
      'nameFirst', 'nameLast', 'nameNick', 'weight',
      'height', 'bats', 'throws', 'debut', 'finalGame', 'college']]

# Remove rows with NULL playerID
master_df = master_df.loc[master_df['playerID'].isnull()==False]
master_df = master_df.loc[master_df['playerID']!='baezda01']

# Import salary data
salaries_csv_path = "Resources/salaries.csv"
salaries_df = pd.read_csv(salaries_csv_path)
salaries_df.head()

# Import batting data
batting_csv_path = "Resources/Batting.csv"
batting_df = pd.read_csv(batting_csv_path)
batting_df.head()

def mongoize(primary_df,
             secondary_df,
             primary_column_to_merge_on,
             secondary_column_to_merge_on):

    # Create a list to hold dictionaries to insert into the table using insert_many
    to_insert= []
    # Loop through the primary_df to get the data needed
    for i, row_primary in primary_df.iterrows():
        # Create a dictionary to insert the single entry of data from the primary_df
        new_entry = {}

        # loop through each item of row_primary to imput each value into a key value pair
        for j in range(len(row_primary)):
            # get the column name from the primary_df to use as the Key,
            # and get the associated Value using the same method from the current row
            new_entry[primary_df.columns[j]] = row_primary[primary_df.columns[j]]

        # create an entry to hold the list of data from the Secondary Dataframe
        new_entry["Salary Data"] = []

        #use loc to pull the apropreate entries from secondary_df
        found_secondary_df = secondary_df.loc[secondary_df[secondary_column_to_merge_on] == new_entry[primary_column_to_merge_on]]
       # Loop through the secondary_df to get the data needed
        for k, row_secondary in found_secondary_df.iterrows():
            # Create a dictionary to insert the single entry of data from the secondary_df
            new_sub_entry = {}

            # loop through each item of row_secondary to imput each value into a key value pair
            for l in range(len(row_secondary)):
                found = False
                # get the column name from the secondary_df to use as the Key,
                # and get the associated Value using the same method from the current row
                new_sub_entry[secondary_df.columns[l]] = row_secondary[secondary_df.columns[l]]

            # merge only the entries that match the current row's Key to merge on
            if new_sub_entry[secondary_column_to_merge_on] == new_entry[primary_column_to_merge_on]:
                # Delete the data that was used to merge from new_sub_entry as it is
                del new_sub_entry[secondary_column_to_merge_on]
                # add the new_sub_entry to the new_entry
                new_entry["Salary Data"].append(new_sub_entry)

        # add new entry to the list to be inserted
        #print(new_entry)
        to_insert.append(new_entry)
    return to_insert

# Merge data frames with master_df on 'playerID' and create into dictionary using mongoize function
player_data = mongoize(master_df, salaries_df, 'playerID', 'playerID')
batting_data = mongoize(master_df, batting_df, 'playerID', 'playerID')

# Create connection
conn = 'mongodb://localhost:27017'
client = pymongo.MongoClient(conn)
db = client.baseball

# Create player_data collection
collection = db.player_data
collection.insert_many(player_data)

# Create player_data collection
collection = db.batting_data
collection.insert_many(batting_data)

# To verify results of MongoDB database
results = db.batting_data.find()
for result in results:
    print(result)
