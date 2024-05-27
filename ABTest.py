# %%
import re
import pandas as pd
import numpy as np
import streamlit as st
from datetime import datetime
import pymongo
import json
import pymysql
from dotenv import load_dotenv
import os
# %%
load_dotenv('.env')
st.title(" LIVE")
# %%
host = os.getenv("URI")
# %%

client = pymongo.MongoClient(host)
print(client.list_database_names())


participant = client['separatr']['participant']
# ok 1

# %%
# For Unique Test Names
# Define your MongoDB pipeline
pipeline = [
    {"$group": {
        "_id": "$testName",  # Group by testName
        # Find the minimum createdAt date for each group
        "firstCreatedAt": {"$min": "$createdAt"}
    }},
    {"$project": {
        "_id": 0,  # Exclude the MongoDB-generated _id field
        "testName": "$_id",  # Rename _id as testName
        "firstCreatedAt": 1  # Include the firstCreatedAt field
    }}
]

try:
    # Execute the pipeline
    result = list(participant.aggregate(pipeline))

    # Reshape the result into a DataFrame
    alt = pd.DataFrame([
        {
            'testName': item['testName'],
            'firstCreatedAt': item['firstCreatedAt'],  # Keep the original date
            'year': item['firstCreatedAt'].year,  # Extract year
            'month': item['firstCreatedAt'].month,  # Extract month
        }
        for item in result
    ])

    # Sort the DataFrame by year and month
    alt.sort_values(by=['year', 'month'], ascending=False, inplace=True)

except Exception as e:
    print(e)


allTest = alt[(alt['month'].isin([3, 4, 5])) & (alt['year'] == 2024)]

uniquetest = allTest['testName'].unique().tolist()


# # For Uniqutest Clist
# # change the date according to the need
# query = {"createdAt": {"$gt": datetime(2024, 2, 1)}}


# uniquetest = pd.DataFrame(list(participant.find(query)))


# uniquetest = uniquetest['testName'].unique().tolist()


try:
    query = {

        "testName": {"$in": uniquetest}
    }

    pipeline = [
        {"$match": query},
        {"$group": {"_id": {"testName": "$testName", "participatingSection": "$participatingSection"},
                    "startDate": {"$min": "$createdAt"},
                    "LastDate": {"$max": "$createdAt"},
                    "accountCount": {"$sum": 1}}}
    ]

    result = list(participant.aggregate(pipeline))

    # Reshape the result into a DataFrame
    allTestABParticipant = pd.DataFrame([{
        'testName': item['_id']['testName'],
        'startDate': item['startDate'].strftime("%Y-%m-%d"),
        'LastDate': item['LastDate'].strftime("%Y-%m-%d"),
        'participantSection': item['_id']['participatingSection'],
        'accountCount': item['accountCount']
    } for item in result])
except Exception as e:
    print(e)

allTestABParticipant = allTestABParticipant.sort_values(by='testName')


resultData = client['separatr']['resultData']

# Now Same for Results
try:
    query = {

        "testName": {"$in": uniquetest}
    }

    pipeline = [
        {"$match": query},
        {"$group": {"_id": {"testName": "$testName", "selectedData": "$selectedData"},

                    "accountCount": {"$sum": 1}}}
    ]

    result = list(resultData.aggregate(pipeline))

    # Reshape the result into a DataFrame
    allTestABResult = pd.DataFrame([{
        'testName': item['_id']['testName'],

        'selectedData': item['_id']['selectedData'],
        'accountCount': item['accountCount']
    } for item in result])
except Exception as e:
    print(e)
allTestABResult = allTestABResult.sort_values(by='testName')


allTestABResult = allTestABResult.rename(
    columns={'selectedData': 'participantSection', 'accountCount': 'ResultCounts'})


# For AB test Result with percentage
all = allTestABParticipant.merge(allTestABResult, how='left', on=[
                                 'testName', 'participantSection'])
all['Parcentage%'] = ((all['ResultCounts']/all['accountCount'])*100).round(2)


all[['startDate', 'LastDate']] = all[['startDate', 'LastDate']].apply(
    pd.to_datetime, errors='coerce')

# Create a new 'Start month' column
all['StartMonth'] = all['startDate'].dt.month


# For This year
all = all[all['StartMonth'].isin([2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12])]


all['TotalDays'] = (all['LastDate'] - all['startDate']).dt.days
all = all[['testName', 'startDate', 'LastDate', 'StartMonth',
           'TotalDays', 'participantSection',
           'accountCount', 'ResultCounts', 'Parcentage%', ]]


all = all.sort_values(by=['StartMonth', 'testName'],
                      ascending=True).reset_index(drop=True)

all['Status'] = np.where(all['LastDate'].dt.date ==
                         pd.to_datetime('today').date(), 'Running', 'Off')


def add_spaces(input_string):
    # Insert spaces before capital letters (except for the first letter)
    spaced_string = re.sub(r'(?<!^)(?=[A-Z])', ' ', input_string)
    return spaced_string


# Assuming 'all' is your DataFrame and 'addToCartFromCartPageABTest' is the column to transform
all['test_NameSpaced'] = all['testName'].apply(add_spaces)


all['ABTest'] = all['participantSection'].apply(add_spaces)

pagenames = ['cart', 'home', 'browse',
             'shipping', 'details', 'Detail', 'payment']


def pageName(inputString):
    for page in pagenames:
        if page in inputString.lower():  # Using lower() to make it case-insensitive
            return page
    return 'noName'


all['PageName'] = all['testName'].apply(pageName)


all['testLink'] = "http://49.0.201.81:5173/test/"+all['testName']

all = all[['StartMonth', 'startDate', 'LastDate',  'TotalDays', 'testLink', 'testName', 'test_NameSpaced',
           'ABTest', 'accountCount', 'ResultCounts', 'Parcentage%',
           'Status', ]]


all = all.sort_values(by=['StartMonth', 'startDate'],
                      ascending=True).reset_index(drop=True)

all['imageName'] = all['testName'].str.slice(0, 5)


all['A/B'] = all['ABTest'].str.slice(-1)
all['ABImageName'] = all['imageName']+all['A/B']+'.png'
all = all[['StartMonth', 'startDate', 'LastDate', 'TotalDays', 'testLink',
           'testName', 'test_NameSpaced', 'ABTest', 'accountCount', 'ResultCounts',
           'Parcentage%', 'Status', 'ABImageName']]
# %%
wrong = ['id_32_4_4_CartPageOrderAsAGiftButtonColorVioletVSBlueABTest',
         'id_33_2_5_BrowsePageShowOnPageDynamicCategoryDiscountABTest']
all = all[~all['testName'].isin(wrong)]
# %%
# all.to_excel('test.xlsx')
st.dataframe(all)
# %%


# %%
