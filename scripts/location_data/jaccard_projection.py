import pandas as pd
import glob
import re
import math
import urllib.request
# from bs4 import BeautifulSoup
from operator import itemgetter
import networkx as nx
import csv
import os
import matplotlib.pyplot as plt
import itertools
from scipy.spatial import distance
from math import *
import numpy as np


def native_vote_csv_df():
#     filepath = "/Users/tdt62/Desktop/test_data/2018_10_*vote*"
    list_ = []

    filepath = "/projects/canis/nativevote18/twitter/data/2018_10*clean*"
    # filepath = "/Users/tdt62/Desktop/test_data/2018_10_*vote*"


    # Takes all of the csv file and makes one big dataframe
    for name in glob.glob(filepath):
        if(os.stat(name).st_size == 0) == True:
            continue
        else:
            df = pd.read_csv(name, index_col=None, sep="\t")
            list_.append(df)
    #
    filepath = "/projects/canis/nativevote18/twitter/data/2018_11*clean*"
    #     filepath = "/Users/tdt62/Desktop/GraduateResearch/test_data/2018_11_*vote*"

    # Takes all of the csv file and makes one big dataframe
    for name in glob.glob(filepath):
        if(os.stat(name).st_size == 0) == True:
            continue
        else:
            df = pd.read_csv(name,index_col=None, sep='\t')
            list_.append(df)

    filepath = "/projects/canis/nativevote18/twitter/data/2018_12*clean*"
    #     filepath = "/Users/tdt62/Desktop/test_data/2018_12_01*vote*"

    # Takes all of the csv file and makes one big dataframe
    for name in glob.glob(filepath):
        if(os.stat(name).st_size == 0) == True:
            continue
        else:
            df = pd.read_csv(name,index_col=None, sep='\t')
            list_.append(df)

    filepath = "/projects/canis/nativevote18/twitter/data/2019_01*clean*"
    # filepath = "/Users/tdt62/Desktop/test_data/2018_10_*vote*"

    # Takes all of the csv file and makes one big dataframe
    for name in glob.glob(filepath):
        if(os.stat(name).st_size == 0) == True:
            continue
        else:
            df = pd.read_csv(name,index_col=None, sep='\t')
            list_.append(df)


    # Makes the big df in memory
    frame = pd.concat(list_, axis = 0, ignore_index = True)
    frame.fillna("NA", inplace=True)
    return frame

def total_tweets(df):

    # Gets the total number of tweets
    total_tweets_num = df.shape[0]  # gives number of row count
    return total_tweets_num

def unique_tweets(df):

    # Gets the unique tweets i.e. no Reteeted status
    df['rt_isdigit'] = list(map(lambda x: str(x).isdigit(), df['Reteeted_Status']))
    unique_df = df.loc[df['rt_isdigit'] == False]
    return unique_df

def unique_users(df):
    unique_users = len(df.User_ID.unique())
    return unique_users

def original_content_user(df):

    # Gets the unique tweets i.e. no Reteeted status
    df['rt_isdigit'] = list(map(lambda x: str(x).isdigit(), df['Reteeted_Status']))
    unique_df = df.loc[df['rt_isdigit'] == False]
    unique_users = len(unique_df.User_ID.unique())
    return unique_users

def hashtags(df, top_val):

    top_list = []
    hashtag_dict = {}

    def iterate_hashtags(x):
        hashtag_list = list(x.split("'text':"))
        for element in hashtag_list:
            stripped_element = element.split(',')[0].strip("' '{}[]")
            if stripped_element in hashtag_dict and stripped_element != '[]' and stripped_element != 'NA' and stripped_element != '':
                hashtag_dict[stripped_element] += 1
            else:
                hashtag_dict[stripped_element] = 1

    # Create the hashtag dict
    list(map(lambda x: iterate_hashtags(x), df['Hashtags']))

    # Gets the sorted news_list in descending order
    sorted_news_list = (list(sorted(hashtag_dict.items(), key=itemgetter(1), reverse=True)))

    # Gets the top 20 results of the sorted list
    sorted_news_list = sorted_news_list[0:top_val]

    # Stores all the names in a list, so we can read correctky
    for item in sorted_news_list:
        top_list.append(item[0])

    return top_list, sorted_news_list

def make_domain_csv(top_news_dict):
    with open('news_domains.csv', 'w') as csv_file:
        writer = csv.writer(csv_file)
        for key, value in top_news_dict.items():
            writer.writerow([key, value])

def top_user_mentions(df, top_val):

    top_users_list = []
    hashtag_dict = {}

    def iterate_user_mentions(x):

        hashtag_list = list(x.split("'screen_name':"))
        for element in hashtag_list:
            stripped_element = element.split(',')[0].strip("' '{}[]")
            if stripped_element in hashtag_dict and stripped_element != '[]' and stripped_element != 'NA' and stripped_element != '':
                hashtag_dict[stripped_element] += 1
            else:
                hashtag_dict[stripped_element] = 1

    # Create the hashtag dict
    list(map(lambda x: iterate_user_mentions(x), df['User_Mentions']))

    # Gets the sorted news_list in descending order
    sorted_news_list = (list(sorted(hashtag_dict.items(), key=itemgetter(1), reverse=True)))

    # Gets the top 20 results of the sorted list
    sorted_news_list = sorted_news_list[0:top_val]

    # Stores all the names in a list, so we can read correctky
    for item in sorted_news_list:
        top_users_list.append(item[0])

    return top_users_list, sorted_news_list

def get_hashtag_users_df(df):
        hashtag_users_df = df.loc[(df['Hashtags'] != '[]')]
        return hashtag_users_df

def build_mentions_dict(df):

    def make_csv(user_hashtag_dict):
        with open('user_hashtag.csv', 'w') as csv_file:
            writer = csv.writer(csv_file)
            for key, value in user_hashtag_dict.items():
                writer.writerow([key, value])

    def make_jaccard_csv(user_hashtag_dict):
        with open('jaccard_domain.csv', 'w') as csv_file:
            writer = csv.writer(csv_file)
            for key, value in user_hashtag_dict.items():
                writer.writerow([key, value])

    def jaccard_similarity(x,y):
        intersection_cardinality = len(set.intersection(*[set(x), set(y)]))
        union_cardinality = len(set.union(*[set(x), set(y)]))
        return intersection_cardinality/float(union_cardinality)

    df = get_hashtag_users_df(df)
    df.dropna(inplace=True)
    domain_list = list(zip(df['Hashtags'], df['User_ID']))

    domain_dict = {}

    for user_domain in domain_list:
        user_domain_list = str(user_domain[0]).split(',')
#         print(user_domain)
        if len(user_domain_list) == 1:
            stripped_domain = str(user_domain_list[0]).strip("()[] ").lower()
            if(stripped_domain in domain_dict and user_domain[1] != 'na' and stripped_domain != 'na'):
                tmp_list = domain_dict[stripped_domain]
                tmp_list.append(str(user_domain[1]).strip("()[] "))
                domain_dict[stripped_domain] = tmp_list
            else:
                domain_dict[stripped_domain] = [str(user_domain[1]).strip("()[] ")]
        else:
            for domain in user_domain_list:
                stripped_domain = str(domain).strip("()[] ").lower()
                if(stripped_domain in domain_dict and user_domain[1] != 'na' and stripped_domain != 'na'):
                    tmp_list = domain_dict[stripped_domain]
                    tmp_list.append(user_domain[1])
                    domain_dict[stripped_domain] = tmp_list
                else:
                    domain_dict[stripped_domain] = [user_domain[1]]

    make_csv(domain_dict)
    hash_df = pd.read_csv("user_hashtag.csv", names=['User_Hashtags', 'Count'])
    hash_df.dropna(inplace=True)

    jaccard_domain_dict = {}

    for pair in itertools.product(hash_df['User_Hashtags'][2:], repeat=2):
        jaccard_domain_dict[pair] = jaccard_similarity(*pair)

    make_jaccard_csv(jaccard_domain_dict)

    return hash_df

native_df = native_vote_csv_df()

# native_df['Hashtags']
build_mentions_dict(native_df)
