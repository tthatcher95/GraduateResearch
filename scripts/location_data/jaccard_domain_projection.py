import pandas as pd
import glob
import re
import math
import urllib.request
from operator import itemgetter
import networkx as nx
import csv
import os
import matplotlib.pyplot as plt
import itertools
from scipy.spatial import distance
from math import *


def native_vote_csv_df():
    # filepath = "/Users/tdt62/Desktop/GraduateResearch/test_data/2018_10_17_09_vote*"
    # filepath = "/Users/tdt62/Desktop/test_data/2018_10_*vote*"
    list_ = []

#     filepath = "/projects/canis/nativevote18/twitter/data/2018_10*stream_1*"
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

def unique_domains(df):

    # Checks if URLs is empty or not
    def empty_list_check(x):
        if(x != '[]'):
            return True
        else:
            return False

    def get_domains(full_domain):
        if(isinstance(full_domain, str)):
            m = full_domain.split("//")[-1].split("/")[0].split('?')[0]
            return m

        else:
            return full_domain


    # Gets the tweets that have URLs in them i.e. URLs column is not '[]'
    df['domains'] = list(map(lambda x: empty_list_check(x), df['URLs']))

    # Gets the domains to be only the domains i.e. www.xyz.com
    df['actual_domain'] = list(map(lambda x: get_domains(x), df['URLs']))

    # Creates a df that only has domains in the tweet
    unique_df = df.loc[df['domains'] == True]

    # Get the number of domains
    unique_domains = len(unique_df.actual_domain.unique())

    domains_df = unique_df

    return unique_domains, domains_df

def total_tweets(df):

    # Gets the total number of tweets
    total_tweets_num = df.shape[0]  # gives number of row count
    return total_tweets_num

def unique_tweets(df):

    # Gets the unique tweets i.e. no retweeted status
    df['rt_isdigit'] = list(map(lambda x: str(x).isdigit(), df['Reteeted_Status']))
    unique_df = df.loc[df['rt_isdigit'] == False]
    return unique_df

def unique_users(df):
    unique_users = len(df.User_ID.unique())
    return unique_users

def original_content_user(df):

    # Gets the unique tweets i.e. no retweeted status
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
        hashtag_users_df = df.loc[(df['User_Mentions'] != '[]') & (df['Hashtags'] != '[]')]
        return hashtag_users_df

def build_mentions_dict(df):

    def make_csv(user_hashtag_dict):
        with open('user_hashtag.csv', 'w') as csv_file:
            writer = csv.writer(csv_file)
            for key, value in user_hashtag_dict.items():
                writer.writerow([key, value])

    def make_lists(df):
        user_list = list(df['User_Mentions'].split("'screen_name':"))
        hashtag_list = list(df['Hashtags'].split("'text':"))



    user_list = list(df['User_Mentions'].split("'screen_name':"))
    hashtag_list = list(df['Hashtags'].split("'text':"))

    for user in user_list:
        stripped_user = user.split(',')[0].strip("' '{}[]")
        if stripped_user != '[]' and stripped_user != 'NA' and stripped_user != '':
            for hashtag in hashtag_list:
                stripped_hashtag = hashtag.split(',')[0].strip("' '{}[]")
                if stripped_hashtag != '[]' and stripped_hashtag != 'NA' and stripped_hashtag != '':
                    dict_tuple = (stripped_user, stripped_hashtag)
                    if(dict_tuple in user_hashtag_dict):
                        user_hashtag_dict[dict_tuple] += 1
                    else:
                        user_hashtag_dict[dict_tuple] = 1
    make_csv(user_hashtag_dict)
    return user_hashtag_dict

def make_user_domain_dict(df):

    unique, domain_df = unique_domains(df)

    def make_csv(user_hashtag_dict):
        with open('user_domain.csv', 'w') as csv_file:
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

    df.dropna(inplace=True)

    domain_list = list(zip(domain_df['actual_domain'], domain_df['User_ID']))[1:]
    domain_dict = {}
    user_list = []

    for user_domain in domain_list:
        user_domain_list = str(user_domain[0]).split(',')
        if len(user_domain_list) == 1:
            stripped_domain = str(user_domain_list[0]).strip("()[] ")
            if(stripped_domain in domain_dict and user_domain[1] != 'NA' and stripped_domain != 'NA'):
                tmp_list = domain_dict[stripped_domain]
                tmp_list.append(str(user_domain[1]).strip("()[] "))
                domain_dict[stripped_domain] = tmp_list
            else:
                domain_dict[user_domain[0]] = [str(user_domain[1]).strip("()[] ")]
        else:
            for domain in user_domain_list:
                stripped_domain = domain.strip("()[] ")
                if(stripped_domain in domain_dict and user_domain[1] != 'NA' and stripped_domain != 'NA'):
                    tmp_list = domain_dict[stripped_domain]
                    tmp_list.append(str(stripped_domain))
                    domain_dict[stripped_domain] = tmp_list
                else:
                    domain_dict[user_domain[0]] = [str(stripped_domain)]


#     for user_domain in domain_list:
#         if(user_domain[1] in domain_dict):
#             tmp_list = domain_dict[user_domain[1]]
#             tmp_list.append(user_domain[0])
#             domain_dict[user_domain[1]] = tmp_list
#         else:
#             domain_dict[user_domain[1]] = [user_domain[0]]

    make_csv(domain_dict)
    domain_df = pd.read_csv("user_domain.csv", names=['User_Domain', 'Count'])
    domain_df.dropna(inplace=True)

    jaccard_domain_dict = {}

    for pair in itertools.product(domain_df['User_Domain'], repeat=2):
        jaccard_domain_dict[pair] = jaccard_similarity(*pair)

    make_jaccard_csv(jaccard_domain_dict)
    return jaccard_domain_dict


native_df = native_vote_csv_df()

x = make_user_domain_dict(native_df)
