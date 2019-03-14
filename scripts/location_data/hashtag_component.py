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

def get_hashtag_users_df(df):
        hashtag_users_df = df.loc[(df['Hashtags'] != '[]')]
        return hashtag_users_df

def build_component_dict(df):

    def make_csv(user_hashtag_dict):
        with open('user_hashtag_component.csv', 'w') as csv_file:
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

    L = [1, 2, 3, 4]
    df = get_hashtag_users_df(df)
    df.dropna(inplace=True)
    domain_list = list(df['Hashtags'])
    domain_dict = {}
    for user_domain in domain_list:
        user_domain_list = user_domain.split(',')
        if len(user_domain_list) > 1:
            for pair in itertools.product(user_domain_list, repeat=2):
                stripped_pair = (pair[0].strip("[]()"), pair[1].strip("[]()"))
                if(stripped_pair in domain_dict and pair[0] != 'NA' and pair[1] != 'NA'):
                    domain_dict[stripped_pair] += 1
                else:
                    inverse_pair = (stripped_pair[1], stripped_pair[0])
                    if(inverse_pair not in domain_dict and stripped_pair[0] != stripped_pair[1]):
                        domain_dict[stripped_pair] = 1
    make_csv(domain_dict)
    hash_df = pd.read_csv("user_hashtag_component.csv", names=['Hashtag_Pairs', 'Count'])
    hash_df.dropna(inplace=True)
    return hash_df

native_df = native_vote_csv_df()

# native_df['Hashtags']
x = build_component_dict(native_df)
