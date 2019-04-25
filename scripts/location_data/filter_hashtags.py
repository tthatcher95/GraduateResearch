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

df = pd.read_csv('jaccard_hsahtag.csv', names=["Hashtags", "Jaccard"])

def filter_df(x):
    if('_url' in x or '.com' in x or ':' in x):
        return False
    return True

df['Good_Data'] = list(map(lambda x: filter_df(x), df['Hashtags']))
df = df.loc[df['Good_Data'] == True]
df.to_csv("jaccard_hsahtag_final.tsv", sep='\t', columns=["Hashtags", "Jaccard"])
