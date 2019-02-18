import pandas as pd
import glob
import re
import math
import urllib.request
from bs4 import BeautifulSoup
from operator import itemgetter
import csv
import os


keywords = ["Midterm","Vote","Politics","District","Senator","Congress","elect","Representative","Sen","Rep",
            "Republican","Democrat","Dem","Rep","Gov","Debates","GOP","Ballot","Register",
            "Incumbent","Delegate","Potus","Scotus","Supreme court","local news"]

names = ["TweetID", "Timestamp", "Full_Text", "In_Reply_To_User_ID", "User_ID", "User_Name", "User_Screen_Name",
         "Coordinates", "Place", "Bounding_Box", "Quoted_Status_ID", "Retweeted_Status", "Hashtags", "URLs", "User_Mentions",
         "Media", "Language"]

def news_deserts_csv_df(keywords):

    # Last date that is "clean"
    endate = "2018_11_20_21_stream_1_clean.csv"

    filepath = "/projects/canis/news_deserts/twitter/data/2018_10_0[6-9]*clean*"
    #filepath = "/Users/tdt62/Desktop/test_data/2018_11_0[0-5]*clean*"

    list_ = []

    # Takes all of the csv file and makes one big dataframe
    for name in glob.glob(filepath):
        df = pd.read_csv(name,index_col=None, header=0, sep='\t')
        list_.append(df)

    filepath = "/projects/canis/news_deserts/twitter/data/2018_10_[1-3]*clean*"
    #filepath = "/Users/tdt62/Desktop/test_data/2018_11_0[0-5]*clean*"

    # Takes all of the csv file and makes one big dataframe
    for name in glob.glob(filepath):
        df = pd.read_csv(name,index_col=None, header=0, sep='\t')
        list_.append(df)

    filepath = "/projects/canis/news_deserts/twitter/data/2018_11_[0-1][0-9]*clean*"
    #filepath = "/Users/tdt62/Desktop/test_data/2018_11_0[0-5]*clean*"

    # Takes all of the csv file and makes one big dataframe
    for name in glob.glob(filepath):
        df = pd.read_csv(name,index_col=None, header=0, sep='\t')
        list_.append(df)

    # Gets the path from 11/20 Hours 00-19
    filepath = "/projects/canis/news_deserts/twitter/data/2018_11_20_[0-1]*clean*"

    # Takes all of the csv file and makes one big dataframe
    for name in glob.glob(filepath):
        df = pd.read_csv(name,index_col=None, header=0, sep='\t')
        list_.append(df)

    # Gets the path from 11/20 Hours 20-21
    filepath = "/projects/canis/news_deserts/twitter/data/2018_11_20_2[0-1]*clean*"

    # Takes all of the csv file and makes one big dataframe
    for name in glob.glob(filepath):
        df = pd.read_csv(name,index_col=None, header=0, sep='\t')
        list_.append(df)

    # Gets the path from 11/20 Hours 22-29
    filepath = "/projects/canis/news_deserts/twitter/data/2018_11_20_2[2-9]*stream_1*"

    # Takes all of the csv file and makes one big dataframe
    for name in glob.glob(filepath):
        df = pd.read_csv(name,index_col=None, header=0, sep='\t')
        list_.append(df)

    # Gets the path from 11/21 - 11/30
    filepath = "/projects/canis/news_deserts/twitter/data/2018_11_[2-3][1-9]*stream_1*"

    # Takes all of the csv file and makes one big dataframe
    for name in glob.glob(filepath):
        df = pd.read_csv(name,index_col=None, header=0, sep='\t', names=names)
        list_.append(df)

    filepath = "/projects/canis/news_deserts/twitter/data/2018_12*stream_1*"

    # Takes all of the csv file and makes one big dataframe
    for name in glob.glob(filepath):
        df = pd.read_csv(name,index_col=None, header=0, sep='\t', names=names)
        list_.append(df)


    filepath = "/projects/canis/news_deserts/twitter/data/2019_01*stream_1*"

    # Takes all of the csv file and makes one big dataframe
    for name in glob.glob(filepath):
        df = pd.read_csv(name,index_col=None, header=0, sep='\t', names=names)
        list_.append(df)

    filepath = "/projects/canis/news_deserts/twitter/data/2019_02_0[0-6]*stream_1*"

    # Takes all of the csv file and makes one big dataframe
    for name in glob.glob(filepath):
        df = pd.read_csv(name,index_col=None, header=0, sep='\t', names=names)
        list_.append(df)

    # Makes the big df in memory
    frame = pd.concat(list_, axis = 0, ignore_index = True)
    frame.fillna("NA", inplace=True)

    def check_keywords(x):
        for keyword in keywords:
            if(keyword.lower() in x.lower()):
                return True
        return False

        # Make a seperate column which is True if it has the keyword, False if not
    frame['has_keyword'] = list(map(lambda x: check_keywords(x), frame['Full_Text']))

    # Sort based on True/False values from above
    keywords_df = frame.loc[frame['has_keyword'] == True]
    return keywords_df

def total_tweets(df):

    # Gets the total number of tweets
    total_tweets_num = df.shape[0]  # gives number of row count
    return total_tweets_num

def unique_tweets(df):

    def run_isdigit(x):
        if math.isnan(x):
            return False
        else:
            return x.isdigit()

    # Gets the unique tweets i.e. no retweeted status
    df['rt_isdigit'] = list(map(lambda x: x.isdigit(), df['Retweeted_Status']))
    unique_df = df.loc[df['rt_isdigit'] == False]
    unique_tweet_num = unique_df.shape[0]
    return unique_tweet_num

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
            print(full_domain)
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

    def run_isdigit(x):
        if math.isnan(x):
            return False
        else:
            return x.isdigit()

    # Gets the unique tweets i.e. no retweeted status
    df['rt_isdigit'] = list(map(lambda x: x.isdigit(), df['Retweeted_Status']))
    unique_df = df.loc[df['rt_isdigit'] == False]
    unique_tweet_num = total_tweets(unique_df)
    return unique_tweet_num

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

def unique_users(df):
    unique_users = len(df.User_ID.unique())
    return unique_users

def original_content_user(df):

    def run_isdigit(x):
        if math.isnan(x):
            return False
        else:
            return x.isdigit()

    # Gets the unique tweets i.e. no retweeted status
    df['rt_isdigit'] = list(map(lambda x: x.isdigit(), df['Retweeted_Status']))
    unique_df = df.loc[df['rt_isdigit'] == False]
    unique_users = len(unique_df.User_ID.unique())
    return unique_users

def top_news_domains():

    #specify the url
    news_source = "https://www.alexa.com/topsites/category/Top/News"

    #Query the website and return the html to the variable 'page'
    page = urllib.request.urlopen(news_source)

    #Parse the html in the 'page' variable, and find all of the website domains
    soup = BeautifulSoup(page, features="html.parser")
    links = soup.find_all("a")
    link_list = []
    actual_news_list = []
    links_dict = {}
    for link in links:
        link_list.append(link.get("href"))
    i = 0
    for link in link_list:
        if link:
            if "siteinfo/" in link:
                news_link = link.split("/siteinfo/")[1]
                links_dict[news_link] = 0
                actual_news_list.append(news_link)

    return actual_news_list, links_dict

def get_top_news_domains(df, news_list, news_dict, top_val):

    # Function to iterate over the dataframe
    def iterate_domains_dict(x):
        for element in x.split():
            if element in news_list:
                news_dict[x] += 1
        return 0


    top_list = []

    # Iterates over the domain column
    list(map(lambda x: iterate_domains_dict(x), df['actual_domain']))

    # Gets the sorted news_list in descending order
    sorted_news_list = (list(sorted(news_dict.items(), key=itemgetter(1), reverse=True)))

    # Gets the top 20 results of the sorted list
    sorted_news_list = sorted_news_list[0:top_val]

    # Stores all the names in a list, so we can read correctky
    for item in sorted_news_list:
        top_list.append(item[0])

    return top_list, sorted_news_list

def get_top_domains(df, top_val):
    top_list = []
    domain_dict = {}

    def iterate_domains(x):
        domain_list = list(x.split())
        for element in domain_list:
            if element in domain_dict:
                domain_dict[element] += 1
            else:
                domain_dict[element] = 1

    # Create the hashtag dict
    list(map(lambda x: iterate_domains(x), df['actual_domain']))

    # Gets the sorted news_list in descending order
    sorted_domain_list = (list(sorted(domain_dict.items(), key=itemgetter(1), reverse=True)))

    # Gets the top 20 results of the sorted list
    sorted_domain_list = sorted_domain_list[0:top_val]

    # Stores all the names in a list, so we can read correctky
    for item in sorted_domain_list:
        top_list.append(item[0])

    return top_list, sorted_domain_list

def get_number_of_domains(df):
    total_number_domains = len(df.actual_domain)
    return total_number_domains

def count_total_news(news_dict):
    total = sum(list(news_dict.values()))
    return total

def make_domain_csv(top_news_dict):
    with open('news_domains.csv', 'w') as csv_file:
        writer = csv.writer(csv_file)
        for key, value in top_news_dict.items():
            writer.writerow([key, value])

def make_hashtag_csv(hashtag_dict):
    with open('top_hashtags.csv', 'w') as csv_file:
        writer = csv.writer(csv_file)
        for key, value in top_news_dict.items():
            writer.writerow([key, value])

def make_domain_csv(domain_dict):
    with open('top_domains.csv', 'w') as csv_file:
        writer = csv.writer(csv_file)
        for key, value in top_news_dict.items():
            writer.writerow([key, value])

def hashtags(df, top_val):

    top_list = []
    hashtag_dict = {}

    def iterate_hashtags(x):
        hashtag_list = list(x.split(', '))
        for element in hashtag_list:
            if element.strip('[]') in hashtag_dict and x != '[]':
                hashtag_dict[element.strip('[]')] += 1
            else:
                hashtag_dict[element.strip('[]')] = 1

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

def top_user_mentions(df, top_val):

    top_users_list = []
    hashtag_dict = {}

    def iterate_user_mentions(x):

        hashtag_list = list(x.split())
        for element in hashtag_list:
            if element.strip('{}:,').isdigit():
                continue
            elif element.strip('{}:') in hashtag_dict and element != '{}' and not element.strip('{}:').isdigit():
                hashtag_dict[element.strip('{}:')] += 1
            else:
                hashtag_dict[element.strip('{}:')] = 1

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


if __name__== "__main__":

    # Keywords to filter by
    keywords = ["Midterm","Vote","Politics","District","Senator","Congress","elect","Representative","Sen","Rep",
            "Republican","Democrat","Dem","Rep","Gov","Debates","GOP","Ballot","Register",
            "Incumbent","Delegate","Potus","Scotus","Supreme court","local news"]

    # Create big df for manipulating the data
    news_df = news_deserts_csv_df(keywords)

    # Get the count of unique domains and the df associated
    unique_domains_count, domains_df = unique_domains(news_df)

    # Total number of tweets
    count_row = total_tweets(news_df)

    # Unique number of tweets
    unique_tweets_count = unique_tweets(news_df)

    # Unique users based on UserID
    unique_users_count = unique_users(news_df)

    # Original Content creater count
    original_content = original_content_user(news_df)

    # Generates the list and dict associated with the domains
    top_news_list, top_news_dict = top_news_domains()

    # Gets the top 10 news domains
    top_news_list, sorted_top_news_list = get_top_news_domains(domains_df, top_news_list, top_news_dict, 10)

    # Gets top 20 domains
    top_domain_list, sorted_top_news_list = get_top_domains(domains_df, 20)

    # Gets the total number of domains mentioned
    total_domains = get_number_of_domains(domains_df)

    # Count total number of news domains
    total_news_domains = count_total_news(top_news_dict)

    # Generate a CSV for the domain and it's occurence
    make_domain_csv(top_news_dict)

    # Gets the list for top 20 hashtags
    top_hashtags, hashtag_sorted_list = hashtags(news_df, 20)

    # Get the top 20 user mentions
    top_users_list, sorted_news_list = top_user_mentions(news_df, 20)

    # Prints each value
    print("Total Number of Tweets: {}".format(count_row))
    print("Unique Tweets: {}".format(unique_tweets_count))
    print("Unique Domains: {}".format(unique_domains_count))
    print("Unique Users: {}".format(unique_users_count))
    print("Original Content Users: {}".format(original_content))
    print("Total Number of Domains: {}".format(total_domains))
    print("Total Number of News Domains: {}\n".format(total_news_domains))
    print("Top 10 News Domains: {}\n".format(top_news_list))
    print("Top 20 Hashtags: {}\n".format(top_hashtags))
    print("Top 20 Domains: {}\n".format(top_domain_list))
    print("Top 20 User Mentions: {}\n".format(top_users_list))
