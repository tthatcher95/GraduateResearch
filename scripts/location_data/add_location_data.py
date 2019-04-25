### LOCATION DATA SCRIPTS ###
import os
import sys
import csv
import argparse
import pandas as pd
import time


hashtag_state_full =  ['\'Alabama\'', '\'Alaska\'', '\'Arizona\'', '\'Arkansas\'', \
'\'California\'', '\'Colorado\'', '\'Connecticut\'', '\'Delaware\'', '\'Florida\'', \
'\'Georgia\'', '\'Hawaii\'', '\'Idaho\'', '\'Illinois\'' , '\'Indiana\'' , '\'Iowa' , \
'\'Kansas\'' , '\'Kentucky\'' , '\'Louisiana\'' , '\'Maine\'' , '\'Maryland\'' , \
'\'Massachusetts\'' , '\'Michigan\'' , '\'Minnesota\'' , '\'Mississippi\'' , \
'\'Missouri\'' , '\'Montana\'' '\'Nebraska\'' , '\'Nevada' , '\'NewHampshire\'' , \
'\'NewJersey\'' , '\'NewMexico\'' , '\'NewYork\'' , '\'NorthCarolina\'' , \
'\'NorthDakota\'' , '\'Ohio' , '\'Oklahoma\'' , '\'Oregon\'' , '\'Pennsylvania\'', \
'\'RhodeIsland\'', '\'SouthCarolina\'' , '\'SouthDakota\'' , '\'Tennessee\'' , \
'\'Texas\'' , '\'Utah\'' , '\'Vermont\'' , '\'Virginia\'' , '\'Washington\'' , \
'\'WestVirginia\'' , '\'Wisconsin\'', '\'Wyoming\'']

hashtag_state_abbr = ['\'AL\'', '\'AK\'', '\'AZ\'', '\'AR\'', '\'CA\'', '\'CO\'', \
    '\'CT\'', '\'DE\'', '\'FL\'', '\'GA\'', '\'HI\'', '\'ID\'', '\'IL\'', '\'IN\'', \
    '\'IA\'', '\'KS\'', '\'KY\'', '\'LA\'', '\'ME\'', '\'MD\'', '\'MA\'', '\'MI\'', \
    '\'MN\'', '\'MS\'', '\'MO\'', '\'MT\'', '\'NE\'', '\'NV\'', '\'NH\'', '\'NJ\'', \
    '\'NM\'', '\'NY\'', '\'NC\'', '\'ND\'', '\'OH\'', '\'OK\'', '\'OR\'', '\'PA\'', \
    '\'RI\'', '\'SC\'', '\'SD\'', '\'TN\'', '\'TX\'', '\'UT\'', '\'VT\'', '\'VA\'', \
    '\'WA\'', '\'WV\'', '\'WI\'', '\'WY\'']



city_state_abbr = []
city_state_full = []
city_list_indicies = []

city_state_abbr_list = {}


location_list = list(zip(hashtag_state_full, hashtag_state_abbr))

for pair in location_list:
    city_state_abbr_list[pair[1]] = pair[1]

def city_master_list():

    # df = pd.read_csv('/Users/tdt62/Desktop/GraduateResearch/scripts/location_data/uscitiesv1.4.csv', index_col=None, header=0)
    df = pd.read_csv('/projects/canis/scripts/graduate_research/location_data/uscitiesv1.4.csv', index_col=None, header=0)

    return df


def find_city_state(TSV_file, TSV_output_dir, df):

    city_state_dict = {}
    lat_lon_dict = {}

    lat_lon_list = zip(df['city'], zip(df['lat'], df['lng']))
    city_state = list(zip(df['city'], df['state_name']))

    for pair in lat_lon_list:
        lat_lon_dict[pair[0].upper()] = pair[1]

    for pair in city_state:
        if pair[1].upper() in city_state_dict:
            city_state_dict[pair[1].upper()].append((pair[0].upper()))
        else:
            city_state_dict[pair[1].upper()] = [(pair[0].upper())]


    def get_location(df):
        locations = []

        cur_tweet = str(df['Full_Text']).upper().split(' ')
        for word in cur_tweet:
            for state, city in city_state_dict.items():
                try:
                    if('http' not in word and 'https' not in word):
                        if word[0] == '#':
                            if word[1:2] in hashtag_state_abbr:
                                locations.append(city_state_abbr_list[word[1:2]])

                            if word[1:] in city:
                                for city_words in city:
                                    if word[1:] in city_words:
                                        locations.append(((city_words, state), (lat_lon_dict[city_words])))

                        else:
                            if word in city:
                                for city_words in city:
                                    if word in city_words:
                                        locations.append(((city_words, state), (lat_lon_dict[city_words])))
                            if word == state:
                                locations.append(state)
                except:
                    continue

        return locations

    try:
        if(os.stat(name).st_size == 0) == False:
            twitter_df = pd.read_csv(TSV_file, index_col=None, delimiter='\t')
            twitter_df['Locations'] = twitter_df.apply(get_location, axis=1)
            if('Places' in list(twitter_df)):
                twitter_df = twitter_df.drop(['Has_Location', 'Places'], axis=1)
            twitter_df.to_csv(TSV_output_dir + TSV_file.split('/')[-1].split('.')[0] + '_location.csv', sep='\t')
        else:
            print('Empty File: {}'.format(TSV_file))
            return twitter_df

    except TypeError:
        print('TypeError')
        print("Filename: {}".format(TSV_file))

    except KeyError:
        twitter_df = pd.read_csv(TSV_file, index_col=None, delimiter='\t', header=None, names=["Unnamed: 0", "TweetID", "Timestamp", "Full_Text", "In_Reply_To_User_ID", "User_ID", "User_Name", "User_Screen_Name", "Coordinates", "Place", "Bounding_Box", "Quoted_Status_ID", "Retweeted_Status", "Hashtags", "URLs", "User_Mentions", "Media,Language"])
        twitter_df['Locations'] = twitter_df.apply(get_location, axis=1)
        if('Places' in list(twitter_df)):
            twitter_df = twitter_df.drop(['Has_Location', 'Places'], axis=1)
        twitter_df.to_csv(TSV_output_dir + TSV_file.split('/')[-1].split('.')[0] + '_location.csv', sep='\t')
    return twitter_df





def parse_args():
    '''
    Parses the command line arguments for the script

    Returns
    -------

    args : object
           Python arg parser object
    '''
    parser = argparse.ArgumentParser(description='Process some integers.')

    parser.add_argument('TSV_file', type=str, help='Twitter stream TSV file to find city states.')

    parser.add_argument('TSV_output_dir', type=str, help='Directory to put location file in.')

    args = parser.parse_args()

    return args

def main():
    args = parse_args()

    city_list_indicies = city_master_list()

    find_city_state(args.TSV_file, args.TSV_output_dir, city_list_indicies)

if __name__ == "__main__":
    main()
