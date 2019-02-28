### LOCATION DATA SCRIPTS ###
import os
import sys
import csv
import argparse
import pandas as pd
import time

def city_master_list():

    # df = pd.read_csv('/Users/tdt62/Desktop/GraduateResearch/scripts/location_data/uscitiesv1.4.csv', index_col=None, header=0)
    df = pd.read_csv('/projects/canis/scripts/graduate_research/location_data/uscitiesv1.4.csv', index_col=None, header=0)

    return df


def find_city_state(TSV_file, TSV_output_file, df):

    city_state_abbr = set(zip(df['lat'], zip(df['lng'], zip(df['state_id'], zip(df['city'], df['state_name'])))))

    twitter_df = pd.read_csv(TSV_file, index_col=None, header=0, delimiter='\t')

    location_df = pd.DataFrame(columns=['Tweet ID', 'Name Place', 'City ID'])

    def location_csv(twitter_df):
        places = {}
        for city in city_state_abbr:
            place = str(', '.join(city[1][1][1]))
            if str(city[1][1][0]) in twitter_df['Full_Text'] or place in twitter_df['Full_Text']:
                places[place] = [(city[0], city[1][0]), city[1][1][0]]
        if(places):
            return True, places
        else:
            return False, places

    start = time.time()
    twitter_df['Has_Location'], twitter_df['Places'] = list(zip(*twitter_df.apply(location_csv, axis=1)))
    end = time.time()
    print("Time: {}".format(end - start))
    twitter_df.to_csv(TSV_output_file, sep='\t')
    return 0


# master_list = city_master_list()
# # master_list
# frame = find_city_state("/Users/tdt62/Desktop/test_data/clean_data/2018_10_06_09_vote_stream_1_clean.csv", "/Users/tdt62/Desktop/GraduateResearch/scripts/location_data/test.csv", master_list)
# keywords_df = frame.loc[frame['Has_Location'] == True]
# keywords_df


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

    parser.add_argument('TSV_output_file', type=str, help='Output file with found city states.')

    args = parser.parse_args()

    return args

def main():
    args = parse_args()

    city_list_indicies = city_master_list()

    find_city_state(args.TSV_file, args.TSV_output_file, city_list_indicies)

if __name__ == "__main__":
    main()
