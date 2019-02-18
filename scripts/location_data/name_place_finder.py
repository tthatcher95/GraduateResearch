### LOCATION DATA SCRIPTS ###
import os
import sys
import csv
import argparse
import pandas as pd


# https://simplemaps.com/data/us-cities

# twitter.com/anyuser/status/541278904204668929

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

def make_domain_csv(domain_dict):
    with open('top_domains.csv', 'w') as csv_file:
        writer = csv.writer(csv_file)
        for key, value in top_news_dict.items():
            writer.writerow([key, value])

def city_master_list():
    with open('/Users/tdt62/Desktop/GraduateResearch/scripts/location_data/uscitiesv1.4.csv', 'r') as city_list:
        indicies = csv.reader(city_list)
        city_list_indicies = list(indicies)

        for k in range(len(city_list_indicies)):
            abbr_str = city_list_indicies[k][0] + ', ' + city_list_indicies[k][2]
            full_str = city_list_indicies[k][0] + ', ' + city_list_indicies[k][3]
            city_state_abbr.append(abbr_str)
            city_state_full.append(full_str)

    return city_list_indicies


def find_city_state(TSV_file, TSV_output_file, city_list_indicies):

    check_list = pd.read_csv(TSV_file,index_col=None, header=0, delimiter='\t')

    with open(TSV_output_file, 'w') as output:
#         reader = csv.reader(temp, delimiter="\t")
        writer = csv.writer(output, delimiter="\t")


#             check_list = list(reader)

        writer.writerow(['Tweet ID', 'Name Place', 'City ID'])

        for j in range(len(city_state_abbr)):
            if city_state_abbr[j] in check_list['Full_Text'] or city_state_full[j] in check_list['Full_Text']:
                writer.writerow([check_list['TweetID'], city_state_full[j], city_list_indicies[j][15]])
                # print("Found", city_state_full[j])

        for m in range(len(hashtag_state_full)):
            if hashtag_state_abbr[m] in check_list['Hashtags']:
                writer.writerow([check_list['TweetID'], hashtag_state_abbr[m]])
                # print("Found abbr hashtag", hashtag_state_abbr[m])
            if hashtag_state_full[m] in check_list['Hashtags']:
                writer.writerow([check_list['TweetID'], hashtag_state_full[m]])
                # print("Found full hashtag", hashtag_state_full[m])

# master_list = city_master_list()
# find_city_state("/Users/tdt62/Desktop/test_data/2018_11_05_06_stream_1_clean.csv", "/Users/tdt62/Desktop/GraduateResearch/scripts/location_data/test.csv", master_list)



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
