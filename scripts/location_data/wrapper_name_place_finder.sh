#!/bin/bash

# source /projects/canis/scripts/general/bin/activate

for HOUR in /Users/tdt62/Desktop/test_data/*stream_1*.csv; do
	BASE=$(basename $HOUR)
	FILENAME="${BASE%.*}"
	echo $FILENAME
	python3 name_place_finder.py $HOUR /Users/tdt62/Desktop/GraduateResearch/scripts/location_data/"$FILENAME"_places.csv
done

#for HOUR in /projects/canis/news_deserts/sandbox/2018_11_0[4-6]*edit*.csv; do
 #       BASE=$(basename $HOUR)
  #      FILENAME="${BASE%.*}"
   #     echo $FILENAME
    #    python3 name_place_finder.py $HOUR /projects/canis/scripts/graduate_research/"$FILENAME"_places.csv
#done

#for HOUR in /projects/canis/news_deserts/sandbox/2018_11_0[7-9]*edit*.csv; do
 #       BASE=$(basename $HOUR)
  #      FILENAME="${BASE%.*}"
   #     echo $FILENAME
    #    python3 name_place_finder.py $HOUR /projects/canis/scripts/graduate_research/"$FILENAME"_places.csv
#done
