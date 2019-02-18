#!/bin/bash

for FILE in /projects/canis/nativevote18/twitter/data/2018_1[0-2]*_stream_1.csv; do
	/projects/canis/scripts/adam_scripts/slurm_composite_clean.sh $FILE
done

for FILE in /projects/canis/indigenous/twitter/data/2018_1[0-2]*_stream_1.csv; do
        /projects/canis/scripts/adam_scripts/slurm_composite_clean.sh $FILE
done
