#!/bin/bash

mkdir -p Data
mkdir -p Output

if [ $# -eq 0 ]
  then
    read -p "Enter your username: @" TWITTER_USERNAME

    OSLO_PART1_LIST=$(grep -iEo "^.*?/" src/tz.txt | sort -u)
    echo "Pick timezone"
    select PART1 in $OSLO_PART1_LIST
    do
        break
    done

    OSLO_PART2_LIST=$(grep -iE "^$PART1" src/tz.txt | sort -u)

    select USER_TZ in $OSLO_PART2_LIST
    do
        break
    done
else
    TWITTER_USERNAME=$1
    USER_TZ=$2
fi

echo "Getting stats for user $TWITTER_USERNAME (TZ $USER_TZ)"

source venv/bin/activate
python src/get_user.py $TWITTER_USERNAME
echo "Generating info graphics"
Rscript src/gen_stats.R $TWITTER_USERNAME $USER_TZ
