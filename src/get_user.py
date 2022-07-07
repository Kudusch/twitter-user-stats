#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys
import os
import csv
import twitter_functions as tw
from datetime import date, datetime, timedelta
import logging
import configparser
config = configparser.RawConfigParser()

# Setup API
config.read("config.ini")
BEARER_TOKEN = config.get("TWITTER_AUTH", "bearer_token")

logging.getLogger("twitter_functions").setLevel(logging.ERROR)

def daterange(start_date, end_date, days=0, hours=0, months=0, years=0):
    if months != 0:
        cur_date = start_date
        while end_date > cur_date:
            if cur_date.month+months > 12:
                y = int((cur_date.month+months)/12)
                m = (cur_date.month+months) % 12
            else:
                y = 0
                m = cur_date.month+months
            next_date = datetime(cur_date.year+y, m, cur_date.day, cur_date.hour, cur_date.minute, cur_date.second)
            if (next_date > end_date):
                yield (cur_date, end_date)
            else:
                yield (cur_date, next_date)
            cur_date = next_date
    elif years != 0:
        cur_date = start_date
        while end_date > cur_date:
            next_date = datetime(cur_date.year+years, cur_date.month, cur_date.day, cur_date.hour, cur_date.minute, cur_date.second)
            if (next_date > end_date):
                yield (cur_date, end_date)
            else:
                yield (cur_date, next_date)
            cur_date = next_date
    elif days != 0 or hours != 0:
        delta = timedelta(hours = hours, days = days)
        cur_date = start_date
        while end_date > cur_date:
            next_date = cur_date + delta
            if (next_date > end_date):
                yield (cur_date, end_date)
            else:
                yield (cur_date, next_date)
            cur_date = next_date

user_name = sys.argv[1]

try:
    file_age = datetime.now().timestamp() - os.path.getmtime(f"Data/{user_name}.csv")
except:
    file_age = 172801

if file_age > (60*60*24*2):
    tweet_source = "API"
    users = tw.lookup_users([user_name], bearer_token=BEARER_TOKEN, verbose=False)
    start_date = datetime.strptime(users[0]["created_at"], "%Y-%m-%dT%H:%M:%S.000Z")

    query = f"from:{user_name}"
    start_date = datetime(start_date.year, start_date.month, 1, 0, 0, 0)
    end_date = datetime.now() - timedelta(hours = 24)

    n_tweets = 0
    for date_range in daterange(start_date, end_date, months=2):
        from_date, to_date = date_range
        tweets = tw.search_tweets(query, bearer_token=BEARER_TOKEN, start_time=f"{from_date:%Y-%m-%dT%H:%M:%SZ}", end_time=f"{to_date:%Y-%m-%dT%H:%M:%SZ}", mode="all", verbose = False)
        if tweets:
            n_tweets += len(tweets)
        tw.tweets_to_csv(tweets, f"Data/{user_name}.csv", verbose = False, append=True)
        print(f"\tRetrieved {n_tweets} tweets", end = "\r")
else:
    tweet_source = "cache"
    with open(f"Data/{user_name}.csv", "r") as f:
        tweets = csv.reader(f)
        n_tweets = len([t for t in tweets])-1

print(f"\tRetrieved {n_tweets} tweets from {tweet_source}")