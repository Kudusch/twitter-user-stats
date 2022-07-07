#!/usr/bin/env python
# -*- coding: utf-8 -*-

import requests
from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter
import datetime
import math
import csv
import json
import traceback
import time
import os
import logging

logger = logging.getLogger(__name__)
handler = logging.StreamHandler()
formatter = logging.Formatter(
        '[%(levelname)s] | %(asctime)s | "%(message)s" | %(filename)s')
handler.setFormatter(formatter)
logger.addHandler(handler)
logger.setLevel(logging.WARNING)

request_session = requests.Session()
retry_strategy = Retry(
    total=10,
    backoff_factor=5,
    status_forcelist=[500, 502, 503, 504]
)
adapter = HTTPAdapter(max_retries=retry_strategy)
request_session.mount("https://", adapter)

user_dict = {}
included_tweets_dict = {}
places_dict = {}
queried_at = int(datetime.datetime.now().timestamp())

def get_datetime_range(tweets):
    values = [t["created_at"] for t in tweets]
    return(f"created_at from {min(values)} to {max(values)}")

def parse_tweet(raw_tweet):
    parsed_tweet = {
        "status_id":"",
        "created_at":"",
        "text":"",
        "conversation_id":"",
        "hashtags":"",
        "mentions":"",
        "url_location":"",
        "url_unwound":"",
        "url_title":"",
        "url_description":"",
        "url_sensitive":"",
        "geo":"",
        "lang":"",
        "source":"",
        "reply_settings":"",
        "retweet_count":"",
        "reply_count":"",
        "like_count":"",
        "quote_count":"",
        "is_retweet":"",
        "is_reply":"",
        "is_quote":"",
        
        "retweeted_user_id":"",
        "retweeted_user_screen_name":"",
        "retweeted_user_name":"",
        "retweeted_user_followers_count":"",
        "retweeted_user_following_count":"",
        "retweeted_user_tweet_count":"",
        "retweeted_user_listed_count":"",
        "retweeted_user_protected":"",
        "retweeted_user_verified":"",
        "retweeted_user_description":"",
        "retweeted_tweet_status_id":"",
        "retweeted_tweet_conversation_id":"",
        "retweeted_tweet_created_at":"",
        "retweeted_tweet_lang":"",
        "retweeted_tweet_source":"",
        "retweeted_tweet_text":"",
        "retweeted_tweet_retweet_count": "",
        "retweeted_tweet_reply_count": "",
        "retweeted_tweet_like_count": "",
        "retweeted_tweet_quote_count": "",

        "replied_user_id":"",
        "replied_user_screen_name":"",
        "replied_user_name":"",
        "replied_user_followers_count":"",
        "replied_user_following_count":"",
        "replied_user_tweet_count":"",
        "replied_user_listed_count":"",
        "replied_user_protected":"",
        "replied_user_verified":"",
        "replied_user_description":"",
        "replied_tweet_status_id":"",
        "replied_tweet_conversation_id":"",
        "replied_tweet_created_at":"",
        "replied_tweet_lang":"",
        "replied_tweet_source":"",
        "replied_tweet_text":"",
        "replied_tweet_retweet_count": "",
        "replied_tweet_reply_count": "",
        "replied_tweet_like_count": "",
        "replied_tweet_quote_count": "",

        "quoted_user_id":"",
        "quoted_user_screen_name":"",
        "quoted_user_name":"",
        "quoted_user_followers_count":"",
        "quoted_user_following_count":"",
        "quoted_user_tweet_count":"",
        "quoted_user_listed_count":"",
        "quoted_user_protected":"",
        "quoted_user_verified":"",
        "quoted_user_description":"",
        "quoted_tweet_status_id":"",
        "quoted_tweet_conversation_id":"",
        "quoted_tweet_created_at":"",
        "quoted_tweet_lang":"",
        "quoted_tweet_source":"",
        "quoted_tweet_text":"",
        "quoted_tweet_retweet_count": "",
        "quoted_tweet_reply_count": "",
        "quoted_tweet_like_count": "",
        "quoted_tweet_quote_count": "",

        "geo_id":"",
        "geo_full_name":"",
        "geo_name":"",
        "geo_country":"",
        "geo_country_code":"",
        "geo_place_type":"",
        "geo_json":"",

        "user_id":"",
        "screen_name":"",
        "name":"",
        "account_created_at":"",
        "description":"",
        "url":"",
        "location":"",
        "followers_count":"",
        "following_count":"",
        "tweet_count":"",
        "listed_count":"",
        "protected":"",
        "verified":"",
        "queried_at":""
    }

    parsed_tweet["status_id"] = raw_tweet["id"]
    parsed_tweet["created_at"] = raw_tweet["created_at"]
    parsed_tweet["text"] = raw_tweet["text"]
    parsed_tweet["conversation_id"] = raw_tweet["conversation_id"]

    # entities
    if "entities" in raw_tweet.keys():
        if "hashtags" in raw_tweet["entities"].keys():
            parsed_tweet["hashtags"] = json.dumps([i["tag"] for i in raw_tweet["entities"]["hashtags"]])
        
        if "mentions" in raw_tweet["entities"].keys():
            parsed_tweet["mentions"] = json.dumps([i["username"] for i in raw_tweet["entities"]["mentions"]])

        if "urls" in raw_tweet["entities"].keys():
            try:
                parsed_tweet["url_location"] = json.dumps([i["expanded_url"] for i in raw_tweet["entities"]["urls"]])
            except:
                pass
            
            # experimental
            try:
                parsed_tweet["url_unwound"] = json.dumps([i["unwound_url"] for i in raw_tweet["entities"]["urls"]])
            except:
                pass

            try:
                parsed_tweet["url_title"] = json.dumps([i["title"] for i in raw_tweet["entities"]["urls"]])
            except:
                pass
            
            try:
                parsed_tweet["url_description"] = json.dumps([i["description"] for i in raw_tweet["entities"]["urls"]])
            except:
                pass

            try:
                parsed_tweet["url_sensitive"] = raw_tweet["possiby_sensitive"]
            except:
                pass

    
    # geo, needs testing
    # Check: https://developer.twitter.com/en/docs/twitter-api/data-dictionary/object-model/place
    try:
        parsed_tweet["geo_id"] = raw_tweet["geo"]["place_id"]
        parsed_tweet["geo_full_name"] = places_dict[raw_tweet["geo"]["place_id"]]["full_name"]
        parsed_tweet["geo_name"] = places_dict[raw_tweet["geo"]["place_id"]]["name"]
        parsed_tweet["geo_country"] = places_dict[raw_tweet["geo"]["place_id"]]["country"]
        parsed_tweet["geo_country_code"] = places_dict[raw_tweet["geo"]["place_id"]]["country_code"]
        parsed_tweet["place_type"] = places_dict[raw_tweet["geo"]["place_id"]]["geo_place_type"]
        parsed_tweet["geo_json"] = places_dict[raw_tweet["geo"]["place_id"]]["geo_json"]
    except:
        pass
    
    # BCP47 language tag
    try:
        parsed_tweet["lang"] = raw_tweet["lang"]
    except:
        pass
    
    parsed_tweet["reply_settings"] = raw_tweet["reply_settings"]
    try:
        parsed_tweet["source"] = raw_tweet["source"]
    except:
        parsed_tweet["source"] = ""

    parsed_tweet["retweet_count"] = raw_tweet["public_metrics"]["retweet_count"]
    parsed_tweet["reply_count"] = raw_tweet["public_metrics"]["reply_count"]
    parsed_tweet["like_count"] = raw_tweet["public_metrics"]["like_count"]
    parsed_tweet["quote_count"] = raw_tweet["public_metrics"]["quote_count"]

    if "referenced_tweets" in raw_tweet.keys():
        for referenced_tweet in raw_tweet["referenced_tweets"]:
            if referenced_tweet["type"] == "quoted":
                parsed_tweet["quoted_user_id"] = included_tweets_dict[referenced_tweet["id"]]["user_id"]
                parsed_tweet["quoted_user_screen_name"] = included_tweets_dict[referenced_tweet["id"]]["screen_name"]
                parsed_tweet["quoted_user_name"] = included_tweets_dict[referenced_tweet["id"]]["name"]
                parsed_tweet["quoted_user_followers_count"] = included_tweets_dict[referenced_tweet["id"]]["followers_count"]
                parsed_tweet["quoted_user_following_count"] = included_tweets_dict[referenced_tweet["id"]]["following_count"]
                parsed_tweet["quoted_user_tweet_count"] = included_tweets_dict[referenced_tweet["id"]]["tweet_count"]
                parsed_tweet["quoted_user_listed_count"] = included_tweets_dict[referenced_tweet["id"]]["listed_count"]
                parsed_tweet["quoted_user_protected"] = included_tweets_dict[referenced_tweet["id"]]["protected"]
                parsed_tweet["quoted_user_verified"] = included_tweets_dict[referenced_tweet["id"]]["verified"]
                parsed_tweet["quoted_user_description"] = included_tweets_dict[referenced_tweet["id"]]["description"]
                parsed_tweet["quoted_tweet_status_id"] = referenced_tweet["id"]
                parsed_tweet["quoted_tweet_conversation_id"] = included_tweets_dict[referenced_tweet["id"]]["conversation_id"]
                parsed_tweet["quoted_tweet_created_at"] = included_tweets_dict[referenced_tweet["id"]]["created_at"]
                parsed_tweet["quoted_tweet_lang"] = included_tweets_dict[referenced_tweet["id"]]["lang"]
                parsed_tweet["quoted_tweet_source"] = included_tweets_dict[referenced_tweet["id"]]["source"]
                parsed_tweet["quoted_tweet_text"] = included_tweets_dict[referenced_tweet["id"]]["text"]
                parsed_tweet["quoted_tweet_retweet_count"] = included_tweets_dict[referenced_tweet["id"]]["retweet_count"]
                parsed_tweet["quoted_tweet_reply_count"] = included_tweets_dict[referenced_tweet["id"]]["reply_count"]
                parsed_tweet["quoted_tweet_like_count"] = included_tweets_dict[referenced_tweet["id"]]["like_count"]
                parsed_tweet["quoted_tweet_quote_count"] = included_tweets_dict[referenced_tweet["id"]]["quote_count"]
            elif referenced_tweet["type"] == "retweeted":
                parsed_tweet["retweeted_user_id"] = included_tweets_dict[referenced_tweet["id"]]["user_id"]
                parsed_tweet["retweeted_user_screen_name"] = included_tweets_dict[referenced_tweet["id"]]["screen_name"]
                parsed_tweet["retweeted_user_name"] = included_tweets_dict[referenced_tweet["id"]]["name"]
                parsed_tweet["retweeted_user_followers_count"] = included_tweets_dict[referenced_tweet["id"]]["followers_count"]
                parsed_tweet["retweeted_user_following_count"] = included_tweets_dict[referenced_tweet["id"]]["following_count"]
                parsed_tweet["retweeted_user_tweet_count"] = included_tweets_dict[referenced_tweet["id"]]["tweet_count"]
                parsed_tweet["retweeted_user_listed_count"] = included_tweets_dict[referenced_tweet["id"]]["listed_count"]
                parsed_tweet["retweeted_user_protected"] = included_tweets_dict[referenced_tweet["id"]]["protected"]
                parsed_tweet["retweeted_user_verified"] = included_tweets_dict[referenced_tweet["id"]]["verified"]
                parsed_tweet["retweeted_user_description"] = included_tweets_dict[referenced_tweet["id"]]["description"]
                parsed_tweet["retweeted_tweet_status_id"] = referenced_tweet["id"]
                parsed_tweet["retweeted_tweet_conversation_id"] = included_tweets_dict[referenced_tweet["id"]]["conversation_id"]
                parsed_tweet["retweeted_tweet_created_at"] = included_tweets_dict[referenced_tweet["id"]]["created_at"]
                parsed_tweet["retweeted_tweet_lang"] = included_tweets_dict[referenced_tweet["id"]]["lang"]
                parsed_tweet["retweeted_tweet_source"] = included_tweets_dict[referenced_tweet["id"]]["source"]
                parsed_tweet["retweeted_tweet_text"] = included_tweets_dict[referenced_tweet["id"]]["text"]
                parsed_tweet["retweeted_tweet_retweet_count"] = included_tweets_dict[referenced_tweet["id"]]["retweet_count"]
                parsed_tweet["retweeted_tweet_reply_count"] = included_tweets_dict[referenced_tweet["id"]]["reply_count"]
                parsed_tweet["retweeted_tweet_like_count"] = included_tweets_dict[referenced_tweet["id"]]["like_count"]
                parsed_tweet["retweeted_tweet_quote_count"] = included_tweets_dict[referenced_tweet["id"]]["quote_count"]
                
            elif referenced_tweet["type"] == "replied_to":
                try:
                    parsed_tweet["replied_user_id"] = included_tweets_dict[referenced_tweet["id"]]["user_id"]
                    parsed_tweet["replied_user_screen_name"] = included_tweets_dict[referenced_tweet["id"]]["screen_name"]
                    parsed_tweet["replied_user_name"] = included_tweets_dict[referenced_tweet["id"]]["name"]
                    parsed_tweet["replied_user_followers_count"] = included_tweets_dict[referenced_tweet["id"]]["followers_count"]
                    parsed_tweet["replied_user_following_count"] = included_tweets_dict[referenced_tweet["id"]]["following_count"]
                    parsed_tweet["replied_user_tweet_count"] = included_tweets_dict[referenced_tweet["id"]]["tweet_count"]
                    parsed_tweet["replied_user_listed_count"] = included_tweets_dict[referenced_tweet["id"]]["listed_count"]
                    parsed_tweet["replied_user_protected"] = included_tweets_dict[referenced_tweet["id"]]["protected"]
                    parsed_tweet["replied_user_verified"] = included_tweets_dict[referenced_tweet["id"]]["verified"]
                    parsed_tweet["replied_user_description"] = included_tweets_dict[referenced_tweet["id"]]["description"]
                    parsed_tweet["replied_tweet_status_id"] = referenced_tweet["id"]
                    parsed_tweet["replied_tweet_conversation_id"] = included_tweets_dict[referenced_tweet["id"]]["conversation_id"]
                    parsed_tweet["replied_tweet_created_at"] = included_tweets_dict[referenced_tweet["id"]]["created_at"]
                    parsed_tweet["replied_tweet_lang"] = included_tweets_dict[referenced_tweet["id"]]["lang"]
                    parsed_tweet["replied_tweet_source"] = included_tweets_dict[referenced_tweet["id"]]["source"]
                    parsed_tweet["replied_tweet_text"] = included_tweets_dict[referenced_tweet["id"]]["text"]
                    parsed_tweet["replied_tweet_retweet_count"] = included_tweets_dict[referenced_tweet["id"]]["retweet_count"]
                    parsed_tweet["replied_tweet_reply_count"] = included_tweets_dict[referenced_tweet["id"]]["reply_count"]
                    parsed_tweet["replied_tweet_like_count"] = included_tweets_dict[referenced_tweet["id"]]["like_count"]
                    parsed_tweet["replied_tweet_quote_count"] = included_tweets_dict[referenced_tweet["id"]]["quote_count"]
                except:
                    parsed_tweet["replied_user_id"] = raw_tweet["in_reply_to_user_id"]
                
    # user fields
    parsed_tweet["user_id"] = raw_tweet["author_id"]
    parsed_tweet["screen_name"] = user_dict[raw_tweet["author_id"]]["username"]
    parsed_tweet["name"] = user_dict[raw_tweet["author_id"]]["name"]
    parsed_tweet["account_created_at"] = user_dict[raw_tweet["author_id"]]["created_at"]
    parsed_tweet["description"] = user_dict[raw_tweet["author_id"]]["description"]
    parsed_tweet["url"] = user_dict[raw_tweet["author_id"]]["url"]
    parsed_tweet["location"] = user_dict[raw_tweet["author_id"]]["location"]
    parsed_tweet["followers_count"] = user_dict[raw_tweet["author_id"]]["followers_count"]
    parsed_tweet["following_count"] = user_dict[raw_tweet["author_id"]]["following_count"]
    parsed_tweet["tweet_count"] = user_dict[raw_tweet["author_id"]]["tweet_count"]
    parsed_tweet["listed_count"] = user_dict[raw_tweet["author_id"]]["listed_count"]
    parsed_tweet["protected"] = user_dict[raw_tweet["author_id"]]["protected"]
    parsed_tweet["verified"] = user_dict[raw_tweet["author_id"]]["verified"]

    parsed_tweet["is_retweet"] = "False" if parsed_tweet["retweeted_tweet_status_id"] == "" else "True"
    parsed_tweet["is_reply"] = "False" if parsed_tweet["replied_tweet_status_id"] == "" else "True"
    parsed_tweet["is_quote"] = "False" if parsed_tweet["quoted_tweet_status_id"] == "" else "True"

    parsed_tweet["queried_at"] = queried_at

    return(parsed_tweet)

def parse_tweets(r):
    if "includes" in r.json().keys():
        if "users" in r.json()["includes"].keys():
            for user in r.json()["includes"]["users"]:
                if not user["id"] in user_dict.keys():
                    user_dict[user["id"]] = {}
                    try:
                        user_dict[user["id"]]["name"] = user["name"]
                    except:
                        user_dict[user["id"]]["name"] = ""

                    try:
                        user_dict[user["id"]]["username"] = user["username"]
                    except:
                        user_dict[user["id"]]["username"] = ""

                    try:
                        user_dict[user["id"]]["created_at"] = user["created_at"]
                    except:
                        user_dict[user["id"]]["created_at"] = ""

                    try:
                        user_dict[user["id"]]["description"] = user["description"]
                    except:
                        user_dict[user["id"]]["description"] = ""

                    try:
                        user_dict[user["id"]]["url"] = user["entities"]["url"]["urls"][0]["expanded_url"]
                    except:
                        try:
                            user_dict[user["id"]]["url"] = user["url"]
                        except:
                            user_dict[user["id"]]["url"] = ""

                    try:
                        user_dict[user["id"]]["location"] = user["location"]
                    except:
                        user_dict[user["id"]]["location"] = ""

                    try:
                        user_dict[user["id"]]["followers_count"] = user["public_metrics"]["followers_count"]
                    except:
                        user_dict[user["id"]]["followers_count"] = ""

                    try:
                        user_dict[user["id"]]["following_count"] = user["public_metrics"]["following_count"]
                    except:
                        user_dict[user["id"]]["following_count"] = ""

                    try:
                        user_dict[user["id"]]["tweet_count"] = user["public_metrics"]["tweet_count"]
                    except:
                        user_dict[user["id"]]["tweet_count"] = ""

                    try:
                        user_dict[user["id"]]["listed_count"] = user["public_metrics"]["listed_count"]
                    except:
                        user_dict[user["id"]]["listed_count"] = ""

                    try:
                        user_dict[user["id"]]["protected"] = user["protected"]
                    except:
                        user_dict[user["id"]]["protected"] = ""

                    try:
                        user_dict[user["id"]]["verified"] = user["verified"]
                    except:
                        user_dict[user["id"]]["verified"] = ""

        if "tweets" in r.json()["includes"].keys():
            for tweet in r.json()["includes"]["tweets"]:
                if not tweet["id"] in included_tweets_dict.keys():
                    included_tweets_dict[tweet["id"]] = {}
                    
                    included_tweets_dict[tweet["id"]]["conversation_id"] = tweet["conversation_id"]
                    included_tweets_dict[tweet["id"]]["created_at"] = tweet["created_at"]
                    included_tweets_dict[tweet["id"]]["lang"] = tweet["lang"]
                    included_tweets_dict[tweet["id"]]["source"] = tweet["source"]
                    included_tweets_dict[tweet["id"]]["text"] = tweet["text"]

                    included_tweets_dict[tweet["id"]]["retweet_count"] = tweet["public_metrics"]["retweet_count"]
                    included_tweets_dict[tweet["id"]]["reply_count"] = tweet["public_metrics"]["reply_count"]
                    included_tweets_dict[tweet["id"]]["like_count"] = tweet["public_metrics"]["like_count"]
                    included_tweets_dict[tweet["id"]]["quote_count"] = tweet["public_metrics"]["quote_count"]

                    included_tweets_dict[tweet["id"]]["user_id"] = tweet["author_id"]
                    included_tweets_dict[tweet["id"]]["screen_name"] = user_dict[tweet["author_id"]]["username"]
                    included_tweets_dict[tweet["id"]]["name"] = user_dict[tweet["author_id"]]["name"]
                    included_tweets_dict[tweet["id"]]["followers_count"] = user_dict[tweet["author_id"]]["followers_count"]
                    included_tweets_dict[tweet["id"]]["following_count"] = user_dict[tweet["author_id"]]["following_count"]
                    included_tweets_dict[tweet["id"]]["tweet_count"] = user_dict[tweet["author_id"]]["tweet_count"]
                    included_tweets_dict[tweet["id"]]["listed_count"] = user_dict[tweet["author_id"]]["listed_count"]
                    included_tweets_dict[tweet["id"]]["protected"] = user_dict[tweet["author_id"]]["protected"]
                    included_tweets_dict[tweet["id"]]["verified"] = user_dict[tweet["author_id"]]["verified"]
                    included_tweets_dict[tweet["id"]]["description"] = user_dict[tweet["author_id"]]["description"]

        if "places" in r.json()["includes"].keys():
            for place in r.json()["includes"]["places"]:
                if not place["id"] in places_dict.keys():
                    places_dict[place["id"]] = {}
                    try:
                        places_dict[place["id"]]["full_name"] = place["full_name"]
                    except:
                        places_dict[place["id"]]["full_name"] = ""
                    
                    try:
                        places_dict[place["id"]]["name"] = place["name"]
                    except:
                        places_dict[place["id"]]["name"] = ""
                    
                    try:
                        places_dict[place["id"]]["country"] = place["country_code"]
                    except:
                        places_dict[place["id"]]["country"] = ""
                    
                    try:
                        places_dict[place["id"]]["place_type"] = place["place_type"]
                    except:
                        places_dict[place["id"]]["place_type"] = ""
                    
                    try:
                        places_dict[place["id"]]["geo_json"] = json.dumps(place["geo"])
                    except:
                        places_dict[place["id"]]["geo_json"] = ""

    parsed_tweets = []
    for tweet in r.json()["data"]:
        parsed_tweets.append(parse_tweet(tweet))

    return(parsed_tweets)

def parse_user(raw_user):
    parsed_user = {
        "user_id":"",
        "screen_name":"",
        "name":"",
        "created_at":"",
        "description":"",
        "url":"",
        "location":"",
        "followers_count":"",
        "following_count":"",
        "tweet_count":"",
        "listed_count":"",
        "protected":"",
        "verified":"",
        "withheld":"",
        "pinned_tweet_id":"",
        "queried_at":""
    }

    try:
        parsed_user["user_id"] = raw_user["id"]
    except:
        parsed_user["user_id"] = ""

    try:
        parsed_user["screen_name"] = raw_user["username"]
    except:
        parsed_user["screen_name"] = ""

    try:
        parsed_user["name"] = raw_user["name"]
    except:
        parsed_user["screen_name"] = ""

    try:
        parsed_user["created_at"] = raw_user["created_at"]
    except:
        parsed_user["created_at"] = ""

    try:
        parsed_user["description"] = raw_user["description"]
    except:
        user_dict[user["id"]]["description"] = ""

    try:
        parsed_user["url"] = raw_user["entities"]["url"]["urls"][0]["expanded_url"]
    except:
        try:
            parsed_user["url"] = raw_user["url"]
        except:
            parsed_user["url"] = ""

    try:
        parsed_user["location"] = raw_user["location"]
    except:
        parsed_user["location"] = ""

    try:
        parsed_user["followers_count"] = raw_user["public_metrics"]["followers_count"]
    except:
        parsed_user["followers_count"] = ""

    try:
        parsed_user["following_count"] = raw_user["public_metrics"]["following_count"]
    except:
        parsed_user["following_count"] = ""

    try:
        parsed_user["tweet_count"] = raw_user["public_metrics"]["tweet_count"]
    except:
        parsed_user["tweet_count"] = ""

    try:
        parsed_user["listed_count"] = raw_user["public_metrics"]["listed_count"]
    except:
        parsed_user["listed_count"] = ""

    try:
        parsed_user["protected"] = raw_user["protected"]
    except:
        parsed_user["protected"] = ""

    try:
        parsed_user["verified"] = raw_user["verified"]
    except:
        parsed_user["verified"] = ""


    try:
        parsed_user["withheld"] = raw_user["withheld"]
    except:
        parsed_user["withheld"] = ""

    try:
        parsed_user["pinned_tweet_id"] = raw_user["pinned_tweet_id"]
    except:
        parsed_user["pinned_tweet_id"] = ""
    

    parsed_user["queried_at"] = queried_at

    return(parsed_user)

def parse_users(r):
    parsed_users = []
    for user in r.json()["data"]:
        parsed_users.append(parse_user(user))

    return(parsed_users)

def lookup_tweets(tweet_ids, bearer_token, verbose=True):
    if verbose and logger.level >= 20:
        logger.setLevel(logging.INFO)
    
    headers = {
        "Authorization": "Bearer {}".format(bearer_token),
    }
    
    query_ids = tweet_ids
    if len(query_ids) > 100:
        raise Exception("Query too long, halting")
    params = (
        ("ids", ",".join(query_ids)),
        ("tweet.fields", "author_id,created_at,conversation_id,text,lang,geo,entities,reply_settings,public_metrics,source,referenced_tweets"),
        ("user.fields", "id,name,username,created_at,description,url,location,protected,verified,public_metrics,entities"),
        ("expansions", "referenced_tweets.id,referenced_tweets.id.author_id,in_reply_to_user_id,author_id,attachments.media_keys,entities.mentions.username,geo.place_id")
    )
    
    logger.info(f"Searching for tweets with the following parameters (ids: {','.join(query_ids)})")

    try:
        r = request_session.get("https://api.twitter.com/2/tweets", headers=headers, params=params)
    except Exception as e:
        logger.error(f"Error getting tweets (Error: {e})")
        
    if (r.status_code == 429):
        sleep_time = math.ceil((datetime.datetime.fromtimestamp(int(r.headers["x-rate-limit-reset"])) - datetime.datetime.today()).total_seconds()) + 15
        if sleep_time < 1:
            sleep_time = 900
        logger.warning(f"Rate limit exceeded, resuming in {str(sleep_time)} seconds")
        time.sleep(sleep_time)
        r = request_session.get("https://api.twitter.com/2/tweets", headers=headers, params=params)

    if (r.status_code != 200):
        logger.error(f"Error getting tweets (status code: {r.status_code}), halting")
        exit()

    if "errors" in r.json().keys():
        logger.warning(f"No tweets found")
        return(None)

    queried_tweets = parse_tweets(r)
    logger.info(f"Retrieved {len(queried_tweets)} tweets ({get_datetime_range(queried_tweets)})")
    logger.info(f"{r.headers['x-rate-limit-remaining']} of {r.headers['x-rate-limit-limit']} calls remaining.")
    return(queried_tweets)

def lookup_users(user_names, bearer_token, verbose=True):
    if verbose and logger.level >= 20:
        logger.setLevel(logging.INFO)

    headers = {
        "Authorization": "Bearer {}".format(bearer_token),
    }
    if len(user_names) > 100:
        raise Exception("Query too long, halting")
    params = (
        ("usernames", ",".join(user_names)),
        ("user.fields", "id,name,username,created_at,description,url,location,protected,verified,public_metrics,entities,pinned_tweet_id,withheld")
    )
    logger.info(f"Looking up users with the following names: {','.join(user_names)}")

    try:
        r = request_session.get("https://api.twitter.com/2/users/by", headers=headers, params=params)
    except Exception as e:
        logger.error(f"Error getting tweets ({e}), halting")
        exit()
        
    if (r.status_code == 429):
        sleep_time = math.ceil((datetime.datetime.fromtimestamp(int(r.headers["x-rate-limit-reset"])) - datetime.datetime.today()).total_seconds()) + 15
        if sleep_time < 15:
            sleep_time = 900
        logger.warning(f"Rate limit exceeded, resuming in {str(sleep_time)} seconds")
        time.sleep(sleep_time)
        r = request_session.get("https://api.twitter.com/2/users/by", headers=headers, params=params)

    if (r.status_code != 200):
        logger.error(f"Error getting tweets (status code: {r.status_code}), halting")
        exit()

    # if "errors" in r.json().keys():
    #     print("WARNING (errors in response):")
    #     for e in r.json()["errors"]:
    #         print("\t", e["detail"])

    queried_users = parse_users(r)
    logger.info(f"Retrieved {len(queried_users)} users")
    logger.info(f"{r.headers['x-rate-limit-remaining']} of {r.headers['x-rate-limit-limit']} calls remaining.")
    
    if verbose and logger.level >= 20:
        logger.setLevel(logging.WARNING)
    return(queried_users)

def lookup_retweet_users(tweet_id, bearer_token, verbose=True):
    if verbose and logger.level >= 20:
        logger.setLevel(logging.INFO)

    headers = {
        "Authorization": "Bearer {}".format(bearer_token),
    }
    if isinstance(tweet_id, str):
        raise Exception("tweet_id must be a single string, halting")
    
    params = (
        ("user.fields", "id,name,username,created_at,description,url,location,protected,verified,public_metrics,entities,pinned_tweet_id,withheld"),
    )
    logger.info(f"Getting users that retweeted the following tweet: {tweet_id}")

    try:
        r = request_session.get(f"https://api.twitter.com/2/tweets/{tweet_id}/retweeted_by", headers=headers, params=params)
    except Exception as e:
        logger.error(f"Error getting tweets ({e}), halting")
        exit()
        
    if (r.status_code == 429):
        sleep_time = math.ceil((datetime.datetime.fromtimestamp(int(r.headers["x-rate-limit-reset"])) - datetime.datetime.today()).total_seconds()) + 15
        if sleep_time < 15:
            sleep_time = 900
        logger.warning(f"Rate limit exceeded, resuming in {str(sleep_time)} seconds")
        time.sleep(sleep_time)
        r = request_session.get("https://api.twitter.com/2/users/by", headers=headers, params=params)

    if (r.status_code != 200):
        logger.error(f"Error getting tweets (status code: {r.status_code}), halting")
        exit()

    # if "errors" in r.json().keys():
    #     print("WARNING (errors in response):")
    #     for e in r.json()["errors"]:
    #         print("\t", e["detail"])

    queried_users = parse_users(r)
    logger.info(f"Retrieved {len(queried_users)} users")
    logger.info(f"{r.headers['x-rate-limit-remaining']} of {r.headers['x-rate-limit-limit']} calls remaining.")
    
    if verbose and logger.level >= 20:
        logger.setLevel(logging.WARNING)
    
    return(queried_users)

def search_tweets(query, bearer_token, since_id=None, until_id=None, start_time=None, end_time=None, mode="recent", verbose=False):
    if verbose and logger.level >= 20:
        logger.setLevel(logging.INFO)
    
    headers = {
        "Authorization": "Bearer {}".format(bearer_token),
    }
    if len(query) > 1024:
        raise Exception("Query too long, halting")
    params = (
        ("query", query),
        ("max_results", 500),
        ("tweet.fields", "author_id,created_at,conversation_id,text,lang,geo,entities,reply_settings,public_metrics,source,referenced_tweets"),
        ("user.fields", "id,name,username,created_at,description,url,location,protected,verified,public_metrics,entities"),
        ("expansions", "referenced_tweets.id,referenced_tweets.id.author_id,in_reply_to_user_id,author_id,attachments.media_keys,entities.mentions.username,geo.place_id")
    )

    logging_message = [f"{query=}"]

    if since_id:
        params = params + (("since_id", since_id),)
        logging_message.append(f"{since_id=}")

    if until_id:
        params = params + (("until_id", until_id),)
        logging_message.append(f"{until_id=}")

    if start_time:
        params = params + (("start_time", start_time),)
        logging_message.append(f"{start_time=}")
    else:
        logger.warning(f"No start_time was set. By default, a request will return Tweets from up to 30 days ago if you do not include this parameter.")

    if end_time:
        params = params + (("end_time", end_time),)
        logging_message.append(f"{end_time=}")

    logger.info(f"Searching for tweets with the following parameters: {', '.join(logging_message)}")

    try:
        r = request_session.get("https://api.twitter.com/2/tweets/search/{}".format(mode), headers=headers, params=params)
        time.sleep(1) # only one request per second
    except Exception as e:
        logger.error(f"Error getting tweets (Error: {e})")
    
    if (r.status_code == 429):
        sleep_time = math.ceil((datetime.datetime.fromtimestamp(int(r.headers["x-rate-limit-reset"])) - datetime.datetime.today()).total_seconds()) + 15
        if sleep_time < 15:
            sleep_time = 900
        logger.warning(f"Rate limit exceeded, resuming in {str(sleep_time)} seconds")
        time.sleep(sleep_time)
        r = request_session.get("https://api.twitter.com/2/tweets/search/{}".format(mode), headers=headers, params=params)
        time.sleep(1) # only one request per second

    if (r.status_code != 200):
        if (r.status_code == 400):
            logger.error(f"Error getting tweets (status code: {r.status_code}, Bad Request), (message: {r.json()['errors'][0]['message']}), halting")
        else:
            logger.error(f"Error getting tweets (status code: {r.status_code}), halting")
        exit()

    if r.json()["meta"]["result_count"] == 0:
        logger.warning(f"No tweets found")
        return(None)

    searched_tweets = parse_tweets(r)

    if "next_token" in r.json()["meta"]:
        try:
            loop_counter = 0
            while "next_token" in r.json()["meta"]:
                loop_counter += 1
                logger.debug(f"Retrieved {len(searched_tweets)} tweets ({get_datetime_range(searched_tweets)})")
                next_token = r.json()["meta"]["next_token"]
                time.sleep(1.2)
                r = request_session.get("https://api.twitter.com/2/tweets/search/{}".format(mode), headers=headers, params=params + (("pagination_token", next_token),))
                time.sleep(1) # only one request per second

                if (r.status_code == 429):
                    sleep_time = math.ceil((datetime.datetime.fromtimestamp(int(r.headers["x-rate-limit-reset"])) - datetime.datetime.today()).total_seconds()) + 15
                    if sleep_time < 15:
                        sleep_time = 900
                    logger.warning(f"Rate limit exceeded, resuming in {str(sleep_time)} seconds")
                    time.sleep(sleep_time)
                    r = request_session.get("https://api.twitter.com/2/tweets/search/{}".format(mode), headers=headers, params=params + (("pagination_token", next_token),))
                    time.sleep(1) # only one request per second

                if r.json()["meta"]["result_count"] > 0:
                    searched_tweets.extend(parse_tweets(r))
        except Exception:
            logger.warning(f"Error in while loop results, continuing (Traceback: {traceback.format_exc()})")
    
    logger.info(f"Retrieved {len(searched_tweets)} tweets ({get_datetime_range(searched_tweets)})")
    logger.info(f"{r.headers['x-rate-limit-remaining']} of {r.headers['x-rate-limit-limit']} calls remaining.")
    
    if verbose and logger.level >= 20:
        logger.setLevel(logging.WARNING)
    return(searched_tweets)

def tweets_to_csv(queried_tweets, file_name, append=False, verbose=False):
    if verbose and logger.level >= 20:
        logger.setLevel(logging.INFO)
    
    file_mode = "a+" if append else "w"
    if append:
        logger.info(f"Appending to file {file_name}")
    else:
        logger.info(f"Writing to file {file_name}")
        if os.path.isfile(file_name):
            logger.warning(f"Overwriting existing file ({file_name})")

    if queried_tweets:
        with open(file_name, file_mode, newline='') as f:
            writer = csv.writer(f, dialect="unix")
            if not append:
                writer.writerow([
                    "status_id",
                    "created_at",
                    "text",
                    "conversation_id",
                    "hashtags",
                    "mentions",
                    "url_location",
                    "url_unwound",
                    "url_title",
                    "url_description",
                    "url_sensitive",
                    "geo",
                    "lang",
                    "source",
                    "reply_settings",
                    "retweet_count",
                    "reply_count",
                    "like_count",
                    "quote_count",
                    "is_retweet",
                    "is_reply",
                    "is_quote",
                    "retweeted_user_id",
                    "retweeted_user_screen_name",
                    "retweeted_user_name",
                    "retweeted_user_followers_count",
                    "retweeted_user_following_count",
                    "retweeted_user_tweet_count",
                    "retweeted_user_listed_count",
                    "retweeted_user_protected",
                    "retweeted_user_verified",
                    "retweeted_user_description",
                    "retweeted_tweet_status_id",
                    "retweeted_tweet_conversation_id",
                    "retweeted_tweet_created_at",
                    "retweeted_tweet_lang",
                    "retweeted_tweet_source",
                    "retweeted_tweet_text",
                    "retweeted_tweet_retweet_count",
                    "retweeted_tweet_reply_count",
                    "retweeted_tweet_like_count",
                    "retweeted_tweet_quote_count",
                    "replied_user_id",
                    "replied_user_screen_name",
                    "replied_user_name",
                    "replied_user_followers_count",
                    "replied_user_following_count",
                    "replied_user_tweet_count",
                    "replied_user_listed_count",
                    "replied_user_protected",
                    "replied_user_verified",
                    "replied_user_description",
                    "replied_tweet_status_id",
                    "replied_tweet_conversation_id",
                    "replied_tweet_created_at",
                    "replied_tweet_lang",
                    "replied_tweet_source",
                    "replied_tweet_text",
                    "replied_tweet_retweet_count",
                    "replied_tweet_reply_count",
                    "replied_tweet_like_count",
                    "replied_tweet_quote_count",
                    "quoted_user_id",
                    "quoted_user_screen_name",
                    "quoted_user_name",
                    "quoted_user_followers_count",
                    "quoted_user_following_count",
                    "quoted_user_tweet_count",
                    "quoted_user_listed_count",
                    "quoted_user_protected",
                    "quoted_user_verified",
                    "quoted_user_description",
                    "quoted_tweet_status_id",
                    "quoted_tweet_conversation_id",
                    "quoted_tweet_created_at",
                    "quoted_tweet_lang",
                    "quoted_tweet_source",
                    "quoted_tweet_text",
                    "quoted_tweet_retweet_count",
                    "quoted_tweet_reply_count",
                    "quoted_tweet_like_count",
                    "quoted_tweet_quote_count",
                    "geo_id",
                    "geo_full_name",
                    "geo_name",
                    "geo_country",
                    "geo_country_code",
                    "geo_place_type",
                    "geo_json",
                    "user_id",
                    "screen_name",
                    "name",
                    "account_created_at",
                    "description",
                    "url",
                    "location",
                    "followers_count",
                    "following_count",
                    "tweet_count",
                    "listed_count",
                    "protected",
                    "verified",
                    "queried_at"
                    ])
            elif append and os.path.getsize(file_name) == 0:
                writer.writerow([
                    "status_id",
                    "created_at",
                    "text",
                    "conversation_id",
                    "hashtags",
                    "mentions",
                    "url_location",
                    "url_unwound",
                    "url_title",
                    "url_description",
                    "url_sensitive",
                    "geo",
                    "lang",
                    "source",
                    "reply_settings",
                    "retweet_count",
                    "reply_count",
                    "like_count",
                    "quote_count",
                    "is_retweet",
                    "is_reply",
                    "is_quote",
                    "retweeted_user_id",
                    "retweeted_user_screen_name",
                    "retweeted_user_name",
                    "retweeted_user_followers_count",
                    "retweeted_user_following_count",
                    "retweeted_user_tweet_count",
                    "retweeted_user_listed_count",
                    "retweeted_user_protected",
                    "retweeted_user_verified",
                    "retweeted_user_description",
                    "retweeted_tweet_status_id",
                    "retweeted_tweet_conversation_id",
                    "retweeted_tweet_created_at",
                    "retweeted_tweet_lang",
                    "retweeted_tweet_source",
                    "retweeted_tweet_text",
                    "retweeted_tweet_retweet_count",
                    "retweeted_tweet_reply_count",
                    "retweeted_tweet_like_count",
                    "retweeted_tweet_quote_count",
                    "replied_user_id",
                    "replied_user_screen_name",
                    "replied_user_name",
                    "replied_user_followers_count",
                    "replied_user_following_count",
                    "replied_user_tweet_count",
                    "replied_user_listed_count",
                    "replied_user_protected",
                    "replied_user_verified",
                    "replied_user_description",
                    "replied_tweet_status_id",
                    "replied_tweet_conversation_id",
                    "replied_tweet_created_at",
                    "replied_tweet_lang",
                    "replied_tweet_source",
                    "replied_tweet_text",
                    "replied_tweet_retweet_count",
                    "replied_tweet_reply_count",
                    "replied_tweet_like_count",
                    "replied_tweet_quote_count",
                    "quoted_user_id",
                    "quoted_user_screen_name",
                    "quoted_user_name",
                    "quoted_user_followers_count",
                    "quoted_user_following_count",
                    "quoted_user_tweet_count",
                    "quoted_user_listed_count",
                    "quoted_user_protected",
                    "quoted_user_verified",
                    "quoted_user_description",
                    "quoted_tweet_status_id",
                    "quoted_tweet_conversation_id",
                    "quoted_tweet_created_at",
                    "quoted_tweet_lang",
                    "quoted_tweet_source",
                    "quoted_tweet_text",
                    "quoted_tweet_retweet_count",
                    "quoted_tweet_reply_count",
                    "quoted_tweet_like_count",
                    "quoted_tweet_quote_count",
                    "geo_id",
                    "geo_full_name",
                    "geo_name",
                    "geo_country",
                    "geo_country_code",
                    "geo_place_type",
                    "geo_json",
                    "user_id",
                    "screen_name",
                    "name",
                    "account_created_at",
                    "description",
                    "url",
                    "location",
                    "followers_count",
                    "following_count",
                    "tweet_count",
                    "listed_count",
                    "protected",
                    "verified",
                    "queried_at"
                    ])
            for parsed_tweet in queried_tweets:
                writer.writerow([
                    parsed_tweet["status_id"],
                    parsed_tweet["created_at"],
                    parsed_tweet["text"],
                    parsed_tweet["conversation_id"],
                    parsed_tweet["hashtags"],
                    parsed_tweet["mentions"],
                    parsed_tweet["url_location"],
                    parsed_tweet["url_unwound"],
                    parsed_tweet["url_title"],
                    parsed_tweet["url_description"],
                    parsed_tweet["url_sensitive"],
                    parsed_tweet["geo"],
                    parsed_tweet["lang"],
                    parsed_tweet["source"],
                    parsed_tweet["reply_settings"],
                    parsed_tweet["retweet_count"],
                    parsed_tweet["reply_count"],
                    parsed_tweet["like_count"],
                    parsed_tweet["quote_count"],
                    parsed_tweet["is_retweet"],
                    parsed_tweet["is_reply"],
                    parsed_tweet["is_quote"],
                    parsed_tweet["retweeted_user_id"],
                    parsed_tweet["retweeted_user_screen_name"],
                    parsed_tweet["retweeted_user_name"],
                    parsed_tweet["retweeted_user_followers_count"],
                    parsed_tweet["retweeted_user_following_count"],
                    parsed_tweet["retweeted_user_tweet_count"],
                    parsed_tweet["retweeted_user_listed_count"],
                    parsed_tweet["retweeted_user_protected"],
                    parsed_tweet["retweeted_user_verified"],
                    parsed_tweet["retweeted_user_description"],
                    parsed_tweet["retweeted_tweet_status_id"],
                    parsed_tweet["retweeted_tweet_conversation_id"],
                    parsed_tweet["retweeted_tweet_created_at"],
                    parsed_tweet["retweeted_tweet_lang"],
                    parsed_tweet["retweeted_tweet_source"],
                    parsed_tweet["retweeted_tweet_text"],
                    parsed_tweet["retweeted_tweet_retweet_count"],
                    parsed_tweet["retweeted_tweet_reply_count"],
                    parsed_tweet["retweeted_tweet_like_count"],
                    parsed_tweet["retweeted_tweet_quote_count"],
                    parsed_tweet["replied_user_id"],
                    parsed_tweet["replied_user_screen_name"],
                    parsed_tweet["replied_user_name"],
                    parsed_tweet["replied_user_followers_count"],
                    parsed_tweet["replied_user_following_count"],
                    parsed_tweet["replied_user_tweet_count"],
                    parsed_tweet["replied_user_listed_count"],
                    parsed_tweet["replied_user_protected"],
                    parsed_tweet["replied_user_verified"],
                    parsed_tweet["replied_user_description"],
                    parsed_tweet["replied_tweet_status_id"],
                    parsed_tweet["replied_tweet_conversation_id"],
                    parsed_tweet["replied_tweet_created_at"],
                    parsed_tweet["replied_tweet_lang"],
                    parsed_tweet["replied_tweet_source"],
                    parsed_tweet["replied_tweet_text"],
                    parsed_tweet["replied_tweet_retweet_count"],
                    parsed_tweet["replied_tweet_reply_count"],
                    parsed_tweet["replied_tweet_like_count"],
                    parsed_tweet["replied_tweet_quote_count"],
                    parsed_tweet["quoted_user_id"],
                    parsed_tweet["quoted_user_screen_name"],
                    parsed_tweet["quoted_user_name"],
                    parsed_tweet["quoted_user_followers_count"],
                    parsed_tweet["quoted_user_following_count"],
                    parsed_tweet["quoted_user_tweet_count"],
                    parsed_tweet["quoted_user_listed_count"],
                    parsed_tweet["quoted_user_protected"],
                    parsed_tweet["quoted_user_verified"],
                    parsed_tweet["quoted_user_description"],
                    parsed_tweet["quoted_tweet_status_id"],
                    parsed_tweet["quoted_tweet_conversation_id"],
                    parsed_tweet["quoted_tweet_created_at"],
                    parsed_tweet["quoted_tweet_lang"],
                    parsed_tweet["quoted_tweet_source"],
                    parsed_tweet["quoted_tweet_text"],
                    parsed_tweet["quoted_tweet_retweet_count"],
                    parsed_tweet["quoted_tweet_reply_count"],
                    parsed_tweet["quoted_tweet_like_count"],
                    parsed_tweet["quoted_tweet_quote_count"],
                    parsed_tweet["geo_id"],
                    parsed_tweet["geo_full_name"],
                    parsed_tweet["geo_name"],
                    parsed_tweet["geo_country"],
                    parsed_tweet["geo_country_code"],
                    parsed_tweet["geo_place_type"],
                    parsed_tweet["geo_json"],
                    parsed_tweet["user_id"],
                    parsed_tweet["screen_name"],
                    parsed_tweet["name"],
                    parsed_tweet["account_created_at"],
                    parsed_tweet["description"],
                    parsed_tweet["url"],
                    parsed_tweet["location"],
                    parsed_tweet["followers_count"],
                    parsed_tweet["following_count"],
                    parsed_tweet["tweet_count"],
                    parsed_tweet["listed_count"],
                    parsed_tweet["protected"],
                    parsed_tweet["verified"],
                    parsed_tweet["queried_at"]
                    ])
    else:
        logger.warning(f"No tweets to write to file")

    if verbose and logger.level >= 20:
        logger.setLevel(logging.WARNING)

def users_to_csv(queried_users, file_name, append=False, verbose=False):
    file_mode = "a+" if append else "w"
    if append:
        logger.info(f"Appending to file {file_name}")
    else:
        logger.info(f"Writing to file {file_name}")
        if os.path.isfile(file_name):
            logger.warning(f"Overwriting existing file ({file_name})")
    
    if queried_users:
        with open(file_name, file_mode, newline='') as f:
            writer = csv.writer(f, dialect="unix")
            if not append:
                writer.writerow([
                    "user_id",
                    "screen_name",
                    "name",
                    "created_at",
                    "description",
                    "url",
                    "location",
                    "followers_count",
                    "following_count",
                    "tweet_count",
                    "listed_count",
                    "protected",
                    "verified",
                    "withheld",
                    "pinned_tweet_id",
                    "queried_at"
                    ])
            elif append and os.path.getsize(file_name) == 0:
                writer.writerow([
                    "user_id",
                    "screen_name",
                    "name",
                    "created_at",
                    "description",
                    "url",
                    "location",
                    "followers_count",
                    "following_count",
                    "tweet_count",
                    "listed_count",
                    "protected",
                    "verified",
                    "withheld",
                    "pinned_tweet_id",
                    "queried_at"
                    ])
            
            for parsed_user in queried_users:
                writer.writerow([
                    parsed_user["user_id"],
                    parsed_user["screen_name"],
                    parsed_user["name"],
                    parsed_user["created_at"],
                    parsed_user["description"],
                    parsed_user["url"],
                    parsed_user["location"],
                    parsed_user["followers_count"],
                    parsed_user["following_count"],
                    parsed_user["tweet_count"],
                    parsed_user["listed_count"],
                    parsed_user["protected"],
                    parsed_user["verified"],
                    parsed_user["withheld"],
                    parsed_user["pinned_tweet_id"],
                    parsed_user["queried_at"]
                    ])
    else:
        logger.warning(f"No tweets to write to file")

    if verbose and logger.level >= 20:
        logger.setLevel(logging.WARNING)
