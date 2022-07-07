f.read_tweets <- function(file_path) {
tweet_col_types <- cols(
    status_id = col_character(),
    created_at = col_datetime(format = ""),
    text = col_character(),
    conversation_id = col_character(),
    hashtags = col_character(),
    mentions = col_character(),
    url_location = col_character(),
    url_unwound = col_character(),
    url_title = col_character(),
    url_description = col_character(),
    url_sensitive = col_logical(),
    geo = col_character(),
    lang = col_character(),
    source = col_character(),
    reply_settings = col_character(),
    retweet_count = col_double(),
    reply_count = col_double(),
    like_count = col_double(),
    quote_count = col_double(),
    is_retweet = col_logical(),
    is_reply = col_logical(),
    is_quote = col_logical(),
    retweeted_user_id = col_character(),
    retweeted_user_screen_name = col_character(),
    retweeted_user_name = col_character(),
    retweeted_user_followers_count = col_double(),
    retweeted_user_following_count = col_double(),
    retweeted_user_tweet_count = col_double(),
    retweeted_user_listed_count = col_double(),
    retweeted_user_protected = col_logical(),
    retweeted_user_verified = col_logical(),
    retweeted_user_description = col_character(),
    retweeted_tweet_status_id = col_character(),
    retweeted_tweet_conversation_id = col_character(),
    retweeted_tweet_created_at = col_datetime(format = ""),
    retweeted_tweet_lang = col_character(),
    retweeted_tweet_source = col_character(),
    retweeted_tweet_text = col_character(),
    retweeted_tweet_retweet_count = col_double(),
    retweeted_tweet_reply_count = col_double(),
    retweeted_tweet_like_count = col_double(),
    retweeted_tweet_quote_count = col_double(),
    replied_user_id = col_character(),
    replied_user_screen_name = col_character(),
    replied_user_name = col_character(),
    replied_user_followers_count = col_double(),
    replied_user_following_count = col_double(),
    replied_user_tweet_count = col_double(),
    replied_user_listed_count = col_double(),
    replied_user_protected = col_logical(),
    replied_user_verified = col_logical(),
    replied_user_description = col_character(),
    replied_tweet_status_id = col_character(),
    replied_tweet_conversation_id = col_character(),
    replied_tweet_created_at = col_datetime(format = ""),
    replied_tweet_lang = col_character(),
    replied_tweet_source = col_character(),
    replied_tweet_text = col_character(),
    replied_tweet_retweet_count = col_double(),
    replied_tweet_reply_count = col_double(),
    replied_tweet_like_count = col_double(),
    replied_tweet_quote_count = col_double(),
    quoted_user_id = col_character(),
    quoted_user_screen_name = col_character(),
    quoted_user_name = col_character(),
    quoted_user_followers_count = col_double(),
    quoted_user_following_count = col_double(),
    quoted_user_tweet_count = col_double(),
    quoted_user_listed_count = col_double(),
    quoted_user_protected = col_logical(),
    quoted_user_verified = col_logical(),
    quoted_user_description = col_character(),
    quoted_tweet_status_id = col_character(),
    quoted_tweet_conversation_id = col_character(),
    quoted_tweet_created_at = col_datetime(format = ""),
    quoted_tweet_lang = col_character(),
    quoted_tweet_source = col_character(),
    quoted_tweet_text = col_character(),
    quoted_tweet_retweet_count = col_double(),
    quoted_tweet_reply_count = col_double(),
    quoted_tweet_like_count = col_double(),
    quoted_tweet_quote_count = col_double(),
    geo_id = col_character(),
    geo_full_name = col_character(),
    geo_name = col_character(),
    geo_country = col_character(),
    geo_country_code = col_character(),
    geo_place_type = col_character(),
    geo_json = col_character(),
    user_id = col_character(),
    screen_name = col_character(),
    name = col_character(),
    account_created_at = col_datetime(format = ""),
    description = col_character(),
    url = col_character(),
    location = col_character(),
    followers_count = col_double(),
    following_count = col_double(),
    tweet_count = col_double(),
    listed_count = col_double(),
    protected = col_logical(),
    verified = col_logical(),
    queried_at = col_double()
)
df.tweets <- readr::read_delim(
    file = file_path,
    delim = ",",
    trim_ws = TRUE,
    skip_empty_rows = TRUE,
    escape_double = TRUE,
    col_types = tweet_col_types
) |>
    mutate(queried_at = as_datetime(queried_at))
f.parse_json <- function(j) {
    if (identical(j, character(0))) {
        return(NA)
    } else if (is.na(j)) {
        return(NA)
    } else {
        return(jsonlite::fromJSON(j))
    }
}

df.tweets$hashtags <- lapply(df.tweets$hashtags, f.parse_json)
df.tweets$mentions <- lapply(df.tweets$mentions, f.parse_json)
df.tweets$url_location <- lapply(df.tweets$url_location, f.parse_json)
df.tweets$url_unwound <- lapply(df.tweets$url_unwound, f.parse_json)
df.tweets$url_title <- lapply(df.tweets$url_title, f.parse_json)
df.tweets$url_description <- lapply(df.tweets$url_description, f.parse_json)
df.tweets$url_sensitive <- lapply(df.tweets$url_sensitive, f.parse_json)
return(df.tweets)
}