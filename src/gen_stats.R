is_package_installed <- function(p) {
    return(p %in% rownames(installed.packages()))
}

if(!is_package_installed("jsonlite")) {install.packages("jsonlite")}
suppressMessages(suppressWarnings(library(jsonlite, warn.conflicts = F, quietly = T)))
if(!is_package_installed("readr")) {install.packages("readr")}
suppressMessages(suppressWarnings(library(readr, warn.conflicts = F, quietly = T)))
if(!is_package_installed("dplyr")) {install.packages("dplyr")}
suppressMessages(suppressWarnings(library(dplyr,warn.conflicts = F, quietly = T)))
if(!is_package_installed("ggplot2")) {install.packages("ggplot2")}
suppressMessages(suppressWarnings(library(ggplot2,warn.conflicts = F, quietly = T)))
if(!is_package_installed("scales")) {install.packages("scales")}
suppressMessages(suppressWarnings(library(scales,warn.conflicts = F, quietly = T)))
if(!is_package_installed("lubridate")) {install.packages("lubridate")}
suppressMessages(suppressWarnings(library(lubridate,warn.conflicts = F, quietly = T)))
if(!is_package_installed("patchwork")) {install.packages("patchwork")}
suppressMessages(suppressWarnings(library(patchwork,warn.conflicts = F, quietly = T)))
if(!is_package_installed("tidyr")) {install.packages("tidyr")}
suppressMessages(suppressWarnings(library(tidyr,warn.conflicts = F, quietly = T)))
if(!is_package_installed("stringr")) {install.packages("stringr")}
suppressMessages(suppressWarnings(library(stringr,warn.conflicts = F, quietly = T)))

source("src/read_tweets.R")

argv <- commandArgs(trailingOnly = T)
user_name <- list(name=str_to_lower(argv[1]), tz=argv[2])

df <- f.read_tweets(sprintf("Data/%s.csv", user_name$name)) |> 
    filter(str_to_lower(screen_name) == user_name$name) |> 
    filter(!duplicated(status_id))

user_name$n_tweets <- format(sum(!df$is_retweet), big.mark = ",")
user_name$n_retweets <- format(sum(df$is_retweet), big.mark = ",")
user_name$n_total <- format(nrow(df), big.mark = ",")
user_name$p_retweets <- format(round(100*(mean(df$is_retweet)), 2) , nsmall = 2)
user_name$p_replies <- format(round(100*(mean(df$is_reply)), 2) , nsmall = 2)
user_name$time_on_twitter <- max(df$created_at)-min(df$created_at)

if (sum(!df$is_retweet) != 0) {
    if (units(user_name$time_on_twitter) == "days") {
        if (user_name$time_on_twitter > 100) {
            user_name$tweets_per_day_100 <- df |> 
                filter(created_at > max(created_at)-lubridate::make_difftime(day = 100)) |> 
                nrow()
            user_name$tweets_per_day_100 <- format(round(user_name$tweets_per_day_100/100, 2) , nsmall = 2)
        }
        user_name$tweets_per_day <- format(round(as.numeric(user_name$time_on_twitter)/nrow(df), 2) , nsmall = 2)
    }
}

if (sum(df$is_retweet) != 0) {
    if (units(user_name$time_on_twitter) == "days") {
        if (user_name$time_on_twitter > 100) {
            user_name$tweets_per_day_100_no_rt <- df |> 
                filter(!is_retweet) |> 
                filter(created_at > max(created_at)-lubridate::make_difftime(day = 100)) |> 
                nrow()
            user_name$tweets_per_day_100_no_rt <- format(round(user_name$tweets_per_day_100_no_rt/100, 2) , nsmall = 2)
            
        }
        user_name$tweets_per_day_no_rt <- format(round(as.numeric(user_name$time_on_twitter)/nrow(df |> filter(!is_retweet)), 2) , nsmall = 2)
    }
}

viz <- df |> 
    mutate(
        date = date(created_at),
        h = hour(with_tz(created_at, tz = user_name$tz)),
        m = minute(created_at),
        s = second(created_at),
        wd = factor(format(created_at, "%u"), levels = 1:7, labels = c("Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun")),
        time = (h*60*60)+(m*60)
    ) |> 
    mutate(dmonth = format(created_at, format = "%Y-%m")) |> 
    #mutate(dmonth = factor(dmonth)) |> 
    mutate(year = as.numeric(year(created_at))) |> 
    mutate(is_retweet = factor(ifelse(is_retweet, "Retweet", "Tweet"), levels = c("Tweet", "Retweet"))) |>
    select(date, time, h, m, s, wd, year, dmonth, created_at, is_retweet)

fig.tweets_jitter <- viz |> 
    ggplot(aes(x = as.POSIXct(date), y = time, color = is_retweet)) +
    scale_y_continuous(
        breaks = seq(0, (60*60*24)-1, 60*60),
        labels = \(x) {x/60/60}
    ) +
    # scale_x_datetime(
    #     limits = as.POSIXct(c(as.Date(sprintf("%s-01-01", min(year(viz$created_at)))), as.Date("2023-01-01"))),
    #     breaks = as.POSIXct(seq(as.Date(sprintf("%s-01-01", min(year(viz$created_at)))), as.Date("2024-01-01"), by = "1 years")) ,
    #     date_labels = "%Y"
    # ) +
    scale_color_manual(values = c("#008837", "#7b3294")) +
    geom_jitter(alpha = 0.25) +
    labs(
        y = "Hour",
        x = "Date",
        color = "Type"
    ) + 
    theme(legend.position = "bottom", legend.box = "horizontal")
    
fig.time_of_day <- viz |> 
    count(h) |> 
    ggplot(aes(x = h, y = n)) +
    geom_col() +
    coord_flip() +
    theme_void()

fig.tweets_cols <- viz |> 
    ggplot(aes(x = as.POSIXct(date), fill = is_retweet)) +
    geom_histogram(position = "stack", bins = length(unique(viz$date))/10) +
    scale_fill_manual(values = c("#008837", "#7b3294")) +
    theme_void() +
    theme(legend.position = "none")

fig.stats <- (
    ((fig.tweets_cols | plot_spacer()) + plot_layout(widths = c(10, 2))) / 
    ((fig.tweets_jitter | fig.time_of_day) + plot_layout(widths = c(10, 2)))
) + plot_layout(heights = c(2, 10)) +
    plot_annotation(title = sprintf("@%s", user_name$name), subtitle = sprintf("User has %s tweets (%s%% retweets)", user_name$n_total, user_name$p_retweets))

ggsave(
    sprintf("Output/%s_tweets.png", user_name$name), 
    fig.stats, 
    scale = .8, 
    height = 3000, 
    width = 4500, 
    units = "px", 
    limitsize = F
)

fig.h <- viz |> 
    count(h) |> 
    mutate(p = n/sum(n)) |> 
    ggplot(aes(x = h, y = p)) +
    geom_col() +
    scale_x_continuous(breaks = 0:23) +
    scale_y_continuous(labels = scales::percent) +
    labs(x = "Hours", y = "")

fig.m <- viz |> 
    count(m) |> 
    mutate(p = n/sum(n)) |> 
    ggplot(aes(x = m, y = p)) +
    geom_col() +
    scale_x_continuous(breaks = seq(0, 60, 15), minor_breaks = seq(0, 60, 5)) +
    scale_y_continuous(labels = scales::percent) +
    labs(x = "Minutes", y = "")

fig.s <- viz |> 
    count(s) |> 
    mutate(p = n/sum(n)) |> 
    ggplot(aes(x = s, y = p)) +
    geom_col() +
    scale_x_continuous(breaks = seq(0, 60, 15), minor_breaks = seq(0, 60, 5)) +
    scale_y_continuous(labels = scales::percent) +
    labs(x = "Seconds", y = "")

fig.wd <- viz |> 
    count(wd) |> 
    mutate(p = n/sum(n)) |> 
    ggplot(aes(x = wd, y = p)) +
    geom_col() +
    scale_y_continuous(labels = scales::percent) +
    labs(x = "Weekday", y = "")

fig.tod <- ((fig.wd + fig.h) / (fig.m + fig.s)) + plot_layout(heights = c(6, 3)) +
    plot_annotation(
        title = sprintf("@%s account was created %s ago.", user_name$name, format(round(user_name$time_on_twitter, 0), big.mark = ",")), 
        subtitle = sprintf("@%s posted on average %s tweets per day (%s without retweets).\nOver the last 100 days @%s posted %s tweets per day (%s without retweets).",
                           user_name$name,
                           user_name$tweets_per_day,
                           user_name$tweets_per_day_no_rt,
                           user_name$name,
                           user_name$tweets_per_day_100,
                           user_name$tweets_per_day_100_no_rt
        )
    )

ggsave(
    sprintf("Output/%s_time_of_day.png", user_name$name), 
    fig.tod, 
    scale = .8, 
    height = 3000, 
    width = 4500, 
    units = "px", 
    limitsize = F
)

df.hashtags <- tibble(ht = unlist(df$hashtags)) |> 
    mutate(ht = str_to_lower(ht)) |> 
    filter(!is.na(ht)) |> 
    count(ht, sort = T)

df.text_len <- df |> 
    filter(!is_retweet) |> 
    mutate(text_len = str_length(text)) |> 
    summarise(M = mean(text_len), SD = sd(text_len)/sqrt(n()))

fig.text_len <- df |> 
    filter(!is_retweet) |> 
    mutate(text_len = str_length(text)) |> 
    ggplot(aes(x = text_len)) +
    geom_density()

fig.counts <- df |> 
    select(created_at, retweet_count, reply_count, like_count, quote_count) |> 
    pivot_longer(-created_at) |>
    mutate(value = ifelse(value == 0, NA, value)) |> 
    ggplot(aes(x = created_at, y = value, color = name)) +
    geom_jitter() +
    scale_y_continuous(labels = scales::number) +
    scale_color_brewer(type = "qual") +
    facet_wrap("name", scales = "free") +
    labs(x = "Date", y = "Count")
    

