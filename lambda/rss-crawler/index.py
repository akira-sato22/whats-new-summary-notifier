# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

import boto3
import datetime
import feedparser
import json
import os
import dateutil.parser
from botocore.exceptions import ClientError

DDB_TABLE_NAME = os.environ["DDB_TABLE_NAME"]
DDB_ITEM_TTL_DAYS = 30
RECENT_DAYS = 7

dynamo = boto3.resource("dynamodb")
table = dynamo.Table(DDB_TABLE_NAME)


def recently_published(pubdate):
    """Check if the publication date is recent

    Args:
        pubdate (str): The publication date and time
    """

    now = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None)
    elapsed_time = now - str2datetime(pubdate)
    print(elapsed_time)
    return elapsed_time.days <= RECENT_DAYS


def str2datetime(time_str):
    """Convert the date format from the blog text to a naive UTC datetime

    Args:
        time_str (str): The date and time string, e.g., "Tue, 20 Sep 2022 16:05:47 +0000"
    """

    parsed = dateutil.parser.parse(time_str)
    if parsed.tzinfo is None:
        return parsed
    return parsed.astimezone(datetime.timezone.utc).replace(tzinfo=None)


def write_to_table(link, title, category, pubtime, notifier_name):
    """Write a blog post to DynamoDB unless it already exists

    Args:
        link (str): The URL of the blog post
        title (str): The title of the blog post
        category (str): The category of the blog post
        pubtime (str): The publication date of the blog post
        notifier_name (str): The name of the notifier configuration
    """

    ttl = int(
        (
            datetime.datetime.now() + datetime.timedelta(days=DDB_ITEM_TTL_DAYS)
        ).timestamp()
    )
    item = {
        "url": link,
        "notifier_name": notifier_name,
        "title": title,
        "category": category,
        "pubtime": pubtime,
        "ttl": ttl,
    }

    try:
        # 既存のエントリーは条件付き書き込みでスキップ（get_item + put_item のレースを回避）
        table.put_item(
            Item=item,
            ConditionExpression="attribute_not_exists(#url)",
            ExpressionAttributeNames={"#url": "url"},
        )
        print(item)
    except ClientError as e:
        if e.response["Error"]["Code"] == "ConditionalCheckFailedException":
            print(f"既存のエントリーをスキップします: {title}")
        else:
            print(f"エラーが発生しました: {str(e)}")


def add_blog(rss_name, entries, notifier_name):
    """Add blog posts

    Args:
        rss_name (str): The category of the blog (RSS unit)
        entries (List): The list of blog posts
        notifier_name (str): The name of the notifier configuration
    """

    for entry in entries:
        if recently_published(entry["published"]):
            write_to_table(
                entry["link"],
                entry["title"],
                rss_name,
                str2datetime(entry["published"]).isoformat(),
                notifier_name,
            )
        else:
            print("Old blog entry. skip: " + entry["title"])


def handler(event, context):

    notifier_name = event["notifierName"]
    notifier = event["notifier"]

    rss_urls = notifier["rssUrl"]
    for rss_name, rss_url in rss_urls.items():
        rss_result = feedparser.parse(rss_url)
        print(json.dumps(rss_result))
        print("RSS updated " + rss_result["feed"]["updated"])
        if not recently_published(rss_result["feed"]["updated"]):
            # Do not process RSS feeds that have not been updated for a certain period of time.
            # If you want to retrieve from the past, change this number of days and re-import.
            print("Skip RSS " + rss_name)
            continue
        add_blog(rss_name, rss_result["entries"], notifier_name)
