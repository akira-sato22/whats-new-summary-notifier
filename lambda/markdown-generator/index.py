import boto3
import json
import os
import datetime
from datetime import timedelta
import traceback
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError

# 環境変数
DDB_TABLE_NAME = os.environ.get("DDB_TABLE_NAME", "AWSUpdatesRSSHistory")
S3_BUCKET_NAME = os.environ.get("S3_BUCKET_NAME")
SLACK_BOT_TOKEN_PARAMETER = os.environ.get("SLACK_BOT_TOKEN_PARAMETER")
SLACK_CHANNEL_ID_PARAMETER = os.environ.get("SLACK_CHANNEL_ID_PARAMETER")

# カテゴリグループの定義
CATEGORY_GROUPS = {
    "whats-new": {
        "display_name": "What's New",
        "description": "過去{days}日間のAWS What's New情報の週間サマリーです。",
        "emoji": "🎉",
        "file_suffix": "whats-new",
        "categories": ["Whats new"],  # 完全一致。空の場合はその他すべて
    },
    "others": {
        "display_name": "AWS Blog",
        "description": "過去{days}日間のAWS Blog情報の週間サマリーです。",
        "emoji": "📰",
        "file_suffix": "aws-blog",
        "categories": [],
    },
}

# AWS クライアント
dynamo = boto3.resource("dynamodb")
table = dynamo.Table(DDB_TABLE_NAME)
s3 = boto3.client("s3")
ssm = boto3.client("ssm")


def get_parameter_value(parameter_name):
    """
    Parameter Storeからパラメータを取得する

    Args:
        parameter_name: パラメータ名

    Returns:
        str: パラメータの値
    """
    try:
        response = ssm.get_parameter(Name=parameter_name, WithDecryption=True)
        return response["Parameter"]["Value"]
    except Exception as e:
        print(f"パラメータの取得に失敗しました: {str(e)}")
        return None


def get_news_from_last_n_days(days=7):
    """
    過去N日間のニュースをDynamoDBから取得する

    Args:
        days (int): 取得する日数（デフォルト: 7日）

    Returns:
        list: ニュースのリスト
    """
    current_date = datetime.datetime.now()
    start_date = (current_date - timedelta(days=days)).isoformat()

    try:
        # GSIがあると仮定して日付でのクエリを行う
        # この例では簡略化のため、すべてのアイテムをスキャンして日付でフィルタリング
        response = table.scan()
        items = response["Items"]

        # 追加のページがある場合は取得
        while "LastEvaluatedKey" in response:
            response = table.scan(ExclusiveStartKey=response["LastEvaluatedKey"])
            items.extend(response["Items"])

        # 日付でフィルタリング
        filtered_items = [
            item
            for item in items
            if "pubtime" in item and item["pubtime"] >= start_date
        ]

        # 日付順にソート
        filtered_items.sort(key=lambda x: x["pubtime"], reverse=False)

        return filtered_items

    except Exception as e:
        print(f"DynamoDBからのデータ取得中にエラーが発生しました: {str(e)}")
        traceback.print_exc()
        return []


def group_news_by_category_type(news_items):
    """
    ニュースを"whats-new"と"others"の2つのグループに分類する

    Args:
        news_items (list): ニュースのリスト

    Returns:
        dict: グループごとに分類されたニュース
    """
    groups = {group_name: [] for group_name in CATEGORY_GROUPS}

    for item in news_items:
        category = item.get("category", "その他")

        # "What's new"カテゴリかどうかチェック
        if category in CATEGORY_GROUPS["whats-new"]["categories"]:
            groups["whats-new"].append(item)
        else:
            groups["others"].append(item)

    return groups


def categorize_news(news_items):
    """
    ニュースをカテゴリごとに分類する

    Args:
        news_items (list): ニュースのリスト

    Returns:
        dict: カテゴリごとに分類されたニュース
    """
    categories = {}

    for item in news_items:
        category = item.get("category", "その他")
        if category not in categories:
            categories[category] = []
        categories[category].append(item)

    return categories


def generate_markdown(news_items, group_name, days=7):
    """
    ニュースのリストからMarkdownを生成する（カテゴリグループ用）

    Args:
        news_items (list): ニュースのリスト
        group_name (str): グループ名（"whats-new" または "others"）
        days (int): 取得した日数

    Returns:
        str: 生成されたMarkdown
    """
    # 日付範囲
    end_date = datetime.datetime.now()
    start_date = end_date - timedelta(days=days)
    date_range = (
        f"{start_date.strftime('%Y-%m-%d')} から {end_date.strftime('%Y-%m-%d')}"
    )

    group_info = CATEGORY_GROUPS[group_name]

    # Markdownのヘッダー
    markdown = (
        f"# AWS 週間アップデート情報 - {group_info['display_name']} ({date_range})\n\n"
    )
    markdown += group_info["description"].format(days=days) + "\n\n"

    # カテゴリごとに分類
    categorized_news = categorize_news(news_items)

    # カテゴリ順にソート
    for category, items in sorted(categorized_news.items()):
        markdown += f"## {category}\n\n"

        for item in items:
            title = item.get("title", "タイトルなし")
            url = item.get("url", "")
            pubtime = item.get("pubtime", "")
            detail = item.get("detail", "")

            # 日付を整形
            try:
                pub_date = datetime.datetime.fromisoformat(pubtime).strftime(
                    "%Y-%m-%d"
                )
            except (ValueError, TypeError):
                pub_date = pubtime

            markdown += f"### [{title}]({url})\n"
            markdown += f"**公開日:** {pub_date}\n\n"

            # 詳細情報が存在する場合は追加
            if detail:
                markdown += f"{detail}\n\n"

    return markdown


def save_to_s3(markdown_content, group_name, filename):
    """
    生成したMarkdownをS3に保存する（グループ別ディレクトリ構造）

    Args:
        markdown_content (str): Markdownの内容
        group_name (str): グループ名（"whats-new" または "others"）
        filename (str): ファイル名
    """
    if not S3_BUCKET_NAME:
        print("S3_BUCKET_NAME環境変数が設定されていません")
        return False

    # グループ別のディレクトリ構造でファイルパスを構築
    group_filename = f"weekly-summaries/{group_name}/{filename}"

    try:
        s3.put_object(
            Bucket=S3_BUCKET_NAME,
            Key=group_filename,
            Body=markdown_content.encode("utf-8"),
            ContentType="text/markdown; charset=utf-8",
        )
        return True
    except Exception as e:
        print(f"S3へのアップロード中にエラーが発生しました: {str(e)}")
        traceback.print_exc()
        return False


def push_to_slack(markdown_content, news_count, group_name):
    """
    Slackにメッセージとファイルを送信する（グループ別）

    Args:
        markdown_content (str): Markdownの内容
        news_count (int): ニュース数
        group_name (str): グループ名（"whats-new" または "others"）
    """
    try:
        # SlackチャンネルIDを取得
        channel_id = get_parameter_value(SLACK_CHANNEL_ID_PARAMETER)
        if not channel_id:
            print("SLACK_CHANNEL_IDの取得に失敗しました")
            return False

        # Slackトークンを取得
        slack_token = get_parameter_value(SLACK_BOT_TOKEN_PARAMETER)
        if not slack_token:
            print("SLACK_BOT_TOKENの取得に失敗しました")
            return False

        # Slack SDKクライアントを初期化
        client = WebClient(token=slack_token)

        # 現在の日付を取得してファイル名を生成
        now = datetime.datetime.now()

        group_info = CATEGORY_GROUPS[group_name]
        file_name = f"{now.strftime('%Y%m%d')}-{group_info['file_suffix']}-updates.md"

        # 初期メッセージを作成
        initial_message = f"AWS 週間アップデート情報 - {group_info['display_name']} {group_info['emoji']}\n更新件数: {news_count}件"

        # ファイルをアップロード
        try:
            response = client.files_upload_v2(
                channel=channel_id,
                content=markdown_content,
                filename=file_name,
                title=file_name,
                initial_comment=initial_message,
            )
            if not response["ok"]:
                print(f"Slackへのファイルアップロードに失敗しました: {response}")
                return False
            return True
        except SlackApiError as e:
            print(f"Slackへのファイルアップロードに失敗しました: {e.response['error']}")
            return False

    except Exception as e:
        print(f"Slackへの送信中にエラーが発生しました: {str(e)}")
        traceback.print_exc()
        return False


def handler(event, context):
    """
    Lambda関数のハンドラー（カテゴリグループ別処理）

    Args:
        event (dict): イベントデータ
        context (object): Lambda実行コンテキスト
    """
    try:
        # イベントからパラメータを取得
        days = event.get("days", 7)

        # 現在の日付を取得
        now = datetime.datetime.now()

        # 過去N日間のニュースを取得
        news_items = get_news_from_last_n_days(days)

        if not news_items:
            print(f"過去{days}日間のニュースが見つかりませんでした")
            return {
                "statusCode": 404,
                "body": json.dumps(
                    {"message": f"過去{days}日間のニュースが見つかりませんでした"}
                ),
            }

        # ニュースをカテゴリグループに分類
        grouped_news = group_news_by_category_type(news_items)

        response_data = {
            "message": "週間サマリーのMarkdownファイルが正常に生成されました",
            "groups": {},
        }

        # 各グループについて処理を実行
        for group_name, group_items in grouped_news.items():
            if not group_items:  # グループにアイテムがない場合はスキップ
                print(f"{group_name}グループにニュースがありません")
                continue

            print(f"{group_name}グループの処理を開始: {len(group_items)}件")

            # グループ別のファイル名を生成
            group_suffix = CATEGORY_GROUPS[group_name]["file_suffix"]
            filename = f"{now.strftime('%Y/%m/%d')}/{now.strftime('%Y%m%d')}-{group_suffix}-updates.md"

            # Markdownを生成
            markdown_content = generate_markdown(group_items, group_name, days)

            group_response = {"news_count": len(group_items), "filename": filename}

            # S3に保存
            if S3_BUCKET_NAME:
                success = save_to_s3(markdown_content, group_name, filename)
                if success:
                    s3_url = f"s3://{S3_BUCKET_NAME}/weekly-summaries/{group_name}/{filename}"
                    group_response["s3_location"] = s3_url

                    # Slackに通知を送信
                    slack_success = push_to_slack(
                        markdown_content=markdown_content,
                        news_count=len(group_items),
                        group_name=group_name,
                    )
                    group_response["slack_notification"] = (
                        "success" if slack_success else "failed"
                    )
                else:
                    group_response["s3_save"] = "failed"
                    group_response["slack_notification"] = "skipped"

            response_data["groups"][group_name] = group_response

        return {"statusCode": 200, "body": json.dumps(response_data)}

    except Exception as e:
        print(f"エラーが発生しました: {str(e)}")
        traceback.print_exc()
        return {"statusCode": 500, "body": json.dumps({"error": str(e)})}
