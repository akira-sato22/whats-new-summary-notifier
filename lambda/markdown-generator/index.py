import boto3
import json
import os
import datetime
from datetime import timedelta
import traceback
from urllib.parse import urlparse

# 環境変数
DDB_TABLE_NAME = os.environ.get("DDB_TABLE_NAME", "AWSUpdatesRSSHistory")
S3_BUCKET_NAME = os.environ.get("S3_BUCKET_NAME")

# AWS クライアント
dynamo = boto3.resource("dynamodb")
table = dynamo.Table(DDB_TABLE_NAME)
s3 = boto3.client('s3')

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
        items = response['Items']
        
        # 追加のページがある場合は取得
        while 'LastEvaluatedKey' in response:
            response = table.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
            items.extend(response['Items'])
        
        # 日付でフィルタリング
        filtered_items = []
        for item in items:
            if 'pubtime' in item and item['pubtime'] >= start_date:
                filtered_items.append(item)
        
        # 日付順にソート
        filtered_items.sort(key=lambda x: x['pubtime'], reverse=False)
        
        return filtered_items
        
    except Exception as e:
        print(f"DynamoDBからのデータ取得中にエラーが発生しました: {str(e)}")
        traceback.print_exc()
        return []

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
        category = item.get('category', 'その他')
        if category not in categories:
            categories[category] = []
        categories[category].append(item)
    
    return categories

def generate_markdown(news_items, days=7):
    """
    ニュースのリストからMarkdownを生成する
    
    Args:
        news_items (list): ニュースのリスト
        days (int): 取得した日数
        
    Returns:
        str: 生成されたMarkdown
    """
    # 日付範囲
    end_date = datetime.datetime.now()
    start_date = end_date - timedelta(days=days)
    date_range = f"{start_date.strftime('%Y-%m-%d')} から {end_date.strftime('%Y-%m-%d')}"
    
    # Markdownのヘッダー
    markdown = f"# AWS 週間アップデート情報 {date_range}\n\n"
    markdown += f"過去{days}日間のAWS新機能・アップデート情報の週間サマリーです。\n\n"
    
    # カテゴリごとに分類
    categorized_news = categorize_news(news_items)
    
    # カテゴリ順にソート
    for category, items in sorted(categorized_news.items()):
        markdown += f"## {category}\n\n"
        
        for item in items:
            title = item.get('title', 'タイトルなし')
            url = item.get('url', '')
            pubtime = item.get('pubtime', '')
            
            # 日付を整形
            try:
                pub_date = datetime.datetime.fromisoformat(pubtime).strftime('%Y-%m-%d')
            except:
                pub_date = pubtime
            
            markdown += f"### [{title}]({url})\n"
            markdown += f"**公開日:** {pub_date}\n\n"
    
    return markdown

def save_to_s3(markdown_content, filename):
    """
    生成したMarkdownをS3に保存する
    
    Args:
        markdown_content (str): Markdownの内容
        filename (str): ファイル名
    """
    if not S3_BUCKET_NAME:
        print("S3_BUCKET_NAME環境変数が設定されていません")
        return False
    
    try:
        s3.put_object(
            Bucket=S3_BUCKET_NAME,
            Key=filename,
            Body=markdown_content.encode('utf-8'),
            ContentType='text/markdown; charset=utf-8'
        )
        return True
    except Exception as e:
        print(f"S3へのアップロード中にエラーが発生しました: {str(e)}")
        traceback.print_exc()
        return False

def handler(event, context):
    """
    Lambda関数のハンドラー
    
    Args:
        event (dict): イベントデータ
        context (object): Lambda実行コンテキスト
    """
    try:
        # イベントからパラメータを取得
        days = event.get('days', 7)
        
        # 現在の日付を取得
        now = datetime.datetime.now()
        # 週間サマリー用のファイル名
        filename = f"aws-weekly-updates-{now.strftime('%Y-%m-%d')}.md"
        
        # 過去N日間のニュースを取得
        news_items = get_news_from_last_n_days(days)
        
        if not news_items:
            print(f"過去{days}日間のニュースが見つかりませんでした")
            return {
                'statusCode': 404,
                'body': json.dumps({'message': f'過去{days}日間のニュースが見つかりませんでした'})
            }
        
        # Markdownを生成
        markdown_content = generate_markdown(news_items, days)
        
        # S3に保存
        if S3_BUCKET_NAME:
            success = save_to_s3(markdown_content, filename)
            if success:
                s3_url = f"s3://{S3_BUCKET_NAME}/{filename}"
                return {
                    'statusCode': 200,
                    'body': json.dumps({
                        'message': '週間サマリーのMarkdownファイルが正常に生成されました',
                        's3_location': s3_url,
                        'news_count': len(news_items)
                    })
                }
        
        # S3に保存しない場合、または保存に失敗した場合
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': '週間サマリーのMarkdownが正常に生成されましたが、S3には保存されていません',
                'markdown': markdown_content,
                'news_count': len(news_items)
            })
        }
    
    except Exception as e:
        print(f"エラーが発生しました: {str(e)}")
        traceback.print_exc()
        return {
            'statusCode': 500,
            'body': json.dumps({'error': str(e)})
        } 