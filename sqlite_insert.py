import json
import boto3
import sqlite3
import os

# バケット名,オブジェクト名
BUCKET_NAME = 'python-get-object-0001'
OBJECT_KEY_NAME = 'mydatabase.db'
TMP_FILE_PATH = '/tmp/database.db'
s3 = boto3.resource('s3')

def lambda_handler(event, context):
    
    learned_content = ""
    category = ""
    learned_time = ""

    try:
        learned_content = event.get('queryStringParameters').get('learned_content')
        category = event.get('queryStringParameters').get('category')
        learned_time = event.get('queryStringParameters').get('learned_time')


        # S3からSQLiteのデータベースファイルを取得する
        bucket = s3.Bucket(BUCKET_NAME)
        obj = bucket.Object(OBJECT_KEY_NAME)

        # SQLiteデータベースファイルの存在確認
        if has_s3_object(BUCKET_NAME, OBJECT_KEY_NAME):
            response = obj.get()
            body = response['Body'].read()
            
            # SQLiteのデータベースファイルをLambdaの/tempに配置する
            with open(TMP_FILE_PATH, 'wb') as file:
                file.write(body)

        conn = sqlite3.connect('/tmp/database.db')
        cursor = conn.cursor()
        cursor.execute('''CREATE TABLE IF NOT EXISTS memo (id INTEGER PRIMARY KEY AUTOINCREMENT, learned_content TEXT, category INTEGER, learned_time INTEGER)''')
        sql = "INSERT INTO memo (learned_content, category, learned_time) VALUES (?, ?, ?)"
        cursor.execute(sql, (learned_content, category, learned_time))

        conn.commit()
        conn.close()

        # 操作を終えたデータベースファイルをS3に保存する
        with open(TMP_FILE_PATH, 'rb') as file:
            data = file.read()
            s3.Bucket(BUCKET_NAME).put_object(Key=OBJECT_KEY_NAME, Body=data)

    except Exception as e:
        response = {
            'statusCode': 400,
            'body': json.dumps({'error': 'Invalid input_data'}),
            'headers': {
                'Content-Type': 'application/json'
            }
        }
        return response

    response = {
        'statusCode': 201,
        'body': json.dumps({'message': 'Success'}),
        'headers': {
            'Content-Type': 'application/json'
        }
    }
    
    return response

# 対象のファイルがS3に存在するか確認する
def has_s3_object(bucket, path):
    try:
        s3.meta.client.head_object(Bucket=bucket, Key=path)
        return True
    except:
        return False
