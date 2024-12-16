import os
import json
from datetime import datetime
from dotenv import load_dotenv
from flask import Flask, request, jsonify, Response
from google.cloud import bigquery

# 自動引用環境變數檔案
load_dotenv(verbose=True)
app = Flask(__name__)

# 初始化 BigQuery 客戶端
bq_client = bigquery.Client()

current_time= datetime.now().strftime("%Y%m%d")

@app.route('/', methods=['POST'])
def get_marketing_copy():
    try:
        # 解析 POST 請求中的 cust_uuid
        data = request.get_json()
        cust_uuid = data.get("cust_uuid")
        if not cust_uuid:
            return jsonify({"error": "cust_uuid is required"}), 400
        # 查詢
        query = f"""
            SELECT
            c.cust_uuid,
            g.marketing_copy
            FROM
            `{os.getenv('PROJECT_ID')}.{os.getenv('DATASET')}.CUST_GRP_MAP_{current_time}` c
            JOIN
            `{os.getenv('PROJECT_ID')}.{os.getenv('DATASET')}.GROUP_META` g
            ON
            c.group_uuid = g.group_uuid
            WHERE
            c.cust_uuid = "{cust_uuid}";
        """

        # 執行查詢
        query_job = bq_client.query(query)
        results = query_job.result()

        # 將結果轉為 JSON
        response = []
        for row in results:
            response.append({
                "客戶 ID": row.cust_uuid,
                "行銷文案": row.marketing_copy
            })
        response_json = json.dumps(response, ensure_ascii=False)
        return Response(response_json, content_type="application/json; charset=utf-8")

    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == "__main__":
    # 確保你有正確設置 GOOGLE_APPLICATION_CREDENTIALS 環境變數
    app.run(host="0.0.0.0", port=8080)
