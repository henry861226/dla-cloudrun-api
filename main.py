import os
import json
import random
import logging
from dotenv import load_dotenv
from flask import Flask, request, jsonify, Response
from google.cloud import bigquery

# 自動引用環境變數檔案
load_dotenv(verbose=True, override=True)

# 設置日誌格式
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

app = Flask(__name__)

# 初始化 BigQuery 客戶端
bq_client = bigquery.Client()

@app.route('/', methods=['POST'])
def get_marketing_copy():
    try:
        # 解析 POST 請求中的 cust_uuid
        data = request.get_json()
        cust_uuid = data.get("cust_uuid")
        if not cust_uuid:
            return jsonify({"error": "cust_uuid is required"}), 400
        
        # 取狀態已完成的最新日期
        enable_date = check_sts_table()

        # 查詢
        multi_query = f"""
            SELECT 
                marketing_copy,
                period
            FROM
                `{os.getenv('PROJECT_ID')}.{os.getenv('DATASET')}.CUST_GRP_MAP_{enable_date}` c
            JOIN
                `{os.getenv('PROJECT_ID')}.{os.getenv('DATASET')}.ACTIVE_COPY` g
            ON 
                c.group_uuid = g.group_uuid
            WHERE
                c.cust_uuid = "{cust_uuid}"
        """
        test_query = bq_client.query(multi_query)
        results = list(test_query)
        # 如果结果為空，返回默認信息或處理
        if not results:
            return f"No marketing_copy found for cust_uuid={cust_uuid}."
        # 隨機選一條文案，並將結果轉為 JSON
        response = []
        random_row = random.choice(results)
        response.append({
            "客戶 ID": cust_uuid,
            "行銷文案": random_row["marketing_copy"],
            "檔期": random_row["period"]
        })
        response_json = json.dumps(response, ensure_ascii=False)
        return Response(response_json, content_type="application/json; charset=utf-8")

    except Exception as e:
        return jsonify({"error": str(e)}), 500

# 取最新狀態已完成的日期
def check_sts_table():
    query= f"""
        SELECT exec_date FROM `{os.getenv('PROJECT_ID')}.{os.getenv('DATASET')}.JOB_STS`
        WHERE status=true
        ORDER BY exec_date DESC
        LIMIT 1
    """
    result = next(bq_client.query(query).result(), None)
    exec_date = result.exec_date if result else None
    logging.info(f"Latest Enable Table: CUST_GRP_MAP_{exec_date}")
    return exec_date

if __name__ == "__main__":
    # 確保你有正確設置 GOOGLE_APPLICATION_CREDENTIALS 環境變數
    app.run(host="0.0.0.0", port=8080)
