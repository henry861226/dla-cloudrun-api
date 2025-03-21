import os
import random
import logging
import uvicorn
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from google.cloud import bigquery
import json
from google.cloud import firestore
from datetime import datetime

# 自動引用環境變數檔案
load_dotenv(verbose=True, override=True)

# 設置日誌格式
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

app = FastAPI()

# 初始化 BigQuery 客戶端
bq_client = bigquery.Client()

# 初始化 Firestore 客戶端
ft = firestore.Client(database="trccubcardpoc")

# 定義 Pydantic 模型
class MarketingRequest(BaseModel):
    cust_uuid: str
    period: str

@app.get('/test_gke')
def test_gke():
    logging.info("API Success.")
    return "OKOK", 200
    


# 格式化資料
def format_member_feedback_doc(doc):
    formatted_doc = {
        "TXN_DATE": datetime.strptime(doc["TXN_DATE"]["$date"], "%Y-%m-%dT%H:%M:%S.%fZ") if "$date" in doc["TXN_DATE"] else None,
        "BEF_AVAILABLE_BONUS_POINT": doc["BEF_AVAILABLE_BONUS_POINT"],
        "CHANGE_BONUS_POINT": doc["CHANGE_BONUS_POINT"],
        "AFT_AVAILABLE_BONUS_POINT": doc["AFT_AVAILABLE_BONUS_POINT"],
        "TXN_TYPE": doc["TXN_TYPE"],
        "MEMBER": doc["MEMBER"]
    }
    return formatted_doc
# 格式化資料
def format_feedback_doc(doc):
    formatted_doc = {
        "TXN_DATE": datetime.strptime(doc["TXN_DATE"]["$date"], "%Y-%m-%dT%H:%M:%S.%fZ") if "$date" in doc["TXN_DATE"] else None,
        "FEEDBACK_DATE": datetime.strptime(doc["FEEDBACK_DATE"]["$date"], "%Y-%m-%dT%H:%M:%S.%fZ") if "$date" in doc["FEEDBACK_DATE"] else None,
        "GROUP_DESC": doc["GROUP_DESC"],
        "FEEDBACK_DESC": doc["FEEDBACK_DESC"],
        "FEEDBACK_POINT": doc["FEEDBACK_POINT"],
        "NTD_TXN_AMT": doc["NTD_TXN_AMT"],
        "MERCHANT_NAME": doc["MERCHANT_NAME"].strip(),
        "MEMBER": doc["MEMBER"]
    }
    return formatted_doc

@app.post('/batch-ft')
def import_firestore_json():
    # 指定要存入 Firestore 的集合名稱
    collection_name = "cubcard_feedback"

    # 讀取 JSON 檔案
    json_file_path = "../../外部卡友/cathaybk-chatbot-data-snapshot-20250307/mongo_cathay-poc-data_FEEDBACK.json"  # 替換成你的 JSON 檔案路徑
    with open(json_file_path, "r", encoding="utf-8") as f:
        data = json.load(f)
    # 批次寫入 Firestore
    batch = ft.batch()
    collection_ref = ft.collection(collection_name)

    for doc in data:
        doc_id = doc["_id"]["$oid"]  # 使用 MongoDB 的 ObjectId 作為 Firestore 文件 ID
        formatted_doc = format_feedback_doc(doc)
        batch.set(collection_ref.document(doc_id), formatted_doc)

    # 提交批次操作
    batch.commit()

    print(f"成功將 {len(data)} 筆資料匯入 Firestore!")

@app.post('/')
async def get_marketing_copy(data: MarketingRequest):
    try:
        # 解析 POST 請求中的 cust_uuid & period
        cust_uuid = data.cust_uuid
        period = data.period
        if not cust_uuid:
            logging.WARN("cust_uuid is required")
            raise HTTPException(status_code=400, detail="cust_uuid is required")
        if not period:
            logging.WARN("period is required")
            raise HTTPException(status_code=400, detail="period is required") 
        # 取狀態已完成的最新日期
        enable_date = check_sts_table()

        # 查詢
        multi_query = f"""
            SELECT 
                uuid,
                marketing_copy,
                period
            FROM
                `{os.getenv('PROJECT_ID')}.{os.getenv('DATASET')}.CUST_GRP_MAP_{enable_date}` c
            JOIN
                `{os.getenv('PROJECT_ID')}.{os.getenv('DATASET')}.ACTIVE_COPY` g
            ON 
                c.group_uuid = g.group_uuid AND g.period="{period}"
            WHERE
                c.cust_uuid = "{cust_uuid}"
        """
        query_results = bq_client.query(multi_query)
        results = list(query_results)
        # 如果结果為空，返回默認信息或處理
        if not results:
            logging.WARN(f"No marketing_copy found for cust_uuid={cust_uuid}.")
            return f"No marketing_copy found for cust_uuid={cust_uuid}."
        # 隨機選一條文案
        random_row = random.choice(results)
        response = {
            "客戶 ID": cust_uuid,
            "行銷文案UUID": random_row["uuid"],
            "行銷文案": random_row["marketing_copy"],
            "檔期": random_row["period"]
        }
        return response

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

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

# FastAPI Start
if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8080)