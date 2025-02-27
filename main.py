import os
import random
import logging
import uvicorn
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from google.cloud import bigquery

# 自動引用環境變數檔案
load_dotenv(verbose=True, override=True)

# 設置日誌格式
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

app = FastAPI()

# 初始化 BigQuery 客戶端
bq_client = bigquery.Client()

# 定義 Pydantic 模型
class MarketingRequest(BaseModel):
    cust_uuid: str
    period: str

# @app.route('/', methods=['POST'])
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