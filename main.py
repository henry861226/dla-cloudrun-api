import os
import random
import logging
import uvicorn
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from google.cloud import bigquery
from google.api_core.client_options import ClientOptions
from google.auth.credentials import AnonymousCredentials

# 自動引用環境變數檔案
load_dotenv(verbose=True, override=True)

# 設置日誌格式
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

app = FastAPI()

# # 開發用：bq emulator
# client_options = ClientOptions(api_endpoint="http://0.0.0.0:9050")
# bq_client = bigquery.Client(
#   "dla-poc-447003",
#   client_options=client_options,
#   credentials=AnonymousCredentials(),
# )
# # @app.route('/test', methods=['POST'])
# @app.post('/test')
# async def test():
#     try:
#         query = f"""
#             SELECT * FROM `{os.getenv('PROJECT_ID')}.{os.getenv('DATASET')}.test_tb`
#         """
#         query_job = bq_client.query(query)
#         # for row in query_job:
#         #     print(f"{row['structarr']}")
#         # return "test", 200
#         results = [dict(row) for row in query_job]  # 轉換為字典列表
#         return results
#     except Exception as e:
#         # return jsonify({"error": str(e)}), 500
#         raise HTTPException(status_code=500, detail=str(e))
# 初始化 BigQuery 客戶端
bq_client = bigquery.Client()

# 定義 Pydantic 模型
class MarketingRequest(BaseModel):
    cust_uuid: str

# @app.route('/', methods=['POST'])
@app.post('/')
async def get_marketing_copy(data: MarketingRequest):
    try:
        # 解析 POST 請求中的 cust_uuid
        cust_uuid = data.cust_uuid
        if not cust_uuid:
            raise HTTPException(status_code=400, detail="cust_uuid is required")
        
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
        query_results = bq_client.query(multi_query)
        results = list(query_results)
        # 如果结果為空，返回默認信息或處理
        if not results:
            return f"No marketing_copy found for cust_uuid={cust_uuid}."
        # 隨機選一條文案
        random_row = random.choice(results)
        response = {
            "客戶 ID": cust_uuid,
            "行銷文案": random_row["marketing_copy"],
            "檔期": random_row["period"]
        }
        return response

    except Exception as e:
        # return jsonify({"error": str(e)}), 500
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