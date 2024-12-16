import logging
import os
import vertexai
from flask import Flask, request
from vertexai.generative_models import GenerativeModel
from google.cloud import bigquery
from google.cloud import storage
from google.cloud.exceptions import NotFound
from dotenv import load_dotenv
from jinja2 import Environment, FileSystemLoader
from datetime import datetime

# # 設定環境變數指向服務帳戶金鑰
# os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "path/to/your/service-account-key.json"
# 自動引用環境變數檔案
load_dotenv(verbose=True)
# Init Flask
app = Flask(__name__)

# 初始化 BigQuery 客戶端
bq_client = bigquery.Client()

# 設置日誌格式
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# 初始化 Vertex AI
vertexai.init(project=os.getenv("PROJECT_ID"), location="us-central1")

@app.route("/", methods=["POST"])
def handle_request():
    """處理 HTTP 請求，觸發工作流"""
    # 1. 從 BigQuery 獲取數據
    bq_data = fetch_bq_data()
    # 2. 使用 LLM 生成數據
    for index, record in enumerate(bq_data):
        prompt = f"""
            使用以下資訊生成文本：
            標籤: {record['tags']}
            合規要求: {record['compliance']}
            題目: {record['prompt']}
        """
        generated_text = generate_llm_output(prompt).strip()
        print(f"===== generated {index} text =====")
        print(generated_text)
        print("================")
        update_llm_text(record['group_uuid'], generated_text)
    logging.info("Complete LLM Gen.")
    
    return "Completing GROUP_META LLM Generating."

def fetch_bq_data():
    """從 BigQuery 資料表中獲取數據"""
    query = f"""
    SELECT 
        group_uuid, 
        ARRAY_TO_STRING(ARRAY(SELECT tags FROM UNNEST(tags) AS tags LIMIT 10), ',') AS tags, 
        compliance, 
        prompt 
    FROM `{os.getenv("PROJECT_ID")}.{os.getenv("DATASET")}.GROUP_META`
    """
    query_job = bq_client.query(query)
    rows = query_job.result()
    return [{"group_uuid": row.group_uuid, "tags": row.tags, "compliance": row.compliance, "prompt": row.prompt,} for row in rows]

def generate_llm_output(prompt):
    """使用 GenerativeModel 生成數據"""
    gen_config= {
        "temperature": 0.5,          # 控制生成的隨機性
        "max_output_tokens": 1024,   # 限制生成文字的最大字數
        "top_k": 40,                 # 用於限制候選 token 數
        "top_p": 0.5                 # 依據累積概率選取候選 token
    }
    model = GenerativeModel(
        "gemini-1.5-flash-002",
        generation_config=gen_config
    )
    response = model.generate_content(prompt)
    return response.text

def update_llm_text(group_uuid, llm_text):
    """將 LLM 生成的數據回寫到 GROUP_META 表中"""
    query = f"""
    UPDATE `{os.getenv('PROJECT_ID')}.{os.getenv('DATASET')}.GROUP_META`
    SET marketing_copy = "{llm_text}"
    WHERE group_uuid = "{group_uuid}"
    """
    print(group_uuid)
    bq_client.query(query).result()
    
app.run(port=int(os.environ.get("PORT", 8080)),host='0.0.0.0',debug=True)
# if __name__ == "__main__": 
#     main()
