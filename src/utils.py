import asyncpg
from langchain_openai import ChatOpenAI
import os
from langchain_core.messages import AIMessageChunk, HumanMessage, SystemMessage
# from langchain_community.chat_models import ChatTongyi
from langchain_core.tools import Tool
import json
from langchain_community.llms import Tongyi
from openai.types.shared import reasoning, reasoning_effort

import dotenv

dotenv.load_dotenv()

# def get_llm(model: str = "gpt-4o"):
#     return ChatTongyi(
#         base_url=os.getenv("LOCAL_LLM_BASE_URL"), 
#         model=model,
#         api_key=os.getenv("LOCAL_LLM_KEY"),
#         temperature=0,
#     )

def get_llm(model: str = "gpt-4o"):
    return ChatOpenAI(
        base_url=os.getenv("LOCAL_LLM_BASE_URL"), 
        model=model,
        api_key=os.getenv("LOCAL_LLM_KEY"),
        temperature=0,
    )


def serialize_message(obj):
    """将 LangChain 消息对象转换为可序列化的字典"""
    return dict(obj)


def safe_json_dumps(obj, ensure_ascii=False):
    """安全地序列化对象，处理不可序列化的类型"""
    def default_serializer(o):
        return serialize_message(o)
    
    return json.dumps(obj, ensure_ascii=ensure_ascii, default=default_serializer)



def get_message_type(message: dict):
    message_type = "tool" if len(message[0]["additional_kwargs"]) > 0 else "message"
    node = message[1]['langgraph_node']
    return {
        "message_type": message_type,
        "node": node,
    }

async def get_connection():
    return await asyncpg.connect(
        # host="wisant-poc-db",
        host=os.getenv("POSTGRES_HOST"),
        port=os.getenv("POSTGRES_PORT"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
        database=os.getenv("POSTGRES_DB")
    )

async def exec_insert_sql(sql: str, *params):
    conn = await get_connection()
    try:
        async with conn.transaction():
            result = await conn.fetchval(sql, *params)
    finally:
        await conn.close()

    # 返回插入id
    return result


async def exec_select_sql(sql: str):
    conn = await get_connection()

    try:
        async with conn.transaction():
            result = await conn.fetch(sql)
    finally:
        await conn.close()

    return [dict(record) for record in result]


