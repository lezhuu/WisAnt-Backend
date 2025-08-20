from flask import Flask, Response, jsonify, request, stream_with_context
import asyncio
import contextlib

import os
from dotenv import load_dotenv
from src.build_workflow import Workflow
from src.utils import exec_select_sql, get_connection, safe_json_dumps


load_dotenv()

app = Flask(__name__)


@app.get("/")
def index():
    return jsonify(status="ok", message="WisAnt POC Backend is running")


@app.get("/health")
def health():
    return jsonify(status="healthy")


@app.route("/get_sales_data", methods=["GET", "POST"])
async def get_sales_data():
    heatmap_data_id = request.args.get("heatmap_data_id")
    if not heatmap_data_id:
        data = request.get_json(silent=True) or {}
        heatmap_data_id = data.get("heatmap_data_id") or request.form.get("heatmap_data_id")
    if not heatmap_data_id:
        return jsonify(error="missing field: heatmap_data_id"), 400

    connection = await get_connection()
    async with connection.transaction():
        sql = await connection.fetchval(f"SELECT sql FROM public.sql_data WHERE id = $1", heatmap_data_id)
    result = await exec_select_sql(sql)
    return jsonify(result)


@app.post("/chat/stream")
def chat_stream():
    """
    A SSE stream endpoint for chat.

    request body:
    {
        "text": "user input",
    }

    response:
    stream
    """
    data = request.get_json(silent=True) or {}
    user_text = data.get("text")
    if not user_text:
        return jsonify(error="missing field: text"), 400


    model = os.getenv("MODEL_NAME", "qwen3:8b")
    # model = "gpt-4o"
    agent_executor_model = data.get("agent_executor_model", model)
    planner_model = data.get("planner_model", model)
    replanner_model = data.get("replanner_model", model)

    stream = Workflow(
        agent_executor_model=agent_executor_model,
        planner_model=planner_model,
        replanner_model=replanner_model
    ).astream_events(user_text)

    def sse_stream():
        # 直接同步生成器拉取异步流，按 astream_events 返回的三元组原样 JSON 化输出
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        ait = stream.__aiter__()
        try:
            while True:
                try:
                    result = loop.run_until_complete(ait.__anext__())
                except StopAsyncIteration:
                    break
                yield "data: " + safe_json_dumps(result) + "\n\n"
        except (GeneratorExit, ConnectionError, BrokenPipeError, OSError):
            # 客户端断开连接时，优雅退出
            return
        finally:
            # 关闭异步生成器以中止工作流 & 关闭事件循环
            with contextlib.suppress(Exception):
                loop.run_until_complete(ait.aclose())
            with contextlib.suppress(Exception):
                loop.close()

    headers = {
        "Cache-Control": "no-cache",
        "Connection": "keep-alive",
        "X-Accel-Buffering": "no",
    }
    return Response(stream_with_context(sse_stream()), mimetype="text/event-stream", headers=headers)

if __name__ == "__main__":
    port = int(os.getenv("PORT", "8080"))
    debug = os.getenv("FLASK_DEBUG", "0") == "1"
    app.run(host="0.0.0.0", port=port, debug=debug)


