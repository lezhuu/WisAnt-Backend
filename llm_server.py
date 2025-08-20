from flask import Flask, Response, jsonify, request, stream_with_context
import json
import os
from dotenv import load_dotenv
from openai import OpenAI
import re

load_dotenv()


client = OpenAI(
    base_url="https://api.pandalla.ai/v1",  # 仅需修改这一行
    api_key=os.getenv("PANDALLA_AI")
)



load_dotenv()

app = Flask(__name__)


@app.post("/chat/completions")
@app.post("/v1/chat/completions")
def chat():
    # 注意：如果视图函数包含 yield，函数会变为生成器，导致请求上下文在迭代时不可用。
    # 因此我们将流式输出移入内部生成器，并使用 stream_with_context 包装。
    data = request.get_json(silent=True) or {}
    presence_penalty = data.get("presence_penalty", 0)
    messages = data.get("messages", [])
    model = data.get("model", "gpt-4o-mini")
    max_tokens = data.get("max_tokens", 1024)
    temperature = data.get("temperature", 0.8)
    top_p = data.get("top_p", 1)
    stream = data.get("stream", False)
    response_format = data.get("response_format", None)
    tools = data.get("tools", None)
    # 供应商自定义扩展参数
    extra_body = data.get("extra_body", {}) if isinstance(data.get("extra_body", {}), dict) else {}

    
    

    # if response_format is not None:
    #     if response_format.get("type") == "json_schema":
    #         extra_body["guided_json"] = response_format['json_schema']
    #         extra_body["enable_thinking"] = False

    # print("="*80)
    # print(extra_body)

    # # 当使用 JSON 输出约束时，部分提供方要求在提示中出现 "json" 字样
    # # 这里在检测到 response_format 为 json_object/json_schema 且消息中未出现 "json" 时，追加一条系统提示
    # try:
    #     rf_type = (response_format or {}).get("type") if isinstance(response_format, dict) else None
    # except Exception:
    #     rf_type = None

    # def _messages_contain_json_word(msgs):
    #     try:
    #         for m in msgs or []:
    #             content = m.get("content") if isinstance(m, dict) else None
    #             if isinstance(content, str) and re.search(r"json", content, flags=re.IGNORECASE):
    #                 return True
    #     except Exception:
    #         pass
    #     return False

    # if rf_type in {"json_object", "json_schema"} and not _messages_contain_json_word(messages):
    #     messages = list(messages or [])
    #     messages.append({
    #         "role": "system",
    #         "content": (
    #             "重要：仅以json格式输出，且必须是一个合法的单一JSON对象；不要包含除JSON之外的任何文本。"
    #         ),
    #     })

    # # 若 JSON 模式启用，强制关闭供应商的 thinking 模式以避免冲突
    # if rf_type in {"json_object", "json_schema"}:
    #     extra_body = dict(extra_body)
    #     # 常见供应商开关：enable_thinking / enable_reasoning（稳妥起见同时关闭）
    #     extra_body.setdefault("enable_thinking", False)
    #     extra_body.setdefault("enable_reasoning", False)
    #     extra_body.setdefault("guided_json", response_format)

    # print
    # 调用下游兼容 OpenAI 的服务
    response = client.chat.completions.create(
        model=model,
        messages=messages,
        max_tokens=max_tokens,
        temperature=temperature,
        top_p=top_p,
        presence_penalty=presence_penalty,
        stream=stream,
        response_format=response_format,
        tools=tools,
        extra_body=extra_body
    )



    if stream:
        def sse_stream():
            for chunk in response:
                print(chunk)
                # 直接透传标准 OpenAI ChatCompletionChunk 的 JSON
                if hasattr(chunk, "model_dump_json"):
                    payload = chunk.model_dump_json()
                elif hasattr(chunk, "model_dump"):
                    payload = json.dumps(chunk.model_dump())
                else:
                    # 最后兜底：尝试通过 __dict__ 序列化
                    payload = json.dumps(getattr(chunk, "__dict__", {}))
                yield f"data: {payload}\n\n"
            # 结束标记
            yield "data: [DONE]\n\n"

        headers = {
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        }
        return Response(stream_with_context(sse_stream()), mimetype="text/event-stream", headers=headers)
    else:
        # 非流式：返回完整的 OpenAI ChatCompletion JSON 结构
        if hasattr(response, "model_dump"):
            data_obj = response.model_dump()
        elif hasattr(response, "model_dump_json"):
            data_obj = json.loads(response.model_dump_json())
        elif hasattr(response, "dict"):
            data_obj = response.dict()  # type: ignore[attr-defined]
        else:
            # 兜底
            data_obj = getattr(response, "__dict__", response)

        return jsonify(data_obj)

if __name__ == "__main__":
    port = int(os.getenv("LLM_SERVER_PORT", "8083"))
    debug = os.getenv("FLASK_DEBUG", "0") == "1"
    app.run(host="0.0.0.0", port=port, debug=debug)


