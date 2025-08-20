import json
from langchain_core.messages import AIMessage, ToolMessage
from langgraph.prebuilt import create_react_agent
from datetime import date
from src.types import PlanExecute
from src.utils import get_llm
from langgraph.config import get_stream_writer

class AgentExacutor:
    def __init__(self, tools, model="gpt-4o", store=None) -> None:
        self.store = store
        self.tools = tools
        self.llm = get_llm(model)
        

    async def execute_step(self, state: PlanExecute):
        writer = get_stream_writer()
        plan = state["plan"]
        plan_str = "\n".join(f"{step}" for i, step in enumerate(plan[1:]))
        task = plan[0]
        today = date.today()
        iso_year, iso_week, _ = today.isocalendar()
        year = today.year
        month = today.month
        day = today.day

        
        tools = self.tools
        if "查询" in task:
            tools = [self.tools[0]]
        elif "趋势" in task:
            tools = [self.tools[1]]
        elif "热力图" in task or "热图" in task:
            tools = [self.tools[2]]

        self.agent_executor = create_react_agent(
            self.llm,
            tools=tools,
            prompt="""你是一名数据分析助手。请遵循以下指导原则来完成任务： 

            - **响应语言**：
                请以中文回应。 

            - **数据准确性**： 
                - 不要虚构字段或表名。
                - 避免重复执行相同的SQL查询。
                - 需要引用图表时，请使用"见曲线图"、"见热力图"等表述，不得虚构图表来源。

            - **工作范围**：
                - 你必须只专注于完成当前任务，不要做任何其他 step 的工作。
            
            - **隐私**：
                - 不要透露表名，字段名，表原始数据等信息。
                - 不要透露任何关于工具调用的信息。
    
            /no_think""",
        )


        writer({"event": "execute_step_start", "task": task})
        task_formatted = f"""
- **已执行步骤**：
    {'\n'.join(state["past_steps"])} 
- **已执行工具调用**：
    {self.store["tool_calls"]} 
- **当前时间信息**： 
    - 年份：{year} 
    - 月份：{month:02d} 
    - 日期：{day:02d} 
- **请严格按照给定的任务和时间信息进行操作**：
    你必须只专注于完成当前的任务，不要做任何其他 step 的工作。当前任务执行完成后应立即退出。
- **你的当前任务是**：
    {task}。  
# 严格指令
**核心要求**：禁止执行以下任务
    <禁止执行>
    {plan_str}
    </禁止执行>
    /no_think"""
        agent_response = await self.agent_executor.ainvoke(
            {"messages": [("user", task_formatted)]}
        )
        response_content = agent_response["messages"][-1].content
        writer({"event": "execute_step_end", "response": response_content, "task": task})


        called_tools = {}
        for message in agent_response["messages"]:
            if isinstance(message, AIMessage):
                ai_message: AIMessage = message
                for tool_call in ai_message.tool_calls:
                    called_tools[tool_call['id']] = {
                        "tool_name": tool_call['name'],
                        "tool_args": tool_call['args'],
                        "result": None,
                    }
            if isinstance(message, ToolMessage):
                tool_message: ToolMessage = message
                if tool_message.tool_call_id in called_tools:
                    called_tools[tool_message.tool_call_id]['result'] = tool_message.content


        self.store["tool_calls"].extend(list(tool for tool in called_tools.values()))
        return {
            "past_steps": [task],
            "tool_calls": list(tool for tool in called_tools.values())
        }