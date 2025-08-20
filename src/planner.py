
from langchain_core.prompts import ChatPromptTemplate
from langgraph.config import get_stream_writer

from src.types import Plan, PlanExecute
from src.utils import get_llm


class Planner:
    def __init__(self, model="gpt-4o", store=None) -> None:
        self.store = store
        planner_prompt = ChatPromptTemplate.from_messages(
            [
                (
                    "system",
                    """根据给定的目标，制定一个简单且逐步的计划。该计划应包含一系列明确的任务，如果正确执行这些任务将得到最终答案。请确保不添加任何多余的步骤，并且每个步骤都包含了所有必要的信息，不要跳过任何步骤。最终步骤的结果应该是最终答案。请遵循以下限制条件：

- 数据库仅覆盖2024年7月至2025年7月的数据；
- 计划中必须有一个步骤用于绘制详细的报告数据热图；
- 在适当的情况下，可以绘制趋势图表（可选：零次、一次或多次）；
- 可以查询`sales_data`表中的聚合数据来辅助生成报告；
- 最终输出应是一份包含文本和视觉元素的报告；
- 不要在计划中透露任何关于函数调用或表访问的信息；
- 请用中文回应。

拆分出的每一步骤都必须**明确**、**单步**、**可执行**。例如：
- 使用聚合函数查询从 xxx 到 xxx 的销售数据。
- 使用从 xxx 到 xxx 的聚合数据绘制趋势图。
- 使用从 xxx 到 xxx 的详细数据绘制热图。
- 根据之前的查询结果生成最终报告。

/no_think""",
                ),
                ("placeholder", "{messages}"),
            ]
        )
        self.planner = planner_prompt | get_llm(model).with_structured_output(Plan, method="function_calling")

    async def plan_step(self, state: PlanExecute):
        writer = get_stream_writer()
        writer({"event": "plan_step_start", "input": state["input"]})
        plan = await self.planner.ainvoke({"messages": [("user", state["input"])]})
        writer({"event": "plan_step_end", "plan": plan.steps})
        return {"plan": plan.steps, "past_steps": [], "tool_calls": []}

