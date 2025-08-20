from langgraph.config import get_stream_writer

from src.types import PlanExecute

class RePlanner:
    def __init__(self, model="gpt-4o", store=None) -> None:
        self.store = store
#         replanner_prompt = ChatPromptTemplate.from_template(
#             """根据给定的目标，制定一个简洁明了的步骤计划。该计划应包含一系列独立的任务，如果正确执行这些任务将得到最终答案。
#             请确保不添加任何不必要的步骤，并且每个步骤都包含了完成所需的所有信息。最终步骤的结果应该是最终答案。请注意以下几点： 
#             - 数据库仅包含2024年7月至2025年7月的数据。 
#             - 计划中必须有一个步骤用于生成详细的报告数据热图。 
#             - 在适当的情况下，可以绘制趋势图表（可选：零次、一次或多次）。 
#             - 最终输出应该是一份包含文本和视觉元素的报告。 
#             - 不要在计划中透露任何关于函数调用的信息。 
            
#             ### 目标 
#             {input} 
            
#             ### 原始计划 
#             {plan}
            
#             ### 已完成的步骤 
#             {past_steps} 
            
#             ### 请根据以上信息按顺序更新你的计划 
# /no_think"""
#         )


#         self.replanner = replanner_prompt | get_llm(model).with_structured_output(Act, method="function_calling")
        pass

    async def replan_step(self, state: PlanExecute):
        writer = get_stream_writer()
        writer({"event": "replan_step_start", "input": state["input"]})
        # output:Act = await self.replanner.ainvoke(state)

        # if len(output.steps) == 0:
        #     writer({"event": "replan_step_end_response", "response": output.response})
        #     return {"response": output.response}
        # else:
        #     writer({"event": "replan_step_end_plan", "plan": output.steps})
        #     return {"plan": output.steps}
        
        
        state["plan"].pop(0)

        if len(state["plan"]) == 0:
            writer({"event": "replan_step_end_response", "response": "计划已全部执行完成。"})
            return {"response": "计划已全部执行完成。"}
        else:
            writer({"event": "replan_step_end_plan", "plan": state["plan"]})

        return {"plan": state["plan"]}