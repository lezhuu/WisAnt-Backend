import contextlib
from src.agent_executor import AgentExacutor
from src.planner import Planner
from src.re_planner import RePlanner
from src.tools import get_tools
from src.types import PlanExecute
from langgraph.graph import StateGraph, START, END

def should_end(state: PlanExecute):
    if "response" in state and state["response"]:
        return END
    else:
        return "agent"

        
class Workflow:
    def __init__(
        self,
        agent_executor_model="gpt-4o",
        planner_model="gpt-4o",
        replanner_model="gpt-4o",
    ) -> None:
        self.store = {
            "tool_calls": [],
        }
        self.agent_executor = AgentExacutor(get_tools(), agent_executor_model, self.store)
        self.planner = Planner(planner_model, self.store)
        self.replanner = RePlanner(replanner_model, self.store)
        self.app = self.init()

    def init(self):
        workflow = StateGraph(PlanExecute)

        # Add the plan node
        workflow.add_node("planner", self.planner.plan_step)

        # Add the execution step
        workflow.add_node("agent", self.agent_executor.execute_step)

        # Add a replan node
        workflow.add_node("replan", self.replanner.replan_step)

        workflow.add_edge(START, "planner")

        # From plan we go to agent
        workflow.add_edge("planner", "agent")

        # From agent, we replan
        workflow.add_edge("agent", "replan")

        workflow.add_conditional_edges(
            "replan",
            # Next, we pass in the function that will determine which node is called next.
            should_end,
            ["agent", END],
        )

        # Finally, we compile it!
        # This compiles it into a LangChain Runnable,
        # meaning you can use it as you would any other runnable
        return  workflow.compile()


    async def astream_events(
        self,
        input_text: str,
        recursion_limit: int = 50,
    ):

        stream = self.app.astream(
            {"input": input_text},
            config={"recursion_limit": recursion_limit},
            subgraphs=True,
            stream_mode=["updates", "messages", "custom"],
        )
        try:
            async for msg, event, message in stream:
                result = {
                    "msg": msg,
                    "event": event,
                    "message": message,
                }
                if event == "messages":
                    message, state = message
                    dict_message = dict(message)
                    result["node"] = state["langgraph_node"]
                    message_type = "tool" if len(dict_message["additional_kwargs"]) > 0 else "message"
                    if message_type == "tool":
                        tool_calls = dict_message["tool_calls"]
                        tool_calls_list = []
                        for tool_call in tool_calls:
                            if tool_call['type'] == "tool_call" and len(tool_call["name"]) > 0:
                                tool_calls_list.append(tool_call["name"])
                        if len(tool_calls_list) > 0:
                            result["event"] = "start_tool_call"
                            result["tool_calls_list"] = tool_calls_list
                        else:
                            result["event"] = "calling_tool"
                    result["message"] = message
                yield result
        finally:
            # Ensure the underlying LangGraph stream is properly closed when client disconnects
            with contextlib.suppress(Exception):
                await stream.aclose()
