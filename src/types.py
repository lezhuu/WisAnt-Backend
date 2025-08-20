from typing import Annotated, Tuple, Any, Dict, List, Optional, Union, Literal
from pydantic import BaseModel, Field
from typing_extensions import TypedDict
import operator


class PlanExecute(TypedDict):
    input: Optional[str]
    plan: Optional[List[str]]
    past_steps: Optional[Annotated[List[Tuple], operator.add]]
    response: Optional[str] 
    tool_calls: Optional[Annotated[List[dict], operator.add]]
    


class Plan(BaseModel):
    """Plan to follow in future"""

    steps: List[str] = Field(
        description="different steps to follow, should be in sorted order"
    )


class Response(BaseModel):
    """Response to user."""

    response: str


class Act(BaseModel):
    """Action to perform, response and steps are mutually exclusive"""
    response: str = Field(
        description="Response to user. empty when no response is needed"
    )
    steps: List[str] = Field(
        description="different steps to follow, should be in sorted order, empty when no steps are needed"
    )