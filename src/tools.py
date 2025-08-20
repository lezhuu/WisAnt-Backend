import decimal
import json
import re
from typing_extensions import Annotated
from langchain_core.tools import tool, InjectedToolCallId
from langgraph.config import get_stream_writer
from src.utils import exec_insert_sql, exec_select_sql


class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, decimal.Decimal):
            # wanted a simple yield str(o) in the next line,
            # but that would mean a yield on the line with super(...),
            # which wouldn't work (see my comment below), so...
            return (str(o) for o in [o])
        return super(DecimalEncoder, self).default(o)

def get_tools():
    tools = [search_postgres, draw_smoothed_line_chart, draw_detail_heatmap]
    return tools

@tool(parse_docstring=True)
async def search_postgres(sql: str) -> str:  
    """Execute a SQL query against the PostgreSQL database.

    This function runs a raw SQL string on the `wisant-poc` Postgres instance and
    returns the fetched records.

    All SQL queries must use aggregate functions to summarize data rather than returning raw detailed rows.
    Use GROUP BY with aggregate functions such as SUM, COUNT, AVG, MIN, MAX, and optionally
    time bucketing by week/month (e.g., GROUP BY year) to obtain compact, informative results.

    Only includes data from July 2024 to July 2025.

    Table ``public.sales_data`` schema:

    - id (integer): Auto-incrementing primary key. Internal reference only.
    - geo_index (text): H3 hex index string (e.g., ``86309959fffffff``); used
        for spatial aggregation and filtering.
    - district_name (text): District name (e.g., ``宝山区``, ``崇明区``).
    - industry (text): Industry name (加油服务、智能业务部餐饮行业、游戏行业).
    - age_rank (text): Age band code (e.g., ``1``, ``2``, ``3``); ranges are
        defined upstream.
    - gender (text): Gender flag (``M``/``F``).
    - year_month (text): Year-month (e.g., ``2024-07``).
    - sales_amount (integer): Sales amount ( integer); treat as relative
        magnitude.
    - trade_count (integer): Number of transactions ( integer).
    - customer_count (integer): Number of customers ( integer).

    Args:
        sql (str): SQL query string to execute.

    Returns:
        str: JSON-encoded list of rows, where each row is a dict of column names to values.

    Notes:
        - To avoid excessive output, SELECT/WITH queries are forced to have a maximum
          LIMIT of 10. If a larger LIMIT exists, it will be capped to 10.
    """

    writer = get_stream_writer()
    writer({"event": "tool_start", "tool": "search_postgres", "sql": sql})


    result = None

    def enforce_limit_on_sql(input_sql: str, max_limit: int = 10) -> str:
        sql_trimmed = input_sql.strip()
        sql_without_semicolon = sql_trimmed[:-1] if sql_trimmed.endswith(";") else sql_trimmed
        sql_lower = sql_without_semicolon.lower()

        # Only enforce limits for queries that start with SELECT or WITH, to avoid
        # breaking non-SELECT statements.
        if sql_lower.startswith("select") or sql_lower.startswith("with"):
            # If a LIMIT already exists, cap it to the maximum allowed value.
            has_limit = re.search(r"\\blimit\\s+(\\d+)", sql_lower)
            if has_limit:
                current_limit = int(has_limit.group(1))
                if current_limit > max_limit:
                    # Replace the first LIMIT value in a case-insensitive manner.
                    capped_sql = re.sub(r"(?i)\\blimit\\s+\\d+", f"LIMIT {max_limit}", sql_without_semicolon, count=1)
                    return capped_sql
                return sql_without_semicolon
            # If no LIMIT exists, append one.
            return f"{sql_without_semicolon} LIMIT {max_limit}"

        # Return non-SELECT/WITH statements as-is.
        return sql_without_semicolon

    final_sql = enforce_limit_on_sql(sql)
    writer({"event": "tool_prepared", "tool": "search_postgres", "final_sql": final_sql})

    
    result = await exec_select_sql(final_sql)

    final_result = json.dumps(result, ensure_ascii=False, cls=DecimalEncoder)

    writer({"event": "tool_end", "tool": "search_postgres", "row_count": len(result)})
    return final_result



@tool(parse_docstring=True)
def draw_smoothed_line_chart(
    data_x: list[str], 
    data_y: list[int], 
    x_label: str,
    y_label: str,
    tool_call_id: Annotated[str, InjectedToolCallId]
) -> str:
    """Draw a smoothed line chart.

    Args:
        data_x (list[str]): The x-axis data.
        data_y (list[int]): The y-axis data.
        x_label (str): The label of the x-axis.
        y_label (str): The label of the y-axis.

    Returns:
        str: if draw success, return "chart drawn", otherwise return "chart draw failed"
    """
    writer = get_stream_writer()
    writer({"event": "tool_start", "tool": "draw_smoothed_line_chart", "data_x_len": len(data_x), "data_y_len": len(data_y)})
    # drawing logic would go here
    writer({"event": "tool_end", "tool": "draw_smoothed_line_chart", "status": "ok",  "tool_call_id": tool_call_id, "data": {
        "x": data_x, "y": data_y, "x_label": x_label, "y_label": y_label,
    }})
    # return "smoothed line chart drawn successfully"
    return "smoothed line chart drawn successfully"


@tool(parse_docstring=True)
async def draw_detail_heatmap(sql:str, heat_data_column: str, tool_call_id: Annotated[str, InjectedToolCallId]):
    """Draw a 3D heatmap that includes time, region, and detailed information.
    需要保证sql获取到的数据足够充分, 必须包含表中所有的字段

    Args:
        sql (str): SQL query used to retrieve the data for drawing the detailed heatmap;
        heat_data_column (str): The single column of the heatmap, must be a single number column.

    Returns:
        str: if draw success, return "detailed heatmap drawn successfully", otherwise return "datamap draw failed"
    """
    writer = get_stream_writer()
    writer({"event": "tool_start", "tool": "draw_detail_heatmap"})

    # 将sql写入到数据库中
    heatmap_data_id = await exec_insert_sql("INSERT INTO public.sql_data (sql) VALUES ($1) RETURNING id", sql)
    # drawing logic would go here
    writer({"event": "tool_end", "tool": "draw_detail_heatmap", "status": "ok", "tool_call_id": tool_call_id, "data": {
        "heatmap_data_id": heatmap_data_id,
        "heat_data_column": heat_data_column,
    }})
    return "detailed heatmap drawn successfully"