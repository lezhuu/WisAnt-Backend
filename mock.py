import json
import random
import asyncio
import asyncpg
from datetime import date

from src.utils import get_connection




# areas = [
#     "86309959fffffff",
#     "86309b94fffffff",
#     "86309b96fffffff",
#     "8630982cfffffff",
#     "863099597ffffff",
#     "863099587ffffff",
#     "8630995b7ffffff",
#     "86309825fffffff",
#     "8630995afffffff",
#     "86309958fffffff",
#     "863099437ffffff",
#     "86309b967ffffff",
#     "86309b82fffffff",
#     "86309b847ffffff",
#     "86309b84fffffff",
#     "86309b867ffffff",
#     "86309b86fffffff",
#     "86309b877ffffff",
#     "86309b807ffffff",
#     "86309b947ffffff",
#     "86309b80fffffff",
#     "86309b95fffffff",
#     "8630982c7ffffff",
#     "863099507ffffff",
#     "86309951fffffff",
#     "863099537ffffff",
#     "863099c97ffffff",
#     "86309826fffffff",
#     "863099527ffffff",
#     "863099c87ffffff",
#     "863099c9fffffff",
#     "863099cb7ffffff",
#     "863099517ffffff",
#     "8630995a7ffffff",
#     "86309824fffffff",
#     "863098247ffffff",
#     "863098277ffffff",
#     "863098357ffffff",
#     "86309836fffffff",
#     "86309820fffffff",
#     "863098227ffffff",
#     "8630982efffffff",
#     "863098257ffffff",
#     "86309834fffffff",
#     "863098367ffffff",
#     "863098207ffffff",
#     "86309821fffffff",
#     "8630982e7ffffff",
#     "863098267ffffff",
#     "863098347ffffff",
#     "86309835fffffff",
#     "863098377ffffff",
#     "86309822fffffff",
#     "86309bb5fffffff",
#     "86309bacfffffff",
#     "86309b377ffffff",
#     "8630994dfffffff",
#     "86309bb67ffffff",
#     "86309bad7ffffff",
#     "86309ba47ffffff",
#     "8630994e7ffffff",
#     "863099457ffffff",
#     "86309badfffffff",
#     "8630994efffffff",
#     "86309bb77ffffff",
#     "86309bae7ffffff",
#     "86309ba57ffffff",
#     "8630994f7ffffff",
#     "86309baefffffff",
#     "86309ba5fffffff",
#     "86309baf7ffffff",
#     "86309ba67ffffff",
#     "86309bb07ffffff",
#     "86309ba77ffffff",
#     "864b26d97ffffff",
#     "863099487ffffff",
#     "86309bb0fffffff",
#     "86309b327ffffff",
#     "86309bba7ffffff",
#     "86309948fffffff",
#     "86309bb17ffffff",
#     "86309b32fffffff",
#     "86309bbafffffff",
#     "863099497ffffff",
#     "863099407ffffff",
#     "86309bb1fffffff",
#     "86309949fffffff",
#     "86309bb27ffffff",
#     "86309940fffffff",
#     "86309ba07ffffff",
#     "864b26db7ffffff",
#     "8630994a7ffffff",
#     "863099417ffffff",
#     "86309bb2fffffff",
#     "86309ba0fffffff",
#     "8630994afffffff",
#     "86309bb37ffffff",
#     "86309941fffffff",
#     "8630994b7ffffff",
#     "863099427ffffff",
#     "86309ba1fffffff",
#     "86309942fffffff",
#     "86309bb47ffffff",
#     "86309ba27ffffff",
#     "8630994c7ffffff",
#     "86309bb4fffffff",
#     "86309ba2fffffff",
#     "86309b367ffffff",
#     "8630994cfffffff",
#     "86309bb57ffffff",
#     "86309bac7ffffff",
#     "86309ba37ffffff",
#     "8630994d7ffffff",
#     "8630996a7ffffff",
#     "863099687ffffff",
#     "86309bd6fffffff",
#     "86309b11fffffff",
#     "86309b8b7ffffff",
#     "86309b127ffffff",
#     "8630986c7ffffff",
#     "86309b1a7ffffff",
#     "86309bc4fffffff",
#     "86309b887ffffff",
#     "86309b1afffffff",
#     "86309b137ffffff",
#     "86309b88fffffff",
#     "86309bd4fffffff",
#     "86309b897ffffff",
#     "8630986dfffffff",
#     "86309b107ffffff",
#     "86309bc67ffffff",
#     "86309b89fffffff",
#     "86309b10fffffff",
#     "86309bd5fffffff",
#     "86309bc6fffffff",
#     "86309bd67ffffff",
#     "86309b117ffffff",
#     "86309b99fffffff",
#     "86309bd2fffffff",
#     "86309b917ffffff",
#     "86309b977ffffff",
#     "86309b81fffffff",
#     "86309b9a7ffffff",
#     "86309b8afffffff",
#     "86309864fffffff",
#     "86309b90fffffff",
#     "86309b817ffffff",
#     "86309b8a7ffffff",
#     "86309b907ffffff",
#     "86309b937ffffff",
#     "86309b997ffffff",
#     "86309866fffffff",
#     "86309b92fffffff",
#     "8630986cfffffff",
#     "86309b98fffffff",
#     "86309b837ffffff",
#     "86309b927ffffff",
#     "86309b957ffffff",
#     "86309b987ffffff",
#     "86309b9b7ffffff",
#     "86309b91fffffff",
#     "8630986efffffff",
#     "86309b827ffffff",
#     "86309b9afffffff",
#     "863098777ffffff",
#     "86309875fffffff",
#     "863098757ffffff",
#     "86309874fffffff",
#     "863098747ffffff",
#     "863098677ffffff",
#     "863098667ffffff",
#     "86309865fffffff",
#     "863098657ffffff",
#     "863098647ffffff",
#     "86309862fffffff",
#     "86309838fffffff",
#     "8630982f7ffffff",
#     "8630982dfffffff",
#     "8630982d7ffffff",
#     "8630982afffffff",
#     "8630982a7ffffff",
#     "86309829fffffff",
#     "863098297ffffff",
#     "86309828fffffff",
#     "863098287ffffff",
#     "863098217ffffff",
#     "86309b857ffffff",
#     "86309ba97ffffff",
#     "86309b8efffffff",
#     "86309bab7ffffff",
#     "86309bb8fffffff",
#     "86309b8d7ffffff",
#     "86309b85fffffff",
#     "86309ba9fffffff",
#     "86309b8f7ffffff",
#     "86309b147ffffff",
#     "86309b167ffffff",
#     "86309bb97ffffff",
#     "86309ba87ffffff",
#     "86309b8dfffffff",
#     "86309bbb7ffffff",
#     "86309baa7ffffff",
#     "86309b12fffffff",
#     "86309b14fffffff",
#     "86309b16fffffff",
#     "86309b8c7ffffff",
#     "86309bb9fffffff",
#     "86309ba8fffffff",
#     "86309b8e7ffffff",
#     "86309ba17ffffff",
#     "86309baafffffff",
#     "86309b157ffffff",
#     "86309bb87ffffff",
#     "86309b177ffffff",
#     "86309b8cfffffff",
#     "863099007ffffff",
#     "863099c0fffffff",
#     "86309d697ffffff",
#     "8630990d7ffffff",
#     "863099c47ffffff",
#     "863099d77ffffff",
#     "8630991a7ffffff",
#     "863099d17ffffff",
#     "863099dafffffff",
#     "863099017ffffff",
#     "8630990afffffff",
#     "86309d6a7ffffff",
#     "8630990e7ffffff",
#     "863099c57ffffff",
#     "863099cefffffff",
#     "86309d6dfffffff",
#     "863099087ffffff",
#     "86309911fffffff",
#     "8630991b7ffffff",
#     "863099d27ffffff",
#     "863098a6fffffff",
#     "863099027ffffff",
#     "863099c2fffffff",
#     "86309d61fffffff",
#     "86309d6b7ffffff",
#     "863099d5fffffff",
#     "8630990f7ffffff",
#     "86309918fffffff",
#     "863099c67ffffff",
#     "863099c07ffffff",
#     "86309d68fffffff",
#     "863099d37ffffff",
#     "863099037ffffff",
#     "86309d6c7ffffff",
#     "86309919fffffff",
#     "863099c77ffffff",
#     "863099d0fffffff",
#     "86309900fffffff",
#     "8630990a7ffffff",
#     "86309d69fffffff",
#     "863099d47ffffff",
#     "863099c4fffffff",
#     "863099ce7ffffff",
#     "86309d6d7ffffff",
#     "863099117ffffff",
#     "8630991afffffff",
#     "863099d1fffffff",
#     "86309901fffffff",
#     "8630990b7ffffff",
#     "863099c27ffffff",
#     "86309d617ffffff",
#     "86309d6afffffff",
#     "863099d57ffffff",
#     "863099187ffffff",
#     "863099c5fffffff",
#     "86309908fffffff",
#     "86309d687ffffff",
#     "863099d2fffffff",
#     "8630990c7ffffff",
#     "863099c37ffffff",
#     "863099ccfffffff",
#     "863099197ffffff",
#     "863099c6fffffff",
#     "86309d6f7ffffff",
#     "863099d07ffffff",
#     "863099467ffffff",
#     "863099547ffffff",
#     "86309955fffffff",
#     "863099447ffffff",
#     "863099577ffffff",
#     "8630997a7ffffff",
#     "863099637ffffff",
#     "86309960fffffff",
#     "863099717ffffff",
#     "863099607ffffff",
#     "8630997afffffff",
#     "86309944fffffff"
# ]

industry = ["加油服务", "智能业务部餐饮行业", "游戏行业"]

age = ["<20", "20-40", ">=40"]

salary = ["低", "较低", "中等", "较高", "高"]

gender = ["M", "F"]

year_weeks = [
    "2024-1",
    "2024-31",
    "2024-32",
    "2024-33",
    "2024-34",
    "2024-35",
    "2024-36",
    "2024-37",
    "2024-38",
    "2024-39",
    "2024-40",
    "2024-41",
    "2024-42",
    "2024-43",
    "2024-44",
    "2024-45",
    "2024-46",
    "2024-47",
    "2024-48",
    "2024-49",
    "2024-50",
    "2024-51",
    "2024-52",
    "2025-1",
    "2025-10",
    "2025-11",
    "2025-12",
    "2025-13",
    "2025-14",
    "2025-15",
    "2025-16",
    "2025-17",
    "2025-18",
    "2025-19",
    "2025-2",
    "2025-20",
    "2025-21",
    "2025-22",
    "2025-23",
    "2025-24",
    "2025-25",
    "2025-26",
    "2025-27",
    "2025-28",
    "2025-29",
    "2025-3",
    "2025-30",
    "2025-31",
    "2025-4",
    "2025-5",
    "2025-6",
    "2025-7",
    "2025-8",
    "2025-9",
]

with open("data/shanghai-h3-index-to-district-res.json", "r") as f:
    data = json.load(f)

areas = list(data.keys())
def get_year_month_by_year_week(year_week:str):
    year_week_stripped = year_week.strip()
    parts = year_week_stripped.split("-")
    if len(parts) != 2:
        raise ValueError("year_week must be in 'YYYY-W' or 'YYYY-WW' format")

    iso_year = int(parts[0])
    iso_week = int(parts[1])
    iso_week_monday = date.fromisocalendar(iso_year, iso_week, 1)
    return iso_week_monday.year, iso_week_monday.month, iso_week


def get_random_mock_sales():
    return random.randint(10, 100000)


def get_district_name_by_geo_index(geo_index: str):
    return data[geo_index]

def generate_sales_data():
    
    # geo_index	industry	age_rank	gender	year_week	sales_amount	trade_count	customer_count
    geo_index = random.randint(0, len(areas) - 1)
    industry_index = random.randint(0, len(industry) - 1)
    age_index = random.randint(0, len(age) - 1)
    gender_index = random.randint(0, len(gender) - 1)
    year_week_index = random.randint(0, len(year_weeks) - 1)
    year, month, week = get_year_month_by_year_week(year_weeks[year_week_index])
    sales = get_random_mock_sales()
    trade_count = random.randint(1, 1000)
    customer_count = random.randint(1, 100)
    return {
        "geo_index": areas[geo_index],
        "district_name": get_district_name_by_geo_index(areas[geo_index]),
        "industry": industry[industry_index],
        "age_rank": age[age_index],
        "gender": gender[gender_index],
        "year_month": f"{year}-{month:02d}",
        "sales_amount": sales,
        "trade_count": trade_count,
        "customer_count": customer_count
    }


async def _ensure_sql_table(connection: asyncpg.Connection) -> None:
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS public.sql_data (
        id SERIAL PRIMARY KEY,
        sql TEXT NOT NULL
    );
    """
    await connection.execute(create_table_sql)

    comment_sql_statements = [
        "COMMENT ON TABLE public.sql_data IS 'SQL 查询语句'",
        "COMMENT ON COLUMN public.sql_data.id IS '主键'",
        "COMMENT ON COLUMN public.sql_data.sql IS 'SQL 查询语句'",
    ]
    for stmt in comment_sql_statements:
        await connection.execute(stmt)

        



async def _ensure_sales_data_table(connection: asyncpg.Connection) -> None:
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS public.sales_data (
        id SERIAL PRIMARY KEY,
        geo_index TEXT NOT NULL,
        district_name TEXT NOT NULL,
        industry TEXT NOT NULL,
        age_rank TEXT NOT NULL,
        gender TEXT NOT NULL,
        year_month TEXT NOT NULL,
        sales_amount INTEGER NOT NULL,
        trade_count INTEGER NOT NULL,
        customer_count INTEGER NOT NULL
    );
    """
    await connection.execute(create_table_sql)

    comment_sql_statements = [
        "COMMENT ON TABLE public.sales_data IS '模拟销售指标数据，按 H3 地理单元、行业、年龄段、性别和年-周聚合'",
        "COMMENT ON COLUMN public.sales_data.id IS '主键'",
        "COMMENT ON COLUMN public.sales_data.geo_index IS 'H3 区块索引'",
        "COMMENT ON COLUMN public.sales_data.district_name IS '区域名称'",
        "COMMENT ON COLUMN public.sales_data.industry IS '行业名称：加油服务、智能业务部餐饮行业、游戏行业'",
        "COMMENT ON COLUMN public.sales_data.age_rank IS '年龄段：1-3'",
        "COMMENT ON COLUMN public.sales_data.gender IS '性别：M/F'",
        "COMMENT ON COLUMN public.sales_data.year_month IS '年-月'",
        "COMMENT ON COLUMN public.sales_data.sales_amount IS '销售金额（整数）'",
        "COMMENT ON COLUMN public.sales_data.trade_count IS '交易笔数'",
        "COMMENT ON COLUMN public.sales_data.customer_count IS '客户数'",
    ]
    for stmt in comment_sql_statements:
        await connection.execute(stmt)

    # Optional lightweight indexes for common filters/lookups
    await connection.execute("CREATE INDEX IF NOT EXISTS idx_sales_data_area ON public.sales_data(geo_index);")
    await connection.execute("CREATE INDEX IF NOT EXISTS idx_sales_data_industry ON public.sales_data(industry);")


async def write_sales_data_to_pg(number=10000):
    # ports:
    #   - 5432:5432
    # volumes:
    #   - ./data/postgres:/var/lib/postgresql/data
    # environment:
    #   POSTGRES_DB: wisant-poc
    #   POSTGRES_PASSWORD: wisant-poc
    #   POSTGRES_USER: wisant-poc
    # healthcheck:
    #   test: pg_isready -U pgweb -h 127.0.0.1
    #   interval: 5s
    conn = await get_connection()
    try:
        # Ensure table exists before inserting
        await _ensure_sales_data_table(conn)
        await _ensure_sql_table(conn)

        # Prepare batch of rows
        rows = []
        for _ in range(number):
            sales_data = generate_sales_data()
            rows.append(
                (
                    sales_data["geo_index"],
                    sales_data["district_name"],
                    sales_data["industry"],
                    sales_data["age_rank"],
                    sales_data["gender"],
                    sales_data["year_month"],
                    sales_data["sales_amount"],
                    sales_data["trade_count"],
                    sales_data["customer_count"],
                )
            )

        insert_sql = (
            "INSERT INTO public.sales_data (geo_index, district_name, industry, age_rank, gender, year_month, sales_amount, trade_count, customer_count) "
            "VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)"
        )

        async with conn.transaction():
            await conn.executemany(insert_sql, rows)
    finally:
        await conn.close()



if __name__ == "__main__":
    asyncio.run(write_sales_data_to_pg(100000))








