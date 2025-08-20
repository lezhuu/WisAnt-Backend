import asyncio
import csv
import os
from typing import List, Tuple

from src.utils import get_connection
from mock import (
    _ensure_sales_data_table,
    get_district_name_by_geo_index,
    get_year_month_by_year_week,
)


REQUIRED_HEADERS = [
    "geo_index",
    "industry",
    "age_rank",
    "gender",
    "year_week",
    "sales_amount",
    "trade_count",
    "customer_count",
]


def _sniff_dialect(file_path: str) -> csv.Dialect:
    with open(file_path, "r", encoding="utf-8-sig", newline="") as f:
        sample = f.read(2048)
    try:
        return csv.Sniffer().sniff(sample, delimiters=[",", "\t", ";", "|"])
    except Exception:
        # 回退：优先按制表符（与示例一致），再退到逗号
        class Tsv(csv.Dialect):
            delimiter = "\t"
            quotechar = '"'
            doublequote = True
            skipinitialspace = False
            lineterminator = "\n"
            quoting = csv.QUOTE_MINIMAL

        return Tsv


def _sanitize_geo_index(value: str) -> str:
    value = (value or "").strip()
    return value


def _validate_headers(headers: List[str]) -> None:
    normalized = [h.strip() for h in headers]
    missing = [h for h in REQUIRED_HEADERS if h not in normalized]
    if missing:
        raise ValueError(f"CSV 缺少必要表头: {', '.join(missing)}")


def _parse_int(value: str) -> int:
    value = (value or "").strip()
    if value == "":
        return 0
    return int(float(value))


def _row_to_record(row: dict) -> Tuple[str, str, str, str, str, int, int, int]:
    geo_index = _sanitize_geo_index(row.get("geo_index", ""))
    industry = (row.get("industry") or "").strip()
    age_rank = str((row.get("age_rank") or "").strip())
    gender = (row.get("gender") or "").strip()
    year_week = (row.get("year_week") or "").strip()

    # year_week -> year_month
    year, month, _week = get_year_month_by_year_week(year_week)
    year_month = f"{year}-{month:02d}"

    # 金额/计数字段
    sales_amount = _parse_int(row.get("sales_amount"))
    trade_count = _parse_int(row.get("trade_count"))
    customer_count = _parse_int(row.get("customer_count"))

    # district_name 通过 geo_index 映射，失败时给出占位
    try:
        district_name = get_district_name_by_geo_index(geo_index)
    except Exception:
        district_name = "未知区域"

    return (
        geo_index,
        district_name,
        industry,
        age_rank,
        gender,
        year_month,
        sales_amount,
        trade_count,
        customer_count,
    )


async def insert_csv_to_sales_data(file_path: str, batch_size: int = 1000) -> int:
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"文件不存在: {file_path}")

    dialect = _sniff_dialect(file_path)

    conn = await get_connection()
    inserted = 0
    try:
        await _ensure_sales_data_table(conn)

        rows: List[Tuple[str, str, str, str, str, int, int, int]] = []

        with open(file_path, "r", encoding="utf-8-sig", newline="") as f:
            reader = csv.DictReader(f, dialect=dialect)
            _validate_headers(reader.fieldnames or [])

            for row in reader:
                try:
                    record = _row_to_record(row)
                except Exception:
                    # 单行解析失败，跳过
                    continue

                rows.append(record)

                if len(rows) >= batch_size:
                    await _bulk_insert(conn, rows)
                    inserted += len(rows)
                    rows.clear()

        if rows:
            await _bulk_insert(conn, rows)
            inserted += len(rows)

    finally:
        await conn.close()

    return inserted


async def _bulk_insert(conn, rows: List[Tuple[str, str, str, str, str, int, int, int]]):
    sql = (
        "INSERT INTO public.sales_data "
        "(geo_index, district_name, industry, age_rank, gender, year_month, sales_amount, trade_count, customer_count) "
        "VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)"
    )
    async with conn.transaction():
        await conn.executemany(sql, rows)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="将 CSV/TSV 销售数据导入到 PostgreSQL 的 public.sales_data 表")
    parser.add_argument("file", help="CSV/TSV 文件路径，需包含表头：" + ", ".join(REQUIRED_HEADERS))
    parser.add_argument("--batch-size", type=int, default=1000, help="批量写入大小")
    args = parser.parse_args()

    total = asyncio.run(insert_csv_to_sales_data(args.file, batch_size=args.batch_size))
    print(f"插入完成，共 {total} 行")


