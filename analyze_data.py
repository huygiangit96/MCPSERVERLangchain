from mcp.server.fastmcp import FastMCP
import pandas as pd
import polars as pl
from typing import List, Dict, Any, Optional
from pydantic import Field
from glob import glob
from datetime import datetime
import pytz

mcp=FastMCP(
    name = "analyze_data",
    port=8080
    )


file = 'File.xlsm'

headers = [
    "STT",
    "Loại",
    "Tên",
    "Số thụ lý",
    "Ngày ban hành thụ lý",
    "Ngày nhận thụ lý",
    "Số ngày gia hạn",
    "Ngày hết hạn",
    "Thời hạn còn",
    "Kiểm sát viên",
    "TK tuần thụ lý",
    "TK tháng thụ lý",
    "Quyết định xét xử/họp",
    "Ngày",
    "Văn bản giải quyết",
    "Số giải quyết",
    "Ngày ban hành",
    "Ngày nhận",
    "TK tuần giải quyết",
    "TK tháng giải quyết",
    "PT RKN",
    "Ghi chú",
    "Thời hạn gửi thông báo thụ lý, quyết định tiếp tục, quyết định hòa giải thành",
    "Thời hạn gửi các quyết định, bản án"
]

def GetDataSheet(sheet_name):
    df = pd.read_excel(file, sheet_name=sheet_name, header=None)

    row_idx, col_idx = next(
        ((i, j) for i in range(df.shape[0]) for j in range(df.shape[1])
        if str(df.iat[i, j]).strip() == "STT"),
        (None, None)
    )
    if row_idx is None:
        raise ValueError("'STT' not found.")

    header_upper = df.iloc[row_idx - 1].fillna("")
    header_lower = df.iloc[row_idx].fillna("")

    # Combine
    combined_headers = []
    for upper, lower, idx in zip(header_upper, header_lower, range(len(header_lower))):
        upper_str = str(upper).strip()
        lower_str = str(lower).strip()
        if upper_str:
            combined = f"{upper_str}_{lower_str}" if lower_str else upper_str
        else:
            combined = lower_str if lower_str else f"Unnamed_{idx}"
        combined_headers.append(combined)

    # Ensure uniqueness
    # If there are duplicates, append index
    from collections import Counter

    counter = Counter(combined_headers)
    for i, name in enumerate(combined_headers):
        if counter[name] > 1:
            combined_headers[i] = f"{name}_{i}"

    data_pd = df.iloc[row_idx + 1:].copy()
    data_pd.columns = combined_headers

    df_pl = pl.from_pandas(data_pd)

    stt_col = next(c for c in combined_headers if c.endswith("STT"))
    df_pl = df_pl.filter(pl.col(stt_col).is_not_null())

    cols_to_keep = df_pl.columns[1:-1]
    df_pl = df_pl.select(cols_to_keep)
    df_pl = df_pl.rename(dict(zip(df_pl.columns, headers)))
    return df_pl



polars_sql_aggregate_functions = [
    "avg",
    "count",
    "first",
    "last",
    "max",
    "median",
    "min",
    "sum",
    "quantile_count",
    "quantile_disc",
    "stddev",
    "sum",
    "variance",
]

polars_sql_array_functions = [
    "array_agg",
    "array_contains",
    "array_get",
    "array_length",
    "array_lower",
    "array_mean",
    "array_reverse",
    "array_sum",
    "array_to_string",
    "array_unique",
    "array_upper",
    "unnest",
]

polars_sql_bitwise_functions = [
    "bit_and",
    "bit_count",
    "bit_or",
    "bit_xor",
]

polars_sql_conditional_functions = [
    "coalesce",
    "greatest",
    "if",
    "ifnull",
    "least",
    "nullif",
]

polars_sql_mathematical_functions = [
    "abs",
    "cbrt",
    "ceil",
    "div",
    "exp",
    "floor",
    "ln",
    "log2",
    "log10",
    "mod",
    "pi",
    "pow",
    "round",
    "sign",
    "sqrt",
]

polars_sql_string_functions = [
    "bit_length",
    "concat",
    "concat_ws",
    "date",
    "ends_with",
    "initcap",
    "left",
    "length",
    "lower",
    "ltrim",
    "normalize",
    "octet_length",
    "regexp_like",
    "replace",
    "reverse",
    "right",
    "rtrim",
    "starts_with",
    "strpos",
    "strptime",
    "substr",
    "timestamp",
    "upper",
]

polars_sql_temporal_functions = [
    "date_part",
    "extract",
    "strftime",
]

polars_sql_type_functions = [
    "cast",
    "try_cast",
]

polars_sql_trigonometric_functions = [
    "acos",
    "acosd",
    "asin",
    "asind",
    "atan",
    "atand",
    "atan2",
    "atan2d",
    "cot",
    "cotd",
    "cos",
    "cosd",
    "degrees",
    "radians",
    "sin",
    "sind",
    "tan",
    "tand",
]

polars_sql_functions = {
    "aggregate": polars_sql_aggregate_functions,
    "array": polars_sql_array_functions,
    "bitwise": polars_sql_bitwise_functions,
    "conditional": polars_sql_conditional_functions,
    "mathematical": polars_sql_mathematical_functions,
    "string": polars_sql_string_functions,
    "temporal": polars_sql_temporal_functions,
    "type": polars_sql_type_functions,
    "trigonometric": polars_sql_trigonometric_functions,
}


def gen_polars_sql_functions_str():
    sql_functions_agg = []
    for sql_fn_category, sql_fns in polars_sql_functions.items():
        sql_fns_str = ["- " + sql_fn.capitalize() for sql_fn in sql_fns]
        sql_fns_str = "\n".join(sql_fns_str)
        sql_fn_with_header = f"{sql_fn_category.capitalize()}: \n{sql_fns_str}\n\n"
        sql_functions_agg.append(sql_fn_with_header)
    return "\n".join(sql_functions_agg)


query_description = f"""
The polars sql query to be executed.
polars sql query must use the table name as `self` to refer to the source data.
Supported functions are:
{gen_polars_sql_functions_str()}
"""

@mcp.tool()
def analyze_case_data(
    query: str = Field(
        description=query_description,
    ),
    sheet_name: str = Field(
        description="Tên sheet cần lấy"
    )
) -> List[Dict[str, Any]]:
    """
    Trước khi dùng tool này phải Lấy tên các sheet của file dữ liệu, sau đó phân tích yêu cầu để chọn đúng sheet mà người dùng mong muốn
    Toàn bộ dữ liệu trong sheet là cùng một loại vụ việc/vụ án
    Dùng để lấy dữ liệu các vụ án, vụ việc và phân tích tùy theo yêu cầu của người dùng, tạo ra các câu lệnh sql phù hợp để lấy kết quả người dùng mong muốn
     Usage Guidelines for AI:
        1. Chỉ cần hiển thị kết quả cuối cùng
        2. LUÔN LUÔN chạy "SELECT * FROM self" trước để hiểu cấu trúc
        3. Phân tích schema và sample data
        4. Sau đó viết query phù hợp với yêu cầu, nếu tìm kiếm chính xác không cho ra kết quả thì có thể dùng like
        5. Validate kết quả có hợp lý không
        
        Args:
            query: Câu lệnh SQL cần thực thi
            explain_plan: True nếu muốn xem execution plan
            sheet_name: Tên sheet người dùng muốn lấy dữ liệu
        
        Returns:
            dict: {
                "data": [...],  # Kết quả query
            }
        
    """
    df = GetDataSheet(sheet_name)
    op_df = df.sql(query)
    output_records = op_df.to_dicts()
    return output_records


@mcp.tool()
def analyze_email_data(
    query: str = Field(
        description=query_description,
    ),
) -> List[Dict[str, Any]]:
    """
        Usage Guidelines for AI:
        1. Chỉ cần hiển thị kết quả cuối cùng
        2. LUÔN LUÔN chạy get_schema_email_data trước để hiểu cấu trúc
        3. Phân tích schema và sample data
        4. Sau đó viết query phù hợp với yêu cầu, đối với tìm kiếm, nếu tìm kiếm chính xác không cho ra kết quả thì có thể dùng like
        5. Validate kết quả có hợp lý không
        
        Args:
            query: Câu lệnh SQL cần thực thi
            explain_plan: True nếu muốn xem execution plan
        
        Returns:
            dict: {
                "data": [...],  # Kết quả query
            }
        
        """
    df = pl.read_csv("mail.csv")
    op_df = df.sql(query)
    output_records = op_df.to_dicts()
    return output_records


@mcp.tool()
def get_schema_case_data(
    sheet_name: str = Field(
        description="Tên sheet cần lấy"
    )) -> List[Dict[str, Any]]:
    """
    Lấy tên các cột và kiểu dữ liệu của bảng dữ liệu vụ án
    Args:
            sheet_name: Tên sheet người dùng muốn lấy dữ liệu
    """
    schema = GetDataSheet(sheet_name).schema
    schema_dict = {}
    for key, value in schema.items():
        schema_dict[key] = value
    return schema_dict

@mcp.tool()
def get_schema_email_data() -> List[Dict[str, Any]]:
    """
    Lấy tên các cột và kiểu dữ liệu của bảng dữ liệu email
    """
    df = pl.read_csv("mail.csv")
    schema = df.schema
    schema_dict = {}
    for key, value in schema.items():
        schema_dict[key] = value
    return schema_dict

@mcp.tool()
def get_list_sheet_name() -> Dict:
    """
    Lấy tên các sheet của file dữ liệu
    """
    sheet_name = pd.ExcelFile(file).sheet_names
    return dict(enumerate(sheet_name))

@mcp.tool()
def get_current_time() -> str:
    """
    Lấy thời gian hiện tại của hệ thống
    
    Returns:
        str: Thời gian hiện tại theo định dạng ISO 8601
    """
    current_time = datetime.now()
    return current_time.strftime("%Y-%m-%d %H:%M:%S")


@mcp.tool()
def get_current_time_with_timezone(timezone: str = "UTC") -> str:
    """
    Lấy thời gian hiện tại với múi giờ cụ thể
    
    Args:
        timezone (str): Tên múi giờ (ví dụ: 'Asia/Ho_Chi_Minh', 'UTC', 'US/Eastern')
    
    Returns:
        str: Thời gian hiện tại với múi giờ được chỉ định
    """
    try:
        tz = pytz.timezone(timezone)
        current_time = datetime.now(tz)
        return current_time.strftime("%Y-%m-%d %H:%M:%S %Z")
    except pytz.exceptions.UnknownTimeZoneError:
        return f"Múi giờ không hợp lệ: {timezone}"



# Resource : Cung cấp tài nguyên nào đó
# Prompt: Querry lên để lấy 1 prompt cho 1 tình huống nào đó



if __name__=="__main__":
    mcp.run(transport="streamable-http")