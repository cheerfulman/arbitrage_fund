#!/usr/bin/env python3
# 测试文件：验证中国时区时间设置

from datetime import datetime
import pytz

# 打印当前中国时间
china_time = datetime.now(pytz.timezone('Asia/Shanghai'))
print(f"中国时间 (Asia/Shanghai): {china_time}")
print(f"中国时间日期: {china_time.date()}")

# 打印当前UTC时间，用于比较
utc_time = datetime.utcnow()
print(f"UTC时间: {utc_time}")
print(f"UTC时间日期: {utc_time.date()}")

# 计算时区差
time_diff = china_time - utc_time
print(f"时区差: {time_diff}")

# 测试直接使用datetime.now().date()
local_time = datetime.now()
print(f"本地时间: {local_time}")
print(f"本地时间日期: {local_time.date()}")
