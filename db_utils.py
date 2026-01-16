import mysql.connector
from datetime import datetime, date
import json
from dataclasses import dataclass

# 导入类型注解模块
try:
    from typing import TypeVar, List, Optional
except ImportError:
    from typing_extensions import TypeVar, List, Optional

# 导入时区相关模块
from datetime import datetime, date
import pytz  # 需要确保pytz已安装

# 定义泛型类型变量
T = TypeVar('T')


@dataclass
class AIAnalysis:
    """AI分析结果结构体，对应ai_analyses表"""
    id: int  # 主键ID
    analysis_content: str  # AI分析内容
    fund_name: str  # 基金名称（删除了token_count字段）
    fund_code: str  # 基金代码
    created_at: datetime  # 创建时间
    update_at: datetime  # 更新时间
    date: date  # 分析日期


class DatabaseManager:
    """数据库管理类，用于连接和操作MySQL数据库"""

    def __init__(self, host="db", port=3306, database="arbitrage_fund", user="admin", password="admin123"):
        """初始化数据库连接"""
        self.conn_params = {
            "host": host,
            "port": port,
            "database": database,
            "user": user,
            "password": password
        }
        self.conn = None
        self.cursor = None

    def connect(self) -> bool:
        """建立数据库连接"""
        try:
            self.conn = mysql.connector.connect(**self.conn_params)
            # 使用dictionary=True参数替代MySQLCursorDict类
            self.cursor = self.conn.cursor(dictionary=True)
            print("✅ 数据库连接成功！")
            return True
        except Exception as e:
            print(f"❌ 数据库连接失败：{e}")
            return False

    def disconnect(self) -> None:
        """关闭数据库连接"""
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()
        print("✅ 数据库连接已关闭")

    def create_tables(self) -> bool:
        """创建数据库表"""
        try:
            # 创建基金数据表
            self.cursor.execute('''
                CREATE TABLE IF NOT EXISTS funds (
                    id INT AUTO_INCREMENT PRIMARY KEY COMMENT '主键ID，自增',
                    fund_id VARCHAR(20) NOT NULL COMMENT '基金代码',
                    fund_nm VARCHAR(100) NOT NULL COMMENT '基金名称',
                    price VARCHAR(20) COMMENT '基金当前价格',
                    pre_close VARCHAR(20) COMMENT '前收盘价',
                    price_dt VARCHAR(20) COMMENT '价格日期',
                    increase_rt VARCHAR(20) COMMENT '涨幅比例',
                    volume VARCHAR(20) COMMENT '成交量',
                    amount VARCHAR(20) COMMENT '成交金额',
                    amount_incr VARCHAR(20) COMMENT '成交金额增量',
                    fund_nav VARCHAR(20) COMMENT '基金净值',
                    estimate_value VARCHAR(20) COMMENT '估算价值',
                    discount_rt VARCHAR(20) COMMENT '折溢价率',
                    index_id VARCHAR(20) COMMENT '跟踪指数代码',
                    index_nm VARCHAR(100) COMMENT '跟踪指数名称',
                    index_increase_rt VARCHAR(20) COMMENT '指数涨幅',
                    apply_fee VARCHAR(20) COMMENT '申购费率',
                    apply_status VARCHAR(20) COMMENT '申购状态',
                    redeem_fee VARCHAR(20) COMMENT '赎回费率',
                    redeem_status VARCHAR(20) COMMENT '赎回状态',
                    turnover_rt VARCHAR(20) COMMENT '换手率',
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '记录创建时间'
                )
            ''')

            # 创建AI分析结果表
            self.cursor.execute('''
                CREATE TABLE IF NOT EXISTS ai_analyses (
                    id INT AUTO_INCREMENT PRIMARY KEY COMMENT '主键ID，自增',
                    analysis_content TEXT NOT NULL COMMENT 'AI分析的完整内容',
                    fund_name VARCHAR(100) COMMENT '分析的基金名称',
                    fund_code varchar(20) COMMENT '分析的基金代码',
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '记录创建时间',
                    update_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '记录最后更新时间',
                    date DATE NOT NULL COMMENT '分析数据的日期'
                )
            ''')

            self.conn.commit()
            print("✅ 数据库表创建成功！")
            return True
        except Exception as e:
            print(f"❌ 数据库表创建失败：{e}")
            self.conn.rollback()
            return False

    def save_funds(self, funds) -> bool:
        """保存基金数据到数据库"""
        try:
            # 批量插入基金数据
            for fund in funds:
                self.cursor.execute('''
                    INSERT INTO funds (
                        fund_id, fund_nm, price, pre_close, price_dt,
                        increase_rt, volume, amount, amount_incr,
                        fund_nav, estimate_value, discount_rt, index_id,
                        index_nm, index_increase_rt, apply_fee, apply_status,
                        redeem_fee, redeem_status, turnover_rt
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ''', (
                    fund.fund_id, fund.fund_nm, fund.price, fund.pre_close, fund.price_dt,
                    fund.increase_rt, fund.volume, fund.amount, fund.amount_incr,
                    fund.fund_nav, fund.estimate_value, fund.discount_rt, fund.index_id,
                    fund.index_nm, fund.index_increase_rt, fund.apply_fee, fund.apply_status,
                    fund.redeem_fee, fund.redeem_status, fund.turnover_rt
                ))

            self.conn.commit()
            print(f"✅ 成功保存 {len(funds)} 条基金数据！")
            return True
        except Exception as e:
            print(f"❌ 保存基金数据失败：{e}")
            self.conn.rollback()
            return False

    # 修改save_ai_analysis方法
    def save_ai_analysis(self, analysis_content: str, fund_name: str, fund_code: str) -> bool:
        """保存AI分析结果到数据库"""
        try:
            # 获取当前日期（中国时区）
            china_tz = pytz.timezone('Asia/Shanghai')
            current_date = datetime.now(china_tz).date()
    
            self.cursor.execute('''
                INSERT INTO ai_analyses (analysis_content, fund_name, fund_code, date)
                VALUES (%s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE 
                    analysis_content = VALUES(analysis_content),
                    fund_name = VALUES(fund_name)
            ''', (analysis_content, fund_name, fund_code, current_date))
    
            self.conn.commit()
            print("✅ AI分析结果保存/更新成功！")
            return True
        except Exception as e:
            print(f"❌ 保存AI分析结果失败：{e}")
            self.conn.rollback()
            return False

    def query_to_model(self, model_class: type[T], query: str, params: Optional[tuple] = None) -> List[T]:
        """
        执行查询并将结果映射到指定的模型类
        :param model_class: 模型类，如AIAnalysis
        :param query: SQL查询语句
        :param params: 查询参数
        :return: 模型对象列表
        """
        try:
            if params:
                self.cursor.execute(query, params)
            else:
                self.cursor.execute(query)
            results = self.cursor.fetchall()
            
            model_objects = []
            for row in results:
                # 确保日期字段正确处理
                if 'date' in row and hasattr(model_class, 'date'):
                    # 如果数据库返回的是datetime对象，转换为date
                    if isinstance(row['date'], datetime):
                        row['date'] = row['date'].date()
                model_obj = model_class(**row)
                model_objects.append(model_obj)
            
            return model_objects
        except Exception as e:
            print(f"❌ 查询并映射模型失败：{e}")
            return []