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
    nav_dt: str  # 净值日期
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
            "password": password,
            "autocommit": True,  # 启用自动提交
            "pool_name": "fund_arbitrage_pool",  # 连接池名称
            "pool_size": 5,  # 连接池大小
            "pool_reset_session": True,  # 重置会话
            "connect_timeout": 10,  # 连接超时时间
            "read_timeout": 30,  # 读取超时时间
            "write_timeout": 30,  # 写入超时时间
            "buffered": True,  # 缓冲查询结果
            "charset": "utf8mb4"  # 添加字符集参数，解决中文乱码问题
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

    def is_connected(self) -> bool:
        """检查数据库连接是否有效"""
        if not self.conn:
            return False
        try:
            # 执行简单查询检查连接是否有效
            self.conn.ping(reconnect=False)
            return True
        except:
            return False

    def ensure_connection(self) -> bool:
        """确保数据库连接有效，如果无效则重新连接"""
        if not self.is_connected():
            print("⚠️ 数据库连接已断开，正在尝试重新连接...")
            return self.connect()
        return True

    def create_tables(self) -> bool:
        """创建数据库表"""
        if not self.ensure_connection():
            return False
            
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
                    nav_dt VARCHAR(20) COMMENT '净值日期',
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
                    date DATE COMMENT '数据插入日期',
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '记录创建时间',
                    UNIQUE KEY unique_fund (fund_id, fund_nm, nav_dt) COMMENT '基金代码、名称和净值日期的唯一索引'
                )
            ''')

            # 创建AI分析结果表
            self.cursor.execute('''
                CREATE TABLE IF NOT EXISTS ai_analyses (
                    id INT AUTO_INCREMENT PRIMARY KEY COMMENT '主键ID，自增',
                    analysis_content TEXT NOT NULL COMMENT 'AI分析的完整内容',
                    fund_name VARCHAR(100) COMMENT '分析的基金名称',
                    fund_code varchar(20) COMMENT '分析的基金代码',
                    nav_dt VARCHAR(20) COMMENT '净值日期',
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
        if not self.ensure_connection():
            return False
            
        try:
            # 批量插入基金数据
            for fund in funds:
                self.cursor.execute('''
                    INSERT INTO funds (
                        fund_id, fund_nm, price, pre_close, price_dt,
                        nav_dt, increase_rt, volume, amount, amount_incr,
                        fund_nav, estimate_value, discount_rt, index_id,
                        index_nm, index_increase_rt, apply_fee, apply_status,
                        redeem_fee, redeem_status, turnover_rt, date
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE
                        price = VALUES(price),
                        pre_close = VALUES(pre_close),
                        price_dt = VALUES(price_dt),
                        increase_rt = VALUES(increase_rt),
                        volume = VALUES(volume),
                        amount = VALUES(amount),
                        amount_incr = VALUES(amount_incr),
                        fund_nav = VALUES(fund_nav),
                        estimate_value = VALUES(estimate_value),
                        discount_rt = VALUES(discount_rt),
                        index_id = VALUES(index_id),
                        index_nm = VALUES(index_nm),
                        index_increase_rt = VALUES(index_increase_rt),
                        apply_fee = VALUES(apply_fee),
                        apply_status = VALUES(apply_status),
                        redeem_fee = VALUES(redeem_fee),
                        redeem_status = VALUES(redeem_status),
                        turnover_rt = VALUES(turnover_rt),
                        date = VALUES(date),
                        created_at = CURRENT_TIMESTAMP
                ''', (
                    fund.fund_id, fund.fund_nm, fund.price, fund.pre_close, fund.price_dt,
                    fund.nav_dt, fund.increase_rt, fund.volume, fund.amount, fund.amount_incr,
                    fund.fund_nav, fund.estimate_value, fund.discount_rt, fund.index_id,
                    fund.index_nm, fund.index_increase_rt, fund.apply_fee, fund.apply_status,
                    fund.redeem_fee, fund.redeem_status, fund.turnover_rt, datetime.now().date()
                ))

            self.conn.commit()
            print(f"✅ 成功保存 {len(funds)} 条基金数据！")
            return True
        except Exception as e:
            print(f"❌ 保存基金数据失败：{e}")
            self.conn.rollback()
            return False

    # 修改save_ai_analysis方法
    def save_ai_analysis(self, analysis_content: str, fund_name: str, fund_code: str, nav_dt: str = None, analysis_date: date = None) -> bool:
        """保存AI分析结果到数据库"""
        if not self.ensure_connection():
            return False
            
        try:
            # 如果没有传入nav_dt，使用当前日期
            if not nav_dt:
                nav_dt = datetime.now().date()
            
            # 如果没有传入analysis_date，使用当前日期
            if not analysis_date:
                analysis_date = datetime.now().date()
    
            self.cursor.execute('''
                INSERT INTO ai_analyses (analysis_content, fund_name, fund_code, nav_dt, date)
                VALUES (%s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE 
                    analysis_content = VALUES(analysis_content),
                    fund_name = VALUES(fund_name),
                    nav_dt = VALUES(nav_dt),
                    date = VALUES(date)
            ''', (analysis_content, fund_name, fund_code, nav_dt, analysis_date))
    
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
        if not self.ensure_connection():
            return []
            
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
            # 如果是连接错误，尝试重新连接一次
            if "Lost connection" in str(e) or "2013" in str(e):
                print("⚠️ 检测到连接错误，尝试重新连接...")
                if self.connect():
                    print("✅ 重新连接成功，再次尝试查询...")
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
                    except Exception as e2:
                        print(f"❌ 重新查询失败：{e2}")
            return []
    
    def get_funds_by_date(self, date: str) -> dict:
        """
        根据数据插入日期获取基金信息，返回字典格式
        :param date: 数据插入日期
        :return: 基金信息字典，以(fund_id, fund_nm)为键
        """
        if not self.ensure_connection():
            return {}
        
        try:
            # 查询指定插入日期的所有基金信息
            query = """
                SELECT fund_id, fund_nm, estimate_value, price, apply_status 
                FROM funds 
                WHERE date = %s
            """
            
            self.cursor.execute(query, (date,))
            results = self.cursor.fetchall()
            
            # 将结果构建成字典
            fund_dict = {}
            for fund in results:
                fund_dict[fund['fund_id']] = fund
            
            return fund_dict
        except Exception as e:
            print(f"❌ 根据插入日期查询基金信息失败：{e}")
            return {}
    
    def get_funds_by_nav_dt(self, nav_dt: str) -> dict:
        """
        根据净值日期获取基金信息，返回字典格式
        :param nav_dt: 净值日期
        :return: 基金信息字典，以(fund_id, fund_nm)为键
        """
        if not self.ensure_connection():
            return {}
        
        try:
            # 查询指定净值日期的所有基金信息
            query = """
                SELECT fund_id, fund_nm, estimate_value, price, apply_status 
                FROM funds 
                WHERE nav_dt = %s
            """
            
            self.cursor.execute(query, (nav_dt,))
            results = self.cursor.fetchall()
            
            # 将结果构建成字典
            fund_dict = {}
            for fund in results:
                fund_dict[(fund['fund_id'], fund['fund_nm'])] = fund
            
            return fund_dict
        except Exception as e:
            print(f"❌ 根据净值日期查询基金信息失败：{e}")
            return {}