# 导入环境变量管理库
from dotenv import load_dotenv
import os

# 加载 .env 文件
load_dotenv()

# 其他导入保持不变
from lof_data import fetch_lof_data, LOFDataHandler
from coze_api import CozeAPIClient
from db_utils import DatabaseManager, AIAnalysis
from fastapi import FastAPI, HTTPException
from apscheduler.schedulers.background import BackgroundScheduler
from datetime import datetime
import uvicorn
import logging
import json

# 配置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# 初始化FastAPI应用
app = FastAPI(title="LOF基金套利分析API")

# 从环境变量获取数据库配置（可选，也可以改为环境变量）
db_manager = DatabaseManager(
    host=os.getenv("DB_HOST", "db"),
    port=int(os.getenv("DB_PORT", 3306)),
    database=os.getenv("DB_NAME", "arbitrage_fund"),
    user=os.getenv("DB_USER", "root"),
    password=os.getenv("DB_PASSWORD", "root")
)

# 连接数据库
if not db_manager.connect():
    logger.error("数据库连接失败，程序退出")
    exit(1)

# 创建数据库表
db_manager.create_tables()


def format_lof_funds(funds):
    """将LOFFund对象列表转换为字符串格式"""
    result = "以下是折价率最高的5只LOF基金信息：\n"
    for i, fund in enumerate(funds, 1):
        result += f"\n{i}. 基金基本信息："
        result += f"\n   基金代码：{fund.fund_id}, 基金名称：{fund.fund_nm}"
        result += f"\n   现价：{fund.price}, 昨天价格：{fund.pre_close}, 净值日期：{fund.price_dt}"
        result += f"\n   涨幅：{fund.increase_rt}, 当日成交：{fund.volume}（万）"
        result += f"\n   场内份额：{fund.amount}（万份）, 场内新增：{fund.amount_incr}（万份）"
        result += f"\n   基金净值：{fund.fund_nav}, 实时估值：{fund.estimate_value}"
        result += f"\n   折价率：{fund.discount_rt}, 换手率：{fund.turnover_rt}"
        result += f"\n   跟踪指数代码：{fund.index_id}, 跟踪指数：{fund.index_nm}"
        result += f"\n   指数涨幅：{fund.index_increase_rt}"
        result += f"\n   申购费：{fund.apply_fee}, 申购状态：{fund.apply_status}"
        result += f"\n   赎回费：{fund.redeem_fee}, 赎回状态：{fund.redeem_status}"
    return result


def run_analysis():
    """执行基金分析的主函数"""
    logger.info(f"开始执行基金分析任务，当前时间：{datetime.now()}")
    
    try:
        # 获取LOF基金数据
        data = fetch_lof_data()

        if data:
            # 创建LOFDataHandler实例，按照溢价率倒序排序
            lof_handler = LOFDataHandler(data, sort_by='discount_rt')
            topFiveLof = lof_handler.get_lof_struct_array()[:5]

            # 将基金数据保存到数据库
            db_manager.save_funds(topFiveLof)

            # 将topFiveLof转换为字符串
            funds_str = format_lof_funds(topFiveLof)

            # 调用Coze API
            try:
                # 从环境变量获取配置
                coze_api_token = os.getenv("COZE_API_TOKEN")
                bot_id = os.getenv("COZE_BOT_ID")
                user_id = '123456789'  # 这个也可以放到环境变量中

                # 检查必要的环境变量是否存在
                if not coze_api_token or not bot_id:
                    logger.error("缺少COZE_API_TOKEN或COZE_BOT_ID环境变量")
                    return

                client = CozeAPIClient(api_token=coze_api_token, bot_id=bot_id, user_id=user_id)

                # 构建问题，要求返回指定的JSON格式
                question = (f"{funds_str}\n\n请从以上基金中分析今日是否有适合套利的，")
                question += f"并详细分析各基金的套利优缺点。从场内份额、溢价率、申购费、开放和赎回状态等多角度分析，并且附上推荐理由和盈利预期多少\n"
                question += f"请严格按照以下JSON格式返回结果（不要添加任何其他解释性文字）：\n"
                question += f"[\n"
                question += f"   {{\n"
                question += f"     \"fund_name\": \"基金名称\",\n"
                question += f"     \"fund_code\": \"基金代码\",\n"
                question += f"     \"analysis_content\": \"核心结论：是否值得套利？（值得/需谨慎）；溢价原因：[短期/长期驱动因素]；溢价抹平预期：[快速/缓慢]（时间周期及逻辑）；未来涨幅：[看好/看衰/中性]（理由）；盈利预期：[估算值]\"\n"
                question += f"   }}\n"
                question += f"]\n"
                question += f"如果没有合适的基金套利机会，请返回：[]"

                logger.info("正在向Coze API发送请求...")
                response = client.send_message(question)

                # 打印完整响应内容
                logger.info("\nCoze API完整响应：")
                logger.info(response['content'])
                logger.info(f"\nToken使用量：{response['token_count']}")

                # 解析AI返回的JSON格式数据
                try:
                    ai_results = json.loads(response['content'])
                    
                    # 如果有套利机会，保存每只基金的分析结果
                    if isinstance(ai_results, list) and len(ai_results) > 0:
                        for result in ai_results:
                            if all(key in result for key in ['fund_name', 'fund_code', 'analysis_content']):
                                # 部分1：解析成功时的调用
                                db_manager.save_ai_analysis(
                                    analysis_content=result['analysis_content'],
                                    fund_name=result['fund_name'],  # 删除了token_count参数
                                    fund_code=result['fund_code']
                                )

                        logger.info(f"成功保存 {len(ai_results)} 条AI分析结果")
                    else:
                        logger.info("没有合适的基金套利机会")

                except json.JSONDecodeError as e:
                    logger.error(f"解析AI返回的JSON格式失败：{e}")
                    # 如果JSON解析失败，保存原始响应
                    db_manager.save_ai_analysis(
                        analysis_content=response['content'],
                        fund_name="",
                        fund_code=""
                    )

            except Exception as e:
                logger.error(f"调用Coze API时发生错误：{e}")
        else:
            logger.error("获取基金数据失败")
            
    except Exception as e:
        logger.error(f"执行分析任务时发生错误：{e}")


@app.get("/api/ai-analyses/{date}")
async def get_ai_analyses(date: str):
    """
    获取指定日期的AI分析结果
    - date: 日期格式，例如：2026-01-09
    """
    try:
        # 验证日期格式
        datetime.strptime(date, "%Y-%m-%d")
        
        # 从数据库获取数据并直接映射到AIAnalysis模型
        query = "SELECT * FROM ai_analyses WHERE date = %s"
        ai_analyses = db_manager.query_to_model(AIAnalysis, query, (date,))

        data_list = []
        for ai_analyse in ai_analyses:
            data_list.append({
                "data": ai_analyse.analysis_content,
                "title": f"{ai_analyse.fund_name}({ai_analyse.fund_code})",
            })

        return {
            "status": "success",
            "date": date,
            "count": len(ai_analyses),
            "data": data_list
        }
        
    except ValueError:
        raise HTTPException(status_code=400, detail="日期格式错误，应为YYYY-MM-DD")
    except Exception as e:
        logger.error(f"查询AI分析结果时发生错误：{e}")
        raise HTTPException(status_code=500, detail="服务器内部错误")


@app.post("/api/trigger-analysis")
async def trigger_analysis():
    """手动触发基金分析任务"""
    try:
        # 异步执行run_analysis
        import threading
        threading.Thread(target=run_analysis).start()
        return {"status": "success", "message": "分析任务已开始执行"}
    except Exception as e:
        logger.error(f"触发分析任务时发生错误：{e}")
        raise HTTPException(status_code=500, detail="服务器内部错误")


# 启动定时任务
scheduler = BackgroundScheduler()
# 每天早上9点执行
scheduler.add_job(run_analysis, 'cron', hour=9, minute=0)
scheduler.start()
logger.info("定时任务已启动，每天早上9点执行基金分析")


if __name__ == '__main__':
    # 启动FastAPI服务
    uvicorn.run(app, host="0.0.0.0", port=8000)