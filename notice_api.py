# wechat_notify.py
import requests
import json
import time
from serverchan_sdk import sc_send

def send_wechat_message(content, title=None):
    """
    向企业微信应用发送消息（推送到个人微信）
    """
    # ========== ⚠️ 请修改为你自己的信息 ==========
    CORP_ID = 'wwaaaaf682ae99a77a'  # 替换
    SECRET = 'IBeKGvZw1HJ_YccbpLZ50SmmPJNdDDmZT0-MzMdqMys'  # 替换
    AGENT_ID = 1000002  # 替换
    TO_USER = 'Cheer'  # 你的账号（通讯录里的账号）
    # =============================================

    # 1. 获取 access_token（有效期 2 小时）
    token_url = f"https://qyapi.weixin.qq.com/cgi-bin/gettoken?corpid={CORP_ID}&corpsecret={SECRET}"

    try:
        resp = requests.get(token_url, timeout=10)
        result = resp.json()
        if result['errcode'] != 0:
            print("❌ 获取 token 失败:", result['errmsg'])
            return False
        access_token = result['access_token']
        print("✅ 获取 token 成功:", access_token)
    except Exception as e:
        print("网络错误:", e)
        return False

    # 2. 发送文本消息
    msg_url = "https://qyapi.weixin.qq.com/cgi-bin/message/send"
    payload = {
        "touser": TO_USER,
        "msgtype": "text",
        "agentid": AGENT_ID,
        "text": {"content": content},
        "safe": 0
    }

    for i in range(1):  # 最多重试 3 次
        try:
            response = requests.post(
                msg_url,
                params={'access_token': access_token},
                json=payload,
                timeout=10
            )
            res = response.json()
            if res['errcode'] == 0:
                print("✅ 消息已成功发送到你的微信！")
                return True
            else:
                print(f"❌ 第{i + 1}次发送失败:", res['errmsg'])
                time.sleep(1)
        except Exception as e:
            print(f"❌ 第{i + 1}次网络异常:", e)
            time.sleep(2)

    return False



# 使用server酱进行微信推送
def send_server_jiang(content: str, title:str ="" ):
    # 发送消息
    sendKey = "SCT307611ToIdCj4OrpvnySBqCfdaQW5i9"
    title = title
    desp = content
    options = {"tags": "服务器报警|图片"}  # 可选参数

    response = sc_send(sendKey, title, desp, options)

    print(response)


def send_app_server_jiang(content: str, title:str ="默认" ):
    # 发送消息
    sendKey = "sctp14441tk5stujk36tahdczq7r2lqz"
    title = title
    options = {"tags": "服务器报警|图片"}  # 可选参数

    response = sc_send(sendKey, title, content, options)
    print(response)
# === 使用示例 ===
if __name__ == '__main__':
    send_app_server_jiang("🎉 你好！这是来自服务器的问候。\n当前时间：%s" % time.strftime("%Y-%m-%d %H:%M"))
    # send_wechat_message("🎉 你好！这是来自服务器的问候。\n当前时间：%s" % time.strftime("%Y-%m-%d %H:%M"))
