"""
Coze API客户端类，用于与Coze API进行交互
This example describes how to use the chat interface to initiate conversations,
poll the status of the conversation, and obtain the messages after the conversation is completed.
"""

import os
import time
from typing import List, Optional, Dict, Any
# Our official coze sdk for Python [cozepy](https://github.com/coze-dev/coze-py)
from cozepy import COZE_CN_BASE_URL, Coze, TokenAuth, Message, ChatStatus, MessageContentType, MessageType


class CozeAPIClient:
    """
    Coze API客户端类，提供与Coze API交互的功能
    """

    def __init__(self, api_token: str, bot_id: str, user_id: str = '123456789', base_url: str = COZE_CN_BASE_URL):
        """
        初始化Coze API客户端
        
        Args:
            api_token: Coze API访问令牌
            bot_id: 机器人ID
            user_id: 用户ID，用于标识用户身份
            base_url: API基础URL，默认为中国区地址
        """
        self.api_token = api_token
        self.bot_id = bot_id
        self.user_id = user_id
        self.base_url = base_url
        
        # 初始化Coze客户端
        self.coze = Coze(auth=TokenAuth(token=self.api_token), base_url=self.base_url)
    
    def send_message(self, question: str, additional_messages: Optional[List[Message]] = None) -> Dict[str, Any]:
        """
        发送消息并获取响应
        
        Args:
            question: 用户的问题
            additional_messages: 额外的消息列表（可选）
            
        Returns:
            包含响应内容和使用信息的字典
        """
        # 构建消息列表
        messages = []
        if additional_messages:
            messages.extend(additional_messages)
        messages.append(Message.build_user_question_text(question))
        
        # 创建并轮询聊天
        chat_poll = self.coze.chat.create_and_poll(
            bot_id=self.bot_id,
            user_id=self.user_id,
            additional_messages=messages,
        )
        
        # 构建响应结果
        response_content = []
        for message in chat_poll.messages:
            if message.type == MessageType.ANSWER: # 只拼接答案
                response_content.append(message.content)
        
        return {
            'content': ''.join(response_content),
            'status': chat_poll.chat.status,
            'token_count': chat_poll.chat.usage.token_count if hasattr(chat_poll.chat, 'usage') else 0
        }
    
    def get_bot_info(self) -> Dict[str, Any]:
        """
        获取机器人信息（示例方法，可以根据需要扩展）
        
        Returns:
            机器人信息字典
        """
        # 这里可以添加获取机器人信息的逻辑
        return {
            'bot_id': self.bot_id,
            'base_url': self.base_url
        }


# 示例用法
if __name__ == '__main__':
    # 从环境变量或直接设置获取API令牌
    coze_api_token = 'cztei_qn1Z2DZLCPxSgEi3eFpq9Jm6UDB2QdbbRkOHSapWNy1r5SRWo9aHHlVMZiDNOilYx'
    bot_id = '7591133708552814643'
    user_id = '123456789'
    
    # 创建Coze API客户端实例
    client = CozeAPIClient(api_token=coze_api_token, bot_id=bot_id, user_id=user_id)
    
    # 发送消息并获取响应
    response = client.send_message("")
    
    # 打印响应内容
    print("Response:", response['content'])
    print("Status:", response['status'])
    print("Token usage:", response['token_count'])