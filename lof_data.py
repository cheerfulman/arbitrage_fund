from dataclasses import dataclass

from httpx import get
import requests
import json


@dataclass
class LOFFund:
    """LOF基金结构体，包含所有关键字段"""
    # 基本信息
    fund_id: str  # 股票代码
    fund_nm: str  # 股票名称
    price: str  # 现价
    pre_close: str  # 昨天价格
    price_dt: str  # 净值日期
    increase_rt: str  # 涨幅
    volume: str  # 当日成交（万）
    amount: str  # 场内份额（万份）
    amount_incr: str  # 场内新增（万份）
    fund_nav: str  # 基金净值
    estimate_value: str  # 实时估值
    discount_rt: str  # 折价率
    index_id: str  # 跟踪指数代码
    index_nm: str  # 跟踪指数
    index_increase_rt: str  # 指数涨幅
    apply_fee: str  # 申购费
    apply_status: str  # 申购状态
    redeem_fee: str  # 赎回费
    redeem_status: str  # 赎回状态
    turnover_rt: str  # 换手率
    nav_dt: str # 净值日期

class LOFDataHandler:
    """LOF基金数据处理类，用于接收和处理HTTP请求返回的LOF基金数据"""
    
    def __init__(self, json_data, sort_by=None):
        """
        初始化LOFDataHandler类
        
        Args:
            json_data: 从API获取的JSON数据
            sort_by: 排序字段，可选值：'discount_rt'（折价率）或 'premium_rt'（溢价率）
        """
        self.raw_data = json_data
        self.page_info = {}  # 确保初始化为字典
        self.lof_list = []
        self.sort_by = sort_by
        
        # 解析JSON数据
        self._parse_data()
    
    def _parse_data(self):
        """解析JSON数据，提取页面信息和LOF基金列表"""
        if not self.raw_data:
            return
            
        # 添加调试信息，打印数据结构概览
        print(f"调试信息：数据类型: {type(self.raw_data)}, 数据键列表: {list(self.raw_data.keys()) if isinstance(self.raw_data, dict) else '不是字典类型'}")
        
        # 确保self.raw_data是字典类型
        if not isinstance(self.raw_data, dict):
            print(f"警告：数据不是字典类型，无法解析页面信息，类型为: {type(self.raw_data)}")
            return
            
        # 提取页面信息 - 增强健壮性，支持不同的数据结构
        if 'page' in self.raw_data:
            # 确保page字段是字典类型
            if isinstance(self.raw_data['page'], dict):
                self.page_info = self.raw_data['page']
            elif isinstance(self.raw_data['page'], (int, str)):
                # 如果page是整数或字符串，构建基本的分页信息
                self.page_info = {
                    'page': int(self.raw_data['page']),
                    'total': self.raw_data.get('total', 1),
                    'records': self.raw_data.get('records', len(self.raw_data.get('rows', [])))
                }
            else:
                print(f"警告：page字段类型异常，类型为: {type(self.raw_data['page'])}")
        
        elif 'rows' in self.raw_data:
            # 如果没有明确的分页信息，使用rows的长度作为记录数
            self.page_info = {
                'page': 1,
                'total': 1,
                'records': len(self.raw_data['rows'])
            }
        else:
            print("警告：未找到页面信息")
        
        # 提取LOF基金列表
        if 'rows' in self.raw_data:
            for row in self.raw_data['rows']:
                if 'cell' in row:
                    self.lof_list.append(row['cell'])
        
        # 根据指定字段进行排序
        if self.lof_list and self.sort_by:
            self._sort_lof_list(self.sort_by)
    
    def _sort_lof_list(self, sort_by):
        """
        根据指定字段对LOF基金列表进行排序
        
        Args:
            sort_by: 排序字段，'discount_rt'（折价率）或 'premium_rt'（溢价率）
        """
        valid_sort_fields = ['discount_rt', 'premium_rt']
        
        if sort_by not in valid_sort_fields:
            print(f"警告：无效的排序字段 '{sort_by}'，支持的字段：{', '.join(valid_sort_fields)}")
            return
        
        try:
            # 定义排序函数，将字符串转换为浮点数进行比较
            def sort_key(item):
                value = item.get(sort_by, '0')
                # 处理可能的百分比符号和空值
                if isinstance(value, str):
                    value = value.replace('%', '')
                try:
                    return float(value)
                except (ValueError, TypeError):
                    return 0.0
            
            # 倒序排序
            self.lof_list.sort(key=sort_key, reverse=True)
            print(f"已按照{sort_by}（{self._get_sort_field_name(sort_by)}）倒序排序")
            
        except Exception as e:
            print(f"排序失败: {e}")
    
    def _get_sort_field_name(self, sort_by):
        """获取排序字段的中文名称"""
        field_names = {
            'discount_rt': '折价率',
            'premium_rt': '溢价率'
        }
        return field_names.get(sort_by, sort_by)
    
    def print_all_fields(self):
        """打印所有可用字段名，用于调试"""
        if not self.lof_list:
            print("没有LOF基金数据")
            return
            
        # 显示所有可用字段
        print("所有可用字段：")
        all_fields = set()
        for lof in self.lof_list:
            all_fields.update(lof.keys())
        
        print(sorted(all_fields))
        print(f"共 {len(all_fields)} 个字段")
    
    def print_lof_list(self):
        """打印LOF基金列表信息，按照用户指定的JSON字段显示"""
        if not self.lof_list:
            print("没有LOF基金数据")
            return
            
        print(f"共找到 {len(self.lof_list)} 条LOF基金数据：")
        # 计算表格总宽度
        total_width = 10 + 20 + 8 + 10 + 12 + 10 + 15 + 12 + 12 + 10 + 15 + 12 + 12 + 12 + 15 + 12 + 15 + 15 + 12 + 15 + 10
        print("-" * total_width)
        
        # 按照用户指定的JSON字段打印表头
        print(f"{'股票代码':<10} {'股票名称':<20} {'现价':<8} {'昨天价格':<10} {'净值日期':<12} {'涨幅':<10} {'当日成交（万）':<15} {'场内份额（万份）':<12} {'场内新增（万份）':<12} {'基金净值':<10} {'实时估值':<15} {'折价率':<12} {'跟踪指数代码':<12} {'跟踪指数':<15} {'指数涨幅':<12} {'申购费':<15} {'申购状态':<15} {'赎回费':<12} {'赎回状态':<15} {'换手率':<10}")
        print("-" * total_width)
        
        # 打印每条基金信息
        for lof in self.lof_list:
            code = lof.get('fund_id', '')  # 股票代码
            name = lof.get('fund_nm', '')  # 股票名称
            price = lof.get('price', '')  # 现价
            pre_close = lof.get('pre_close', '')  # 昨天价格
            price_dt = lof.get('price_dt', '')  # 净值日期
            increase_rt = lof.get('increase_rt', '')  # 涨幅
            volume = lof.get('volume', '')  # 当日成交（万）
            amount = lof.get('amount', '')  # 场内份额（万份）
            amount_incr = lof.get('amount_incr', '')  # 场内新增（万份）
            fund_nav = lof.get('fund_nav', '')  # 基金净值
            estimate_value = lof.get('estimate_value', '')  # 实时估值
            discount_rt = lof.get('discount_rt', '')  # 折价率
            index_id = lof.get('index_id', '')  # 跟踪指数代码
            index_nm = lof.get('index_nm', '')  # 跟踪指数
            index_increase_rt = lof.get('index_increase_rt', '')  # 指数涨幅
            apply_fee = lof.get('apply_fee', '')  # 申购费
            apply_status = lof.get('apply_status', '')  # 申购状态
            redeem_fee = lof.get('redeem_fee', '')  # 赎回费
            redeem_status = lof.get('redeem_status', '')  # 赎回状态
            turnover_rt = lof.get('turnover_rt', '')  # 换手率
            
            print(f"{code:<10} {name:<20} {price:<8} {pre_close:<10} {price_dt:<12} {increase_rt:<10} {volume:<15} {amount:<12} {amount_incr:<12} {fund_nav:<10} {estimate_value:<15} {discount_rt:<12} {index_id:<12} {index_nm:<15} {index_increase_rt:<12} {apply_fee:<15} {apply_status:<15} {redeem_fee:<12} {redeem_status:<15} {turnover_rt:<10}")
    
    def get_lof_list(self):
        """获取LOF基金列表"""
        return self.lof_list
    
    def get_page_info(self):
        """获取页面信息"""
        return self.page_info

    def _is_qualified_fund(self, lof):
        """
        检查基金是否符合条件：必须开放申购、开放赎回，且溢价率>4%
        
        Args:
            lof: 基金数据字典
            
        Returns:
            bool: 是否符合条件
        """
        # 检查申购状态，只要不是暂停申购即可
        apply_status = lof.get('apply_status', '')
        if apply_status == '暂停申购':
            return False
        
        # 检查赎回状态
        redeem_status = lof.get('redeem_status', '')
        if redeem_status != '开放赎回':
            return False
        
        # 检查溢价率
        discount_rt = lof.get('discount_rt', '')
        try:
            # 移除百分号并转换为浮点数
            if isinstance(discount_rt, str):
                discount_rt = discount_rt.replace('%', '')
            discount_rt_value = float(discount_rt)
            return discount_rt_value > 4
        except (ValueError, TypeError):
            return False
    
    def get_fund_struct_array(self):
        """获取只包含特定字段的LOFFund结构体数组"""
        struct_array = []
        for lof in self.lof_list:
            # 创建LOFFund对象，包含所有指定字段
            fund = LOFFund(
                fund_id=lof.get('fund_id', ''),
                fund_nm=lof.get('fund_nm', ''),
                price=lof.get('price', ''),
                pre_close=lof.get('pre_close', ''),
                price_dt=lof.get('price_dt', ''),
                increase_rt=lof.get('increase_rt', ''),
                volume=lof.get('volume', ''),
                amount=lof.get('amount', ''),
                amount_incr=lof.get('amount_incr', ''),
                fund_nav=lof.get('fund_nav', ''),
                estimate_value=lof.get('estimate_value', ''),
                discount_rt=lof.get('discount_rt', ''),
                index_id=lof.get('index_id', ''),
                index_nm=lof.get('index_nm', ''),
                index_increase_rt=lof.get('index_increase_rt', ''),
                apply_fee=lof.get('apply_fee', ''),
                apply_status=lof.get('apply_status', ''),
                redeem_fee=lof.get('redeem_fee', ''),
                redeem_status=lof.get('redeem_status', ''),
                turnover_rt=lof.get('turnover_rt', ''),
                nav_dt = lof.get('nav_dt')
            )
            struct_array.append(fund)
        return struct_array

    def get_deserve_arbitrage_fund(self):
        """获取只包含特定字段的LOFFund结构体数组"""
        struct_array = []
        for lof in self.lof_list:
            # 检查基金是否符合条件
            if self._is_qualified_fund(lof):
                # 创建LOFFund对象，包含所有指定字段
                fund = LOFFund(
                    fund_id=lof.get('fund_id', ''),
                    fund_nm=lof.get('fund_nm', ''),
                    price=lof.get('price', ''),
                    pre_close=lof.get('pre_close', ''),
                    price_dt=lof.get('price_dt', ''),
                    increase_rt=lof.get('increase_rt', ''),
                    volume=lof.get('volume', ''),
                    amount=lof.get('amount', ''),
                    amount_incr=lof.get('amount_incr', ''),
                    fund_nav=lof.get('fund_nav', ''),
                    estimate_value=lof.get('estimate_value', ''),
                    discount_rt=lof.get('discount_rt', ''),
                    index_id=lof.get('index_id', ''),
                    index_nm=lof.get('index_nm', ''),
                    index_increase_rt=lof.get('index_increase_rt', ''),
                    apply_fee=lof.get('apply_fee', ''),
                    apply_status=lof.get('apply_status', ''),
                    redeem_fee=lof.get('redeem_fee', ''),
                    redeem_status=lof.get('redeem_status', ''),
                    turnover_rt=lof.get('turnover_rt', ''),
                    nav_dt = lof.get('nav_dt')
                )
                struct_array.append(fund)
        return struct_array


def fetch_lof_data():
    url = 'https://www.jisilu.cn/data/lof/index_lof_list/?___jsl=LST___t=1767022699452&rp=25&page=1'
    
    headers = {
        'accept': 'application/json, text/javascript, */*; q=0.01',
        'accept-language': 'en,zh-CN;q=0.9,zh;q=0.8,zh-TW;q=0.7',
        'priority': 'u=1, i',
        'referer': 'https://www.jisilu.cn/data/lof/',
        'sec-ch-ua': '"Google Chrome";v="143", "Chromium";v="143", "Not A(Brand";v="24"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"macOS"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'same-origin',
        'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36',
        'x-requested-with': 'XMLHttpRequest'
    }
    
    cookies = {
        'kbzw__Session': 'uujg96a2vmodsijvd10b9bbe65',
        'Hm_lvt_164fe01b1433a19b507595a43bf58262': '1767022643',
        'HMACCOUNT': 'EAB3FAD68D80D2E8',
        'kbz_newcookie': '1',
        'Hm_lpvt_164fe01b1433a19b507595a43bf58262': '1767022665'
    }
    
    try:
        response = requests.get(url, headers=headers, cookies=cookies)
        response.raise_for_status()  # 检查请求是否成功
        return response.json()  # 返回JSON响应数据
    except requests.exceptions.RequestException as e:
        print(f'请求发生错误: {e}')
        return None


def fetch_qdii_data():
    url = 'https://www.jisilu.cn/data/qdii/qdii_list/E?___jsl=LST___t=1768899909768&rp=22&page=1'
    
    headers = {
        'accept': 'application/json, text/javascript, */*; q=0.01',
        'accept-language': 'en,zh-CN;q=0.9,zh;q=0.8,zh-TW;q=0.7',
        'priority': 'u=1, i',
        'referer': 'https://www.jisilu.cn/data/qdii/',
        'sec-ch-ua': '"Google Chrome";v="143", "Chromium";v="143", "Not A(Brand";v="24"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"macOS"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'same-origin',
        'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36',
        'x-requested-with': 'XMLHttpRequest'
    }
    
    cookies = {
        'kbzw__Session': 'uujg96a2vmodsijvd10b9bbe65',
        'Hm_lvt_164fe01b1433a19b507595a43bf58262': '1767022643',
        'HMACCOUNT': 'EAB3FAD68D80D2E8',
        'kbz_newcookie': '1',
        'Hm_lpvt_164fe01b1433a19b507595a43bf58262': '1768839049'
    }
    
    try:
        response = requests.get(url, headers=headers, cookies=cookies)
        response.raise_for_status()  # 检查请求是否成功
        return response.json()  # 返回JSON响应数据
    except requests.exceptions.RequestException as e:
        print(f'QDII请求发生错误: {e}')
        return None


def merge_fund_data(lof_data, qdii_data):
    """
    合并LOF和QDII基金数据
    
    Args:
        lof_data: LOF基金数据
        qdii_data: QDII基金数据
        
    Returns:
        合并后的数据，格式与原始数据相同
    """
    if not lof_data:
        return qdii_data
    if not qdii_data:
        return lof_data
    
    merged_data = {
        'page': 1,
        'total': 1,
        'records': 0,
        'rows': []
    }
    
    # 添加LOF基金数据
    if 'rows' in lof_data:
        merged_data['rows'].extend(lof_data['rows'])
    
    # 添加QDII基金数据
    if 'rows' in qdii_data:
        merged_data['rows'].extend(qdii_data['rows'])
    
    # 更新记录数
    merged_data['records'] = len(merged_data['rows'])
    
    return merged_data


def fetch_all_fund_data():
    """
    获取所有基金数据，包括LOF和QDII
    
    Returns:
        合并后的基金数据
    """
    # 获取LOF基金数据
    print('正在获取LOF基金数据...')
    lof_data = fetch_lof_data()
    
    # 获取QDII基金数据
    print('正在获取QDII基金数据...')
    qdii_data = fetch_qdii_data()
    
    # 合并数据
    print('正在合并数据...')
    return merge_fund_data(lof_data, qdii_data)


if __name__ == '__main__':
    # 获取所有基金数据（LOF + QDII）
    data = fetch_all_fund_data()
    
    if data:
        print('请求成功，正在处理数据...')
        print('-' * 60)
        
        # 创建LOFDataHandler实例，按照溢价率倒序排序
        fund_handler = LOFDataHandler(data, sort_by='discount_rt')
        
        # 打印所有可用字段（用于调试）
        fund_handler.print_all_fields()
        print('-' * 60)
        
        # 打印基金列表，按照用户指定的JSON字段显示
        fund_handler.print_lof_list()
        
        # 获取并打印页面信息
        page_info = fund_handler.get_page_info()
        total_width = 10 + 20 + 8 + 10 + 12 + 10 + 15 + 12 + 12 + 10 + 15 + 12 + 12 + 15 + 12 + 15 + 15 + 12 + 15 + 10
        print("-" * total_width)
        
        # 显示页面信息
        if isinstance(page_info, dict):
            current_page = page_info.get('page', 1)
            total_pages = page_info.get('total', 1)
            records = page_info.get('records', len(fund_handler.get_lof_list()))
            print(f"dict 页面信息：当前页 {current_page}/{total_pages}，共 {records} 条记录")
        elif isinstance(page_info, (int, str)):
            print(f"str 页面信息：当前页 {page_info}/1，共 {len(fund_handler.get_lof_list())} 条记录")
        else:
            print(f"页面信息：共 {len(fund_handler.get_lof_list())} 条记录")
    else:
        print('请求失败')