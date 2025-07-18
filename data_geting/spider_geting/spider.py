#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
智联招聘数据分析岗位爬虫
爬取智联招聘网站上数据分析相关的工作岗位信息
"""

import requests
import json
import time
import random
import pandas as pd
from datetime import datetime
import os
from urllib.parse import quote
import logging

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('zhilian_spider.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)

class ZhilianSpider:
    def __init__(self):
        self.session = requests.Session()
        self.base_url = "########################"
        self.search_url = "########################"
        self.job_detail_url = "########################"
        
        # 设置请求头，模拟浏览器访问
        self.headers = {
            'User-Agent': '####################################',
            'Accept': '#####################',
            'Accept-Language': '#####################',
            'Accept-Encoding': '#####################',
            'Connection': '#####################',
            'Referer': '#####################',
            'Origin': '#####################',
            'sec-ch-ua': '#####################',
            'sec-ch-ua-mobile': '#',
            'sec-ch-ua-platform': '#####################',
            'Sec-Fetch-Dest': '#####################',
            'Sec-Fetch-Mode': '#####################',
            'Sec-Fetch-Site': '#####################'
        }
        
        self.session.headers.update(self.headers)
        
        # 数据分析相关关键词
        self.keywords = [
            '数据分析师',
            '数据挖掘',
            '数据工程师',
            '商业分析师',
            'BI分析师',
            '数据运营',
            '数据产品经理',
            '机器学习工程师',
            '算法工程师',
            '数据科学家'
        ]
        
        # 存储爬取的数据
        self.jobs_data = []
        
    def search_jobs(self, keyword, city='全国', page=1, page_size=30):
        """
        搜索工作岗位
        
        Args:
            keyword: 搜索关键词
            city: 城市
            page: 页码
            page_size: 每页数量
            
        Returns:
            dict: 搜索结果
        """
        try:
            params = {
                'start': (page - 1) * page_size,
                'pageSize': page_size,
                'cityId': self._get_city_id(city),
                'workExperience': -1,
                'education': -1,
                'companyType': -1,
                'employmentType': -1,
                'jobWelfareTag': -1,
                'kw': keyword,
                'kt': 3
            }
            
            logging.info(f"请求URL: {self.search_url}")
            logging.info(f"请求参数: {params}")
            
            response = self.session.get(self.search_url, params=params, timeout=10)
            response.raise_for_status()
            
            # 记录响应状态和内容类型
            logging.info(f"响应状态码: {response.status_code}")
            logging.info(f"响应内容类型: {response.headers.get('content-type', 'unknown')}")
            
            # 检查响应内容
            content = response.text.strip()
            if not content:
                logging.error("响应内容为空")
                return None
            
            # 记录响应内容的前500个字符用于调试
            logging.debug(f"响应内容预览: {content[:500]}")
            
            # 尝试解析JSON
            try:
                data = response.json()
            except json.JSONDecodeError as je:
                logging.error(f"JSON解析失败: {je}")
                logging.error(f"响应内容: {content[:1000]}")
                return None
            
            # 检查返回的数据结构
            if not isinstance(data, dict):
                logging.error(f"响应数据格式错误，期望dict，得到: {type(data)}")
                return None
            
            results = data.get('data', {}).get('results', [])
            logging.info(f"搜索关键词 '{keyword}' 第 {page} 页，获取到 {len(results)} 条结果")
            
            return data
            
        except requests.RequestException as e:
            logging.error(f"搜索请求失败: {e}")
            return None
        except Exception as e:
            logging.error(f"搜索过程中发生意外错误: {e}")
            return None
    
    def get_job_detail(self, job_id):
        """
        获取职位详细信息
        
        Args:
            job_id: 职位ID
            
        Returns:
            dict: 职位详细信息
        """
        try:
            params = {
                'number': job_id
            }
            
            response = self.session.get(self.job_detail_url, params=params, timeout=10)
            response.raise_for_status()
            
            data = response.json()
            return data.get('data', {})
            
        except requests.RequestException as e:
            logging.error(f"获取职位详情失败 (ID: {job_id}): {e}")
            return None
        except json.JSONDecodeError as e:
            logging.error(f"职位详情JSON解析失败 (ID: {job_id}): {e}")
            return None
    
    def _get_city_id(self, city):
        """
        获取城市ID
        
        Args:
            city: 城市名称
            
        Returns:
            str: 城市ID
        """
        city_map = {
            '全国': '538',
            '北京': '538',
            '上海': '765',
            '广州': '763',
            '深圳': '765',
            '杭州': '653',
            '南京': '635',
            '成都': '801',
            '武汉': '736',
            '西安': '854',
            '苏州': '639',
            '天津': '531',
            '重庆': '719',
            '青岛': '703',
            '长沙': '749',
            '郑州': '719',
            '大连': '600',
            '宁波': '654',
            '厦门': '682',
            '无锡': '636'
        }
        return city_map.get(city, '538')
    
    def parse_job_info(self, job_data):
        """
        解析职位信息
        
        Args:
            job_data: 职位数据
            
        Returns:
            dict: 解析后的职位信息
        """
        try:
            job_info = {
                'job_id': job_data.get('number', ''),
                'job_title': job_data.get('jobName', ''),
                'company_name': job_data.get('company', {}).get('name', ''),
                'company_size': job_data.get('company', {}).get('size', {}).get('name', ''),
                'company_type': job_data.get('company', {}).get('type', {}).get('name', ''),
                'salary': job_data.get('salary', ''),
                'city': job_data.get('city', {}).get('display', ''),
                'district': job_data.get('district', {}).get('display', ''),
                'experience': job_data.get('workingExp', {}).get('name', ''),
                'education': job_data.get('eduLevel', {}).get('name', ''),
                'job_type': job_data.get('emplType', {}).get('name', ''),
                'publish_time': job_data.get('updateDate', ''),
                'job_description': job_data.get('jobDesc', ''),
                'job_requirements': job_data.get('jobRequirement', ''),
                'benefits': ', '.join([tag.get('name', '') for tag in job_data.get('welfare', [])]),
                'skills': ', '.join([tag.get('name', '') for tag in job_data.get('skillLabel', [])]),
                'crawl_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            }
            
            return job_info
            
        except Exception as e:
            logging.error(f"解析职位信息失败: {e}")
            return None
    
    def crawl_jobs(self, keywords=None, cities=None, max_pages=5):
        """
        爬取职位信息
        
        Args:
            keywords: 关键词列表，默认为None使用预设关键词
            cities: 城市列表，默认为None使用全国
            max_pages: 每个关键词每个城市最大爬取页数
        """
        if keywords is None:
            keywords = self.keywords
        
        if cities is None:
            cities = ['全国']
        
        total_jobs = 0
        
        for keyword in keywords:
            for city in cities:
                logging.info(f"开始爬取关键词: {keyword}, 城市: {city}")
                
                for page in range(1, max_pages + 1):
                    # 添加随机延迟，避免被封
                    time.sleep(random.uniform(1, 3))
                    
                    search_result = self.search_jobs(keyword, city, page)
                    if not search_result or 'data' not in search_result:
                        logging.warning(f"搜索失败或无数据，跳过关键词: {keyword}, 城市: {city}, 页码: {page}")
                        break
                    
                    jobs = search_result['data'].get('results', [])
                    if not jobs:
                        logging.info(f"关键词: {keyword}, 城市: {city}, 页码: {page} 无更多数据")
                        break
                    
                    for job in jobs:
                        # 获取职位详情
                        job_detail = self.get_job_detail(job.get('number', ''))
                        if job_detail:
                            parsed_job = self.parse_job_info(job_detail)
                            if parsed_job:
                                self.jobs_data.append(parsed_job)
                                total_jobs += 1
                        
                        # 添加随机延迟
                        time.sleep(random.uniform(0.5, 1.5))
                    
                    logging.info(f"关键词: {keyword}, 城市: {city}, 页码: {page}, 已爬取: {total_jobs} 条")
        
        logging.info(f"爬取完成，总共获取 {total_jobs} 条职位信息")
    
    def save_to_csv(self, filename=None):
        """
        保存数据到CSV文件
        
        Args:
            filename: 文件名，默认为None自动生成
        """
        if not self.jobs_data:
            logging.warning("没有数据可保存")
            return
        
        if filename is None:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = f'zhilian_data_analysis_jobs_{timestamp}.csv'
        
        df = pd.DataFrame(self.jobs_data)
        df.to_csv(filename, index=False, encoding='utf-8-sig')
        logging.info(f"数据已保存到: {filename}")
        
        # 显示数据统计
        print(f"\n数据统计:")
        print(f"总职位数: {len(df)}")
        print(f"涉及城市数: {df['city'].nunique()}")
        print(f"涉及公司数: {df['company_name'].nunique()}")
        print(f"平均薪资范围: {df['salary'].value_counts().head().to_dict()}")
    
    def save_to_json(self, filename=None):
        """
        保存数据到JSON文件
        
        Args:
            filename: 文件名，默认为None自动生成
        """
        if not self.jobs_data:
            logging.warning("没有数据可保存")
            return
        
        if filename is None:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = f'zhilian_data_analysis_jobs_{timestamp}.json'
        
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(self.jobs_data, f, ensure_ascii=False, indent=2)
        
        logging.info(f"数据已保存到: {filename}")
    
    def get_data_summary(self):
        """
        获取数据摘要统计
        
        Returns:
            dict: 数据统计信息
        """
        if not self.jobs_data:
            return {}
        
        df = pd.DataFrame(self.jobs_data)
        
        summary = {
            'total_jobs': len(df),
            'unique_cities': df['city'].nunique(),
            'unique_companies': df['company_name'].nunique(),
            'salary_distribution': df['salary'].value_counts().head(10).to_dict(),
            'city_distribution': df['city'].value_counts().head(10).to_dict(),
            'company_size_distribution': df['company_size'].value_counts().to_dict(),
            'experience_distribution': df['experience'].value_counts().to_dict(),
            'education_distribution': df['education'].value_counts().to_dict()
        }
        
        return summary


def main():
    """
    主函数
    """
    print("智联招聘数据分析岗位爬虫启动...")
    
    # 创建爬虫实例
    spider = ZhilianSpider()
    
    # 自定义爬取参数
    keywords = ['数据分析师', '数据挖掘', '数据工程师', '商业分析师']
    cities = ['北京', '上海', '深圳', '广州', '杭州']
    
    try:
        # 开始爬取
        spider.crawl_jobs(keywords=keywords, cities=cities, max_pages=3)
        
        # 保存数据
        spider.save_to_csv()
        spider.save_to_json()
        
        # 显示数据摘要
        summary = spider.get_data_summary()
        print("\n=== 数据摘要 ===")
        for key, value in summary.items():
            print(f"{key}: {value}")
        
    except KeyboardInterrupt:
        print("\n用户中断爬取")
        if spider.jobs_data:
            spider.save_to_csv()
            spider.save_to_json()
    except Exception as e:
        logging.error(f"爬取过程中发生错误: {e}")
        if spider.jobs_data:
            spider.save_to_csv()
            spider.save_to_json()


if __name__ == "__main__":
    main()
