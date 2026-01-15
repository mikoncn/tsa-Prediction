import requests
from bs4 import BeautifulSoup
import sqlite3
import datetime
import re
import time
import argparse

# 配置
BASE_URL = "https://www.tsa.gov"
START_URL = "https://www.tsa.gov/travel/passenger-volumes"
DB_NAME = "tsa_data.db"
TABLE_NAME = "traffic"

def init_db():
    """初始化数据库表"""
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    cursor.execute(f'''
        CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
            date TEXT PRIMARY KEY,
            throughput INTEGER
        )
    ''')
    conn.commit()
    conn.close()
    print(f"数据库 {DB_NAME} 初始化完成。")

def parse_date(date_str):
    """将 M/D/YYYY 格式转换为 YYYY-MM-DD"""
    try:
        dt = datetime.datetime.strptime(date_str, "%m/%d/%Y")
        return dt.strftime("%Y-%m-%d")
    except ValueError:
        return None

def parse_number(num_str):
    """移除逗号并转换为整数"""
    try:
        return int(num_str.replace(",", "").strip())
    except ValueError:
        return None

def get_year_links(soup):
    """从主页解析所有年份的链接"""
    links = set()
    # 当前页面本身也是一个数据源（通常是最新年份）
    links.add(START_URL)
    
    # 查找侧边栏或内容区域的年份链接
    # 这里的特征通常是链接文本是年份（4位数字）
    for a in soup.find_all('a', href=True):
        text = a.get_text(strip=True)
        href = a['href']
        
        # 匹配 2019 - 2030 之间的年份
        if re.match(r'^20[1-3][0-9]$', text):
            full_url = href if href.startswith("http") else BASE_URL + href
            links.add(full_url)
            
    return sorted(list(links), reverse=True)

def scrape_page(url):
    """爬取单个页面的数据"""
    print(f"正在爬取: {url} ...")
    try:
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
        }
        resp = requests.get(url, headers=headers, timeout=10)
        resp.raise_for_status()
        
        soup = BeautifulSoup(resp.content, 'html.parser')
        
        # 查找数据表格
        table = soup.find('table')
        if not table:
            print(f"警告: 在 {url} 未找到表格")
            return []
            
        data = []
        rows = table.find_all('tr')
        
        # 假设第一行是表头，从第二行开始并在有td时处理
        for row in rows:
            cols = row.find_all('td')
            if len(cols) >= 2:
                date_text = cols[0].get_text(strip=True)
                num_text = cols[1].get_text(strip=True)
                
                clean_date = parse_date(date_text)
                clean_num = parse_number(num_text)
                
                if clean_date and clean_num is not None:
                    data.append((clean_date, clean_num))
        
        print(f"  - 找到 {len(data)} 条记录")
        return data
        
    except Exception as e:
        print(f"错误: 爬取 {url} 失败 - {e}")
        return []

def save_to_db(all_data):
    """批量保存数据"""
    if not all_data:
        print("没有数据需要保存。")
        return

    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    
    # 使用 INSERT OR REPLACE 避免重复，且更新现有数据
    cursor.executemany(f'''
        INSERT OR REPLACE INTO {TABLE_NAME} (date, throughput)
        VALUES (?, ?)
    ''', all_data)
    
    conn.commit()
    count = cursor.rowcount  # 注意：REPLACE可能会导致受影响行数看起来比插入的多
    conn.close()
    print(f"成功存入/更新了 {len(all_data)} 条记录。")

def main():
    # 解析命令行参数
    parser = argparse.ArgumentParser(description='TSA 旅客吞吐量数据抓取工具')
    parser.add_argument('--latest', action='store_true', 
                        help='仅抓取首页最新数据(增量更新模式)')
    args = parser.parse_args()
    
    init_db()
    
    all_data = []
    
    if args.latest:
        # 增量更新模式: 仅抓取首页
        print("[增量模式] 仅抓取 TSA 首页最新数据...")
        page_data = scrape_page(START_URL)
        all_data.extend(page_data)
    else:
        # 全量模式: 抓取所有年份
        print("[全量模式] 正在获取主页以分析年份链接...")
        try:
            headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
            }
            resp = requests.get(START_URL, headers=headers)
            soup = BeautifulSoup(resp.content, 'html.parser')
            year_links = get_year_links(soup)
            print(f"发现以下年份页面: {year_links}")
            
        except Exception as e:
            print(f"获取主页失败，将尝试默认年份列表: {e}")
            # 后备方案：如果没有找到链接，尝试构建最近几年的 URL
            current_year = datetime.datetime.now().year
            year_links = [START_URL] + [f"{START_URL}/{y}" for y in range(current_year-1, 2018, -1)]
            print(f"使用后备链接列表: {year_links}")

        # 遍历所有页面抓取
        for link in year_links:
            page_data = scrape_page(link)
            all_data.extend(page_data)
            time.sleep(1) # 礼貌爬取，避免请求过快
        
    # 存入数据库
    save_to_db(all_data)
    print("全部完成。")

if __name__ == "__main__":
    main()
