import requests
import json
from urllib.parse import quote
from bs4 import BeautifulSoup
from typing import Optional, List, Dict, Tuple
import concurrent.futures
from functools import lru_cache
import argparse
import csv
import os
import datetime
import sys

# 全局会话，复用TCP连接
session = requests.Session()


def get_ent_info(search_key: str, cookie: str) -> Optional[Tuple[str, str]]:
    """获取企业基本信息（entName和entid）"""
    post_url = "https://www.riskbird.com/riskbird-api/newSearch"
    headers = {
        "Host": "www.riskbird.com",
        "Cookie": cookie,
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:138.0) Gecko/20100101 Firefox/138.0",
        "Accept": "application/json",
        "Accept-Language": "zh-CN,zh;q=0.8,zh-TW;q=0.7,zh-HK;q=0.5,en-US;q=0.3,en;q=0.2",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "close"
    }
    payload = {
        "queryType": "1",
        "searchKey": search_key,
        "pageNo": 1,
        "range": 10,
        "selectConditionData": '{"status":"","sort_field":""}'
    }

    try:
        response = session.post(post_url, headers=headers, json=payload, timeout=10)
        response.raise_for_status()
        data = response.json()
    except requests.exceptions.RequestException as e:
        print(f"企业信息请求失败: {e}")
        return None

    try:
        ent_list = data["data"]["list"]
        if not ent_list:
            print("未找到企业数据")
            return None
        first_ent = ent_list[0]
        return first_ent["entName"], first_ent["entid"]
    except (KeyError, IndexError) as e:
        print(f"解析企业信息失败: {e}")
        return None


def query_equity_investment(ent_id: str, cookie: str, threshold: int) -> List[Dict]:
    """查询符合条件的股权对外投资"""
    equity_url = "https://www.riskbird.com/riskbird-api/graphics/query"
    headers = {
        "Host": "www.riskbird.com",
        "Cookie": cookie,
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:138.0) Gecko/20100101 Firefox/138.0",
        "Accept": "application/json",
        "Accept-Language": "zh-CN,zh;q=0.8,zh-TW;q=0.7,zh-HK;q=0.5,en-US;q=0.3,en;q=0.2",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "close"
    }
    payload = {
        "entid": ent_id,
        "dataType": "entInvest",
        "isExpand": 0
    }

    try:
        response = session.post(equity_url, headers=headers, json=payload, timeout=10)
        response.raise_for_status()
        data = response.json()
    except requests.exceptions.RequestException as e:
        print(f"股权查询请求失败: {e}")
        return []

    results = []
    if data.get("success") and data.get("data"):
        children = data["data"].get("children", [])
        for child in children:
            funded_ratio_str = child.get("fundedRatio", "").replace("%", "")
            if funded_ratio_str.replace(".", "").isdigit():
                funded_ratio = int(float(funded_ratio_str))
                if funded_ratio >= threshold:
                    results.append({
                        "name": child["entname"],
                        "entid": child["entid"],
                        "funded_ratio": funded_ratio
                    })
    return results


@lru_cache(maxsize=128)  # 缓存官网查询结果
def get_official_website(ent_name: str, ent_id: str, cookie: str) -> Optional[str]:
    """获取企业官网链接，使用缓存避免重复查询"""
    encoded_ent_name = quote(ent_name)
    get_url = f"https://www.riskbird.com/ent/{encoded_ent_name}.html?entid={ent_id}"

    headers = {
        "Host": "www.riskbird.com",
        "Cookie": cookie,
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:138.0) Gecko/20100101 Firefox/138.0",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "zh-CN,zh;q=0.8,zh-TW;q=0.7,zh-HK;q=0.5,en-US;q=0.3,en;q=0.2",
        "Accept-Encoding": "gzip, deflate, br",
        "Upgrade-Insecure-Requests": "1",
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "none",
        "Sec-Fetch-User": "?1",
        "Priority": "u=0, i",
        "Te": "trailers",
        "Connection": "close"
    }

    try:
        response = session.get(get_url, headers=headers, timeout=10)
        response.raise_for_status()
        html_text = response.text
    except requests.exceptions.RequestException as e:
        print(f"官网页面请求失败: {e}")
        return None

    try:
        # 优化：直接使用字符串查找替代完整解析
        start_index = html_text.find("官网： <div ")
        if start_index == -1:
            return None

        # 限制搜索范围，减少BeautifulSoup处理的内容
        relevant_html = html_text[start_index:start_index + 1000]
        soup = BeautifulSoup(relevant_html, "lxml")  # 使用lxml解析器（需要安装）
        a_tag = soup.find("a", href=True)
        if not a_tag:
            return None

        href = a_tag["href"]
        return f"http:{href}" if href.startswith("//") else href
    except Exception as e:
        print(f"解析官网链接失败: {e}")
        return None


def fetch_official_website(search_key: str, cookie: str, equity_threshold: int = None, max_workers: int = 5) -> Dict:
    """主函数：获取企业官网并选择性查询股权信息及子公司官网，支持并发处理"""
    # 获取企业基本信息
    ent_info = get_ent_info(search_key, cookie)
    if not ent_info:
        return {"parent_name": None, "official_website": None, "equity_investments": []}

    parent_name, parent_id = ent_info

    # 获取母公司官网链接
    official_website = get_official_website(parent_name, parent_id, cookie)

    # 查询股权信息
    equity_investments = []
    if equity_threshold is not None and 0 <= equity_threshold <= 100:
        child_companies = query_equity_investment(parent_id, cookie, equity_threshold)

        if child_companies:
            print(f"发现{len(child_companies)}家符合条件的子公司，正在并发获取官网信息...")

            # 并发获取子公司官网链接
            with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
                future_to_child = {
                    executor.submit(get_official_website, child["name"], child["entid"], cookie): child
                    for child in child_companies
                }

                for future in concurrent.futures.as_completed(future_to_child):
                    child = future_to_child[future]
                    try:
                        website = future.result()
                        child["website"] = website
                        equity_investments.append(child)
                    except Exception as e:
                        print(f"获取{child['name']}官网时出错: {e}")
                        child["website"] = None
                        equity_investments.append(child)

            # 按股权比例排序
            equity_investments.sort(key=lambda x: x["funded_ratio"], reverse=True)

            # 输出详细信息
            if equity_investments:
                print(f"\n{parent_name} 股权≥{equity_threshold}%的子公司及官网信息：")
                for idx, child in enumerate(equity_investments, 1):
                    print(f"{idx}. 子公司: {child['name']}")
                    print(f"   股权比例: {child['funded_ratio']}%")
                    print(f"   子公司官网: {child['website'] or '未找到官网'}")
                    print("-" * 50)

    return {
        "parent_name": parent_name,
        "official_website": official_website,
        "equity_investments": equity_investments
    }


def save_to_csv(results, output_path):
    with open(output_path, 'w', newline='', encoding='utf-8-sig') as csvfile:
        fieldnames = ['单位名称', '官网地址', '股权']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

        writer.writeheader()
        for result in results:
            # 写入母公司信息
            writer.writerow({
                '单位名称': result["parent_name"],
                '官网地址': result["official_website"] or "",
                '股权': ""
            })
            # 写入子公司信息
            for child in result["equity_investments"]:
                writer.writerow({
                    '单位名称': child["name"],
                    '官网地址': child["website"] or "",
                    '股权': child["funded_ratio"]
                })


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='查询企业官网及子公司股权信息')
    parser.add_argument('-s', '--search', help='搜索单位名称')
    parser.add_argument('-f', '--file', help='包含单位名称的文件路径，每行一个单位名称')
    parser.add_argument('-q', '--equity', type=int, default=0, help='股权阈值，范围0-100')
    parser.add_argument('-t', '--threads', type=int, default=5, help='并发线程数')
    parser.add_argument('-o', '--output', help='输出文件路径')

    args = parser.parse_args()

    if not args.search and not args.file:
        print("请提供搜索单位名称或包含单位名称的文件路径。")
        sys.exit(1)

    try:
        with open('config.txt', 'r') as f:
            user_cookie = f.read().strip()
    except FileNotFoundError:
        print("未找到config.txt文件，请确保该文件存在。")
        sys.exit(1)

    if args.file:
        try:
            with open(args.file, 'r', encoding='utf-8') as file:
                search_keys = [line.strip() for line in file.readlines() if line.strip()]
        except FileNotFoundError:
            print(f"未找到文件: {args.file}")
            sys.exit(1)
    else:
        search_keys = [args.search]

    all_results = []
    for search_key in search_keys:
        result = fetch_official_website(
            search_key=search_key,
            cookie=user_cookie,
            equity_threshold=args.equity,
            max_workers=args.threads
        )
        all_results.append(result)

    if args.output:
        output_path = args.output
    else:
        if not os.path.exists('res'):
            os.makedirs('res')
        timestamp = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
        output_path = os.path.join('res', f'{timestamp}.csv')

    save_to_csv(all_results, output_path)
    print(f"结果已保存到 {output_path}")