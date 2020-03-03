#!/usr/bin/env python
# _*_ coding:utf-8 _*_

import requests
from bs4 import BeautifulSoup
import re


def get_html(url, headers):
    r = requests.get(url, headers=headers)
    r.encoding = 'utf-8'
    return r.text


def get_detail(url):
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) '
                             'Chrome/63.0.3239.132 Safari/537.36'}
    html = get_html(url, headers)
    # print(html)
    soup = BeautifulSoup(html, 'lxml', from_encoding='utf-8')
    # 获得详情
    contents = soup.find_all(class_="txt")[:2]
    content = str(contents[0].get_text()).strip()
    public = contents[0].get("nick-name")
    if content.find("展开全文") >= 0:
        content = str(contents[1].get_text()).strip()[:-5]
    # print(public)
    # print(content)
    return public, content


if __name__ == '__main__':
    url = 'https://s.weibo.com/weibo?q=%23%E9%85%B8%E5%A5%B6%E7%85%8E%E9%A5%BC%23&Refer=top'
    get_detail(url)
