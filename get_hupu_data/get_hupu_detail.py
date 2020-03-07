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
    content = soup.find(class_='quote-content').get_text().strip()
    # print(content)
    public = soup.find(class_='u').get_text().strip()
    # print(public)
    return public, content


if __name__ == '__main__':
    url = 'https://bbs.hupu.com/32782056.html'
    get_detail(url)
