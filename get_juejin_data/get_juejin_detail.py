#!/usr/bin/env python
# _*_ coding:utf-8 _*_

import requests
from bs4 import BeautifulSoup


def get_html(url, headers):
    r = requests.get(url, headers=headers)
    r.encoding = 'utf-8'
    return r.text


def get_detail(url):
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) '
                             'Chrome/63.0.3239.132 Safari/537.36'}
    html = get_html(url, headers)
    soup = BeautifulSoup(html, 'lxml', from_encoding='utf-8')
    hot_number = 0
    content = "detail"
    try:
        hot_number = int(soup.find(class_='views-count').get_text().split()[-1])
        # print(hot_number)
        content = soup.find(class_='article-content').get_text()
        # print(content)
    except Exception as e:
        pass
    return hot_number, content


if __name__ == '__main__':
    url = 'https://juejin.im/entry/5e60bb7c6fb9a07cc200d615'
    get_detail(url)
