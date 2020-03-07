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
    str_new = "detail"
    soup = BeautifulSoup(html, 'lxml', from_encoding='utf-8')
    try:
        content = soup.find(class_='post-topic-des nc-post-content').get_text()
        str_new = re.sub(r"\s+", "\n", content)
    except Exception as e:
        pass
    return str_new.strip()


if __name__ == '__main__':
    url = 'https://www.nowcoder.com/jump?type=ad&source=28&entityId=2799&companyId=1727&url=https%3A%2F%2Fwww.nowcoder.com%2Fdiscuss%2F341197'
    get_detail(url)
