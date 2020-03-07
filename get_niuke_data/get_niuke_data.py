#!/usr/bin/env python
# _*_ coding:utf-8 _*_

import requests
from bs4 import BeautifulSoup
import time
import logging
import os
import re
import pymysql
import redis
from get_niuke_detail import get_detail
from elasticsearch import Elasticsearch
from elasticsearch import helpers


def set_log(filename='logfile'):
    # create logger
    logger_name = "filename"
    logger = logging.getLogger(logger_name)
    logger.setLevel(logging.DEBUG)
    # create file handler
    log_path = './' + filename + '.log'
    fh = logging.FileHandler(log_path)
    ch = logging.StreamHandler()
    # create formatter
    fmt = "%(asctime)-15s %(levelname)s %(filename)s %(lineno)d %(process)d %(message)s"
    date_fmt = "%a %d %b %Y %H:%M:%S"
    formatter = logging.Formatter(fmt, date_fmt)
    # add handler and formatter to logger
    fh.setFormatter(formatter)
    ch.setFormatter(formatter)
    logger.addHandler(fh)
    logger.addHandler(ch)
    return logger


logger = set_log("niuke_log")
mysql_host = ""
redis_host = ""
es_host = ""
get_net = 'curl --cookie "SessionId=fc58c3b57a0b2201" -d "user=&pass=" http:///login'
href_list = []


def check_net():
    exit_code = os.system("curl www.baidu.com")
    if exit_code != 0:
        val = os.system(get_net)
        time.sleep(3)


def get_html(url, headers):
    r = requests.get(url, headers=headers)
    r.encoding = 'utf-8'
    return r.text


def write_to_db(html):
    soup = BeautifulSoup(html, 'lxml', from_encoding='utf-8')
    date = []
    es_datas = []
    title_all = soup.find_all(class_='discuss-main clearfix')
    hot_number_all = soup.find_all(class_='feed-legend-num')
    public_all = soup.find_all(class_='feed-tip')
    for i in range(30):
        rank = i + 1
        # print(rank)
        each_title = title_all[i].a.get_text().strip().split('\n')[0]
        # print(each_title)
        href = "https://www.nowcoder.com" + title_all[i].a.get("href")
        # print(href)
        public = re.findall('\[.*\]', public_all[i].get_text())[0][1:-1]
        # print(public)
        hot_number = int(hot_number_all[i*3].get_text()) * 3 + int(hot_number_all[i*3+1].get_text()) * 2 + int(hot_number_all[i*3+2].get_text())
        # print(hot_number)
        detail = get_detail(href)
        # print(detail)
        # 外键
        t = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
        u_id = int(str(rank) + t[::3][1:])
        # print(u_id)
        date.append((int(rank), each_title, href, public, int(hot_number), detail, t, int(u_id)))
        es_datas.append([each_title, each_title, int(rank), int(hot_number), t.replace(" ", 'T'), int(u_id)])
        time.sleep(1)
    try:
        db = pymysql.connect(mysql_host, "root", "0", "social_network_data")
        cursor = db.cursor()
        try:
            cursor.executemany('insert into niuke_hot_list(rank, title, link, public_name, hot_number, detail, '
                               'timestamp, u_id) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)', date)
            db.commit()
            logger.info("牛客写入数据库完成")
            r = redis.StrictRedis(host=redis_host, port='6379', password='0')
            try:
                if r.exists('HotSpotData::niuke'):
                    r.delete('HotSpotData::niuke')
                logger.info("牛客清除redis完成")
            except Exception as e:
                logger.error("牛客清除redis错误" + str(e))
            finally:
                r.close()
            try:
                # 连接ES
                es = Elasticsearch(
                    [es_host],
                    port=9200
                )
                actions = []
                for d in es_datas:
                    # 拼接插入数据结构
                    action = {
                        "_index": "niuke_data",
                        "_source": {
                            "title_text": d[0],
                            "title_keyword": d[1],
                            "rank": d[2],
                            "hot_number": d[3],
                            "timestamp": d[4],
                            "u_id": d[5]
                        }
                    }
                    # 形成一个长度与查询结果数量相等的列表
                    actions.append(action)
                # 批量插入
                a = helpers.bulk(es, actions)
                logger.info("牛客数据写入es成功")
            except Exception as e:
                logger.error("牛客数据写入es错误：" + str(e))
        except Exception as e:
            logger.error("写入数据库错误:" + str(e))
            db.rollback()
            with open("../data/niuke_hot_list.txt", "a", encoding="utf-8") as f:
                for data in date:
                    data = [str(x) for x in data]
                    f.writelines("\t".join(data) + "\n")
            logger.info("牛客热榜写入文件完成")
        finally:
            db.close()
    except Exception as e:
        logger.error("连接数据失败:" + str(e))
        with open("../data/niuke_hot_list.txt", "a", encoding="utf-8") as f:
            for data in date:
                data = [str(x) for x in data]
                f.writelines("\t".join(data) + "\n")
        logger.info("牛客热榜写入文件完成")


def write_hot_list_to_db():
    url = 'https://www.nowcoder.com/discuss?order=1&type=0&expTag=0'
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) '
                             'Chrome/63.0.3239.132 Safari/537.36'}
    html = get_html(url, headers)
    # print(html)
    write_to_db(html)


if __name__ == '__main__':
    while True:
        try:
            # check_net()
            write_hot_list_to_db()
            time.sleep(60 * 60 * 8)
        except Exception as e:
            logger.error(e)
            time.sleep(10 * 60)
