#!/usr/bin/env python
# _*_ coding:utf-8 _*_

import requests
from bs4 import BeautifulSoup
import time
import logging
import os
import pymysql
import redis
from get_hupu_detail import get_detail
import re
import traceback
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


logger = set_log("hupu_log")
mysql_host = ""
redis_host = ""
es_host = ""
get_net = 'curl --cookie "SessionId=fc58c3b57a0b2201" -d "user=&pass=" http:///login'
detail_href = "https://www.baidu.com/s?rtt=1&bsst=1&cl=2&tn=news&word="


def check_net():
    exit_code = os.system("curl www.baidu.com")
    if exit_code != 0:
        val = os.system(get_net)
        time.sleep(3)


def get_html(url, headers):
    r = requests.get(url, headers=headers, timeout=30)
    r.encoding = r.apparent_encoding
    return r.text


def write_to_db(html):
    soup = BeautifulSoup(html, 'html.parser')
    # 需要爬取的内容所属class
    all_topic = soup.find_all(class_="textSpan")
    # 一共是20条，后面的热榜内容舍去
    main_topic = all_topic[:20]
    date = []
    es_datas = []
    index = 1
    for topic in main_topic:
        rank = index
        # print(rank)
        # 得到标题
        each_title = topic.a.get("title")
        # print(each_title)
        # 得到链接，链接不完整时需要补充
        href = "https://bbs.hupu.com" + topic.a.get("href")
        # print(href)
        # 得到需要计算热度的文本
        hot_number_str = topic.em.get_text().strip()
        index_l = hot_number_str.find("亮")
        index_h = hot_number_str.find("回复")
        # 计算得到热度
        ln = 0
        hn = 0
        if index_l >= 0:
            ln = int(hot_number_str[:index_l])
        if index_h >= 0:
            if index_l >= 0:
                hn = int(hot_number_str[index_l + 1:index_h])
            else:
                hn = int(hot_number_str[:index_h])
        hot_number = ln+hn
        # print(hot_number)
        public, detail = get_detail(href)
        # print(public)
        # print(detail)
        # 外键
        t = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
        u_id = int(str(rank) + t[::3][1:])
        # print(u_id)
        date.append((int(rank), each_title, href, public, int(hot_number), detail, t, int(u_id)))
        es_datas.append([each_title, each_title, int(rank), int(hot_number), t.replace(" ", 'T'), int(u_id)])
        index += 1
        time.sleep(1)
    try:
        db = pymysql.connect(mysql_host, "root", "0", "social_network_data")
        cursor = db.cursor()
        try:
            cursor.executemany('insert into hupu_hot_list(rank, title, link, public_name, hot_number, detail, '
                               'timestamp, u_id) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)', date)
            db.commit()
            logger.info("虎扑写入数据库完成")
            r = redis.StrictRedis(host=redis_host, port='6379', password='0')
            try:
                if r.exists('HotSpotData::hupu'):
                    r.delete('HotSpotData::hupu')
                logger.info("虎扑清除redis完成")
            except Exception as e:
                logger.error("虎扑清除redis错误" + str(e))
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
                        "_index": "hupu_data",
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
                logger.info("虎扑数据写入es成功")
            except Exception as e:
                logger.error("虎扑数据写入es错误：" + str(e))
        except Exception as e:
            logger.error("写入数据库错误:" + str(e))
            db.rollback()
            with open("../data/hupu_hot_list.txt", "a", encoding="utf-8") as f:
                for data in date:
                    data = [str(x) for x in data]
                    f.writelines("\t".join(data) + "\n")
            logger.info("虎扑热榜写入文件完成")
        finally:
            db.close()
    except Exception as e:
        logger.error("连接数据失败:" + str(e))
        with open("../data/hupu_hot_list.txt", "a", encoding="utf-8") as f:
            for data in date:
                data = [str(x) for x in data]
                f.writelines("\t".join(data) + "\n")
        logger.info("虎扑热榜写入文件完成")


def write_hot_list_to_db():
    url = 'https://bbs.hupu.com/all-gambia'
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) '
                             'Chrome/63.0.3239.132 Safari/537.36'}
    html = get_html(url, headers)
    write_to_db(html)


if __name__ == '__main__':
    while True:
        try:
            # check_net()
            write_hot_list_to_db()
            time.sleep(30 * 60)
        except Exception as e:
            logger.error(traceback.format_exc())
            time.sleep(10 * 60)
