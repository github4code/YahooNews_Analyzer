# -*- coding: UTF-8 -*-

import sys, datetime, requests, json, jieba, jieba.analyse, MySQLdb
import numpy as np
from gensim import corpora, models, similarities
from mysql_config import DBConfig
from bs4  import BeautifulSoup

reload(sys)
sys.setdefaultencoding('utf8')

date_list = [2016,9,22]
date = date_list[0]*10000 + date_list[1]*100 + date_list[2]
today_date = (datetime.datetime.now().year)*10000 + (datetime.datetime.now().month)*100 + datetime.datetime.now().day

def get_next_date(date_in):
    date = datetime.datetime(date_in[0], date_in[1], date_in[2])
    date = date + datetime.timedelta(days=1)
    return [int(str(date)[0:4]), int(str(date)[5:7]), int(str(date)[8:10])]

except_list = ["台灣集中市場", "台灣店頭市場", "台股集中市場", "台股店頭市場", "超個股", "持股轉讓日報表",  "店頭市場", "◆"]

while date<=today_date:
    
    try:
        query = "SELECT COUNT(*) from News WHERE date=%s" %date
        db = DBConfig()
        db.dbConnect()
        db.executeQuery(query)
        print db.results[0][0]

        '''for result in db.results:
            for word in except_list:
                if word.encode('utf8') in result[1]:
                    query = """DELETE FROM News WHERE number=%s""" % result[0]
                    db.executeQuery(query)
                    db.dbCommit()
                    print result[1]'''

    except MySQLdb.Error as e:
            print "Error %d: %s" % (e.args[0], e.args[1])
            
    date_list = get_next_date(date_list)
    date = date_list[0]*10000 + date_list[1]*100 + date_list[2]


