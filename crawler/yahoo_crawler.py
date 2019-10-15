# -*- coding: UTF-8 -*-

'''
This program is used to crawl the yahoo news from webpage.
Then store them into MySQL

Program execution: python yahoo_crawler.py

Author: Jason Su
Last modification: 2016/12/17
'''

import sys, requests, MySQLdb, datetime
from mysql_config import DBConfig
from bs4  import BeautifulSoup

reload(sys)
sys.setdefaultencoding('utf8')

# In "2016年12月3日 下午4:31" format, then transfer to 201612031631 format
def get_time(news_date): 
    
    this_time = int(news_date.split('年')[0]) * 100000000
    tmp = news_date.split('年')[1]

    this_time += int(tmp.split('月')[0]) * 1000000
    tmp = tmp.split('月')[1]

    this_time += int(tmp.split('日')[0]) * 10000
    tmp = tmp.split('日')[1][1:]

    if tmp[0:2] == '上午':
        last_hour = int(tmp[2:].split(':')[0])
        if last_hour == 12:
            last_hour = 0

    elif tmp[0:2] == '下午':
        last_hour = int(tmp[2:].split(':')[0])
        if last_hour != 12:
            last_hour += 12

    this_time += last_hour * 100 + int(tmp[2:].split(':')[1])    
    this_date = str(this_time)[0:8]
    
    return this_time
    
    
def main():

    PAGE_NUM = 40
    PAGE_NEWS_NUM = 25
    url_origin = "https://tw.news.yahoo.com/archive/"
    start_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    crawl_counter = 0

    # Take out the date of last news in MySQL database
    try: 
        db = DBConfig()
        db.dbConnect()
        db.executeQuery("SELECT * from Information")
        
        for result in db.results:
            last_index = int(result[0])
            last_crawl_time = int(result[1])
        
    except MySQLdb.Error as e:
        print "Error %d: %s" % (e.args[0], e.args[1])

    sql_index = last_index
        
    for page in xrange(PAGE_NUM):
    
        # From last page to first page 
        news_page = PAGE_NUM - page
        url_target = url_origin + str(news_page) + ".html"

        # get webpage content and put into bs4
        try:
            res = requests.get(url_target)
            if res.status_code != 404:  # handle with error 404 situation
                soup = BeautifulSoup(res.text, 'lxml') 
            else:
                continue
        except:
            continue
  
        for item in soup.select("#MediaStoryList .txt"):

            news_info = item.select("cite")[0].text
            news_source = news_info.split(' - ')[0]
            news_date = news_info.split(' - ')[1]
            this_time = get_time(news_date)
            this_date = this_time/10000
                                                    # advertisement, skip
            if this_time <= last_crawl_time or news_source == "Yahoo 奇摩新聞訊息快遞":
                continue
                
            try:    # get news link and connect
                news_title = item.select("h4 a")[0].text
                news_url = item.select("h4")[0].find('a')['href']
                if 'http' not in news_url:
                    news_url = "https://tw.news.yahoo.com" + news_url  # deal with url format
                else:
                    continue
            except:
                continue

            try:
                res_content = requests.get(news_url)
                if res_content.status_code != 404:
                    soup_content = BeautifulSoup(res_content.text, 'lxml')
                else:
                    continue
            except:
                continue

            try:
                group_tag = soup_content.select(".selected span")
                group_len = len(str(group_tag[0])) - 7
                news_group = str(group_tag[0])[6:group_len]

                # parse news paragraph
                content = str()
                paragraph_num = len(soup_content.select("#mediaarticlebody p"))
                for p in range(paragraph_num):
                    content = content + soup_content.select("#mediaarticlebody p")[p].text
            except:
                continue

            # Store in MySQL
            try:
                sql_index = sql_index + 1

                # Update last info everytime (avoid program halt lossing)
                db.executeQuery("""UPDATE Information SET last_crawl_time=%s, last_crawl_index=%s WHERE 1""" % (str(this_time), str(sql_index)))
                db.dbCommit()        

                if len(content) != 0:
                    query = """INSERT INTO News (number, title, source, time, date, content, category, url, similar) VALUES("%s", "%s", "%s", "%s", "%s", "%s", "%s", "%s", '')""" % ((str(sql_index), news_title, news_source, this_time, this_date, content, news_group, news_url))
                    db.executeQuery(query)
                    db.dbCommit()
                    crawl_counter += 1

            except:
                pass
    try:
        db.executeQuery("""UPDATE Information SET last_crawl_time=%s, last_crawl_index=%s WHERE 1""" % (str(this_time), str(sql_index)))
    except MySQLdb.Error as e:
        print "Error %d: %s" % (e.args[0], e.args[1])
    
    # Check same news and delete it
    try:
        def get_pre_date(date_in):
            date = datetime.datetime(date_in[0], date_in[1], date_in[2])
            date = date - datetime.timedelta(days=1)
            return [int(str(date)[0:4]), int(str(date)[5:7]), int(str(date)[8:10])]
        
        # Back and check duplicated crawling
        last_ctime = [int(str(last_crawl_time)[0:4]), int(str(last_crawl_time)[4:6]), int(str(last_crawl_time)[6:8])]
        date_check = get_pre_date(last_ctime)
        check_crawl_time = date_check[0]*10000 + date_check[1]*100 + date_check[2]
        
        db.executeQuery("SELECT * FROM News WHERE time>=%s" % check_crawl_time)

        delete_counter = 0
        same_list = list()
        results = db.results

        for info in results:

            target_number = info[0]
            target_url    = info[7]
            if len(target_url) == 0:
                continue

            for info in results:
                if len(info[7]) == 0:
                    continue
                if target_url == info[7]: # Same news url
                    if info[0] == target_number: # itself, skip
                        same_list.append(info[0])
                    else:
                        if info[0] not in same_list:
                            db.executeQuery("DELETE FROM News WHERE number=%s" % info[0])
                            db.dbCommit()
                            delete_counter +=  1
        db.dbClose()
    except MySQLdb.Error as e:
        print "Error %d: %s" % (e.args[0], e.args[1])

    print "Start crawling : %s" % start_time
    print "End   crawling : %s" % datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print "%d news has been stored into database." % crawl_counter
    print "%d news has been deleted due to duplicated crawling." % (delete_counter/2)
    
if __name__ == "__main__":
    main()    