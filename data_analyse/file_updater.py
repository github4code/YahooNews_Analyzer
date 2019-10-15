# -*- coding: UTF-8 -*-

'''
This program is used to update dictionary-file, tfidf-model, lda-model

Program execution: python file_updater.py

Author: Jason Su
Last modification: 2016/12/17
'''

import sys, datetime, jieba, jieba.analyse, MySQLdb
from gensim import corpora, models, similarities
from mysql_config import DBConfig

reload(sys)
sys.setdefaultencoding('utf8')

date_list = [2016,9,22]
date = date_list[0]*10000 + date_list[1]*100 + date_list[2]
today_date = (datetime.datetime.now().year)*10000 + (datetime.datetime.now().month)*100 + datetime.datetime.now().day

def get_next_date(date_in):
    date = datetime.datetime(date_in[0], date_in[1], date_in[2])
    date = date + datetime.timedelta(days=1)
    return [int(str(date)[0:4]), int(str(date)[5:7]), int(str(date)[8:10])]

stop_words = set(open("ref/stop_word.txt", "r").read().splitlines())
stop_words.update('\n', '\t', ' ')

jieba.set_dictionary('ref/dict.txt.big')
jieba.load_userdict("ref/userdict.txt")

if 1>0:
    
    try: 
        db = DBConfig()
        db.dbConnect()
        query = "SELECT COUNT(*) from News WHERE date>=%s" % date
        db.executeQuery(query)
        news_num =  int(db.results[0][0])
        
        query = "SELECT number, title, content from News WHERE date>=%s" % date
        db.executeQuery(query)
        
        texts = []
        today_number = []
        
        for result in db.results:

            seglist = jieba.cut(result[2], cut_all=False) 
            line = []

            for word in seglist:
                if word.encode('utf8') not in stop_words and word.isdigit() == False:
                    line.append(word)
                        
            texts.append(line)
            today_number.append(result[0])
            
        '''query = "SELECT date from News WHERE number=%s" % db.results[news_num-1][0]
        db.executeQuery(query)
        query = "UPDATE Information SET last_dict_date=%s WHERE 1" % db.results[0][0]
        db.executeQuery(query)
        db.dbCommit()'''
                
    except MySQLdb.Error as e:
        print "Error %d: %s" % (e.args[0], e.args[1])
        
    dictionary = corpora.Dictionary(texts)
    #dictionary = corpora.Dictionary.load_from_text('news.dict')
    #dictionary.add_documents(texts)
    dictionary.filter_extremes(no_below=10, no_above=0.5, keep_n=50000)
    dictionary.save_as_text('news.dict')
    
    corpus = [dictionary.doc2bow(text) for text in texts]
    corpora.MmCorpus.serialize('news.mm', corpus)
    #tfidf.load('foo.tfidf_model')
    
    tfidf = models.TfidfModel(corpus)
    tfidf.save('foo.tfidf_model')
    print tfidf
    
    #lda = models.ldamodel.LdaModel(tfidf[corpus], id2word=dictionary, num_topics = TOPIC_NUM) 
    #lda.save('foo.lda_model')
    #lda = models.ldamodel.LdaModel.load('foo.lda_model')