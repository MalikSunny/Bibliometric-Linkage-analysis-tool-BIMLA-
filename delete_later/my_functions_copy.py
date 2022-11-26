import os
import pandas as pd
import sqlite3
import collections
from fuzzywuzzy import fuzz

def touch(fname):
    open(fname, 'a').close()
    os.utime(fname, None)

def keywords_database(records):
     row_data=[]
     for i in range(len(records)):
       try:
         for j,k in enumerate(records[i][1].split(';')):
             row_data.append([k.strip().lower(),i,j])
       except:
           pass
     try:
        df=pd.DataFrame(row_data,columns=None)
        df.columns= ['a_keyword', 'record_id', 'keyword_ind']
        touch('keywords.db')
        conn = sqlite3.connect('keywords.db')
        c = conn.cursor()
        c.execute('''CREATE TABLE keyword(id INTEGER PRIMARY KEY AUTOINCREMENT,
                   'a_keyword' text, 'record_id' int, 'keyword_ind' int)''')
        df.to_sql('keyword', conn, if_exists='append', index = False)
        a_keywords=c.execute('''SELECT a_keyword FROM keyword''').fetchall()
        c.close()
        conn.close()
        test=[i[0] for i in a_keywords]
        data=collections.Counter(test)
        data={k: v for k, v in sorted(data_.items(), key=lambda item: item[1], reverse=True)}
        key_= list(data.keys())
        value_ = list(data.values())
        sim_keywords=[]
        for i in range(len(key_)):
            local_sim=[(key_[i],value_[i])]
            for j in range(i+1,len(key_)):
                if fuzz.ratio(key_[i],key_[j])>95:
                      local_sim.append((key_[j],value_[j]))
            if len(local_sim)>1:
                 sim_keywords.append(local_sim)
        return sim_keywords
     except:
        conn = sqlite3.connect('keywords.db')
        c = conn.cursor()
        a_keywords=c.execute('''SELECT id,a_keyword FROM keyword''').fetchall()
        c.close()
        conn.close()
        test=[i[0] for i in a_keywords]
        data_=collections.Counter(test)
        data={k: v for k, v in sorted(data_.items(), key=lambda item: item[1], reverse=True)}
        key_= list(data.keys())
        value_ = list(data.values())
        sim_keywords=[]
        for i in range(len(key_)):
            local_sim=[(key_[i],value_[i])]
            for j in range(i+1,len(key_)):
                if fuzz.ratio(key_[i],key_[j])>95:
                      local_sim.append((key_[j],value_[j]))
            if len(local_sim)>1:
                 sim_keywords.append(local_sim)
        return sim_keywords        

def sim_key_database(data):
    row_data=[]
    for i,j in enumerate(data):
        for k in j:
            row_data.append([i,k[0],k[1],0])
    try:
       df=pd.DataFrame(row_data,columns=None)
       df.columns= ['sim_record_id', 'a_keyword', 'key_freq','status']
       touch('sim_keywords.db')
       conn = sqlite3.connect('sim_keywords.db')
       c = conn.cursor()
       c.execute('''CREATE TABLE sim_keyword(id INTEGER PRIMARY KEY AUTOINCREMENT,
                   'sim_record_id' int, 'a_keyword' text, 'key_freq' int, 'status' int)''')
       df.to_sql('sim_keyword', conn, if_exists='append', index = False)
       data=c.execute('''SELECT * FROM sim_keyword''').fetchall()
       c.close()
       conn.close()
       df=pd.DataFrame(data,columns=None)
       df.columns= ['id','sim_record_id', 'a_keyword', 'key_freq','status']
       g=[]
       for i in df.groupby(by=['sim_record_id']):
           local=[]
           for k,l,m,n,o in zip(i[1]['id'].values,i[1]['sim_record_id'].values,i[1]['a_keyword'].values,i[1]['key_freq'].values,i[1]['status'].values):
                 local.append([k,l,m,n,o])
           g.append(local)
       return g
    except:
       conn = sqlite3.connect('sim_keywords.db')
       c = conn.cursor()
       data=c.execute('''SELECT * FROM sim_keyword''').fetchall()
       c.close()
       conn.close()
       df=pd.DataFrame(data,columns=None)
       df.columns= ['id','sim_record_id', 'a_keyword', 'key_freq','status']
       g=[]
       for i in df.groupby(by=['sim_record_id']):
           local=[]
           for k,l,m,n,o in zip(i[1]['id'].values,i[1]['sim_record_id'].values,i[1]['a_keyword'].values,i[1]['key_freq'].values,i[1]['status'].values):
               local.append([k,l,m,n,o])
           g.append(local)
       return g
