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
        touch('keywords_first.db')
        conn = sqlite3.connect('keywords_first.db')
        c = conn.cursor()
        c.execute('''CREATE TABLE keyword(id INTEGER PRIMARY KEY AUTOINCREMENT,
                   'a_keyword' text, 'record_id' int, 'keyword_ind' int)''')
        df.to_sql('keyword', conn, if_exists='append', index = False)
        a_keywords=c.execute('''SELECT id,a_keyword FROM keyword''').fetchall()
        c.close()
        conn.close()
        df=pd.DataFrame(a_keywords,columns=None)
        df.columns= ['table_first_id','a_keyword']
        table_2=[]
        for i in df.groupby(by=['a_keyword']):
                 table_2.append([i[0],str(list(i[1]['table_first_id'].values)),len(i[1]['table_first_id'].values)])
        df=pd.DataFrame(table_2,columns=None)
        df.columns= ['a_keyword','table_first_ids','key_freq']
        df.sort_values(by = ['key_freq'], ascending = [False],inplace=True)
        touch('keywords_second.db')
        conn = sqlite3.connect('keywords_second.db')
        c = conn.cursor()
        c.execute('''CREATE TABLE keyword(id INTEGER PRIMARY KEY AUTOINCREMENT,'a_keyword' text,'table_first_ids' text,'key_freq' int)''')
        df.to_sql('keyword', conn, if_exists='append', index = False)
        test=c.execute('''SELECT id,a_keyword, table_first_ids FROM keyword''').fetchall()
        c.close()
        conn.close()
        test_id= [i[0] for i in test]
        test_keyword=[i[1] for i in test]
        test_freq=[len(i[2][1:i[2].find(']')].split(',')) for i in test]
        sim_keywords=[]
        for i in range(len(test_id)):
            local_sim=[(test_id[i],test_keyword[i],test_freq[i])]
            for j in range(i+1,len(test_id)):
                if fuzz.ratio(test_keyword[i],test_keyword[j])>95:
                   local_sim.append((test_id[j],test_keyword[j],test_freq[j]))
            if len(local_sim)>1:
               sim_keywords.append(local_sim)
        row_data=[]
        for i,j in enumerate(sim_keywords):
              for k in j:
                  row_data.append([i,k[0],k[1],k[2],0])
        df=pd.DataFrame(row_data,columns=None)
        df.columns= ['sim_record_id', 'table_second_id','a_keyword', 'key_freq','status']
        df.sort_values(by = ['sim_record_id', 'key_freq'], ascending = [True, False],inplace=True)
        touch('sim_keywords.db')
        conn = sqlite3.connect('sim_keywords.db')
        c = conn.cursor()
        c.execute('''CREATE TABLE sim_keyword(id INTEGER PRIMARY KEY AUTOINCREMENT,'sim_record_id' int, 'a_keyword' text, 'key_freq' int, 'status' int, 'table_second_id' int)''')
        df.to_sql('sim_keyword', conn, if_exists='append', index = False)
        #data=c.execute("SELECT sim_record_id,a_keyword,key_freq,status  FROM sim_keyword").fetchall()
        data=c.execute("SELECT id,sim_record_id, a_keyword, key_freq,status  FROM sim_keyword").fetchall()
        c.close()
        conn.close()
        df=pd.DataFrame(data,columns=None)
        #df.columns= ['sim_record_id', 'a_keyword', 'key_freq','status']
        df.columns= ['id','sim_record_id', 'a_keyword', 'key_freq','status',]
        g=[]
        for i in df.groupby(by=['sim_record_id']):
            local=[]
            for k,l,m,n,o in zip(i[1]['id'].values, i[1]['sim_record_id'].values,i[1]['a_keyword'].values,i[1]['key_freq'].values,i[1]['status'].values):
                local.append([k,l,m,n,o])
            g.append(local)
        return g
     except:
        print('eccept happend')        


def record_update(a_keys):
    ids=[i[1] for i in a_keys]
    akeys=[i[0] for i in a_keys]
    conn = sqlite3.connect('sim_keywords.db')
    c=conn.cursor()
    table_sec_id=[]
    for i in ids:
        table_sec_id.append(c.execute("SELECT table_second_id  FROM sim_keyword where sim_record_id==(?)",(i,)).fetchall())
    c.close()
    conn.close()
    sec_ids=[]
    for i in table_sec_id:
        sec_ids.append([k for j in i for k in j])
    conn = sqlite3.connect('keywords_second.db')
    c=conn.cursor()
    table_first_ids=[]
    for i in sec_ids:
        local=[]
        for j in i:
            local.append(c.execute("SELECT table_first_ids  FROM keyword where id==(?)",(j,)).fetchall())
        table_first_ids.append(local)
    first_ids=[]
    for i in table_first_ids:
        first_ids.append([m.strip() for j in i for k in j for l in k for m in l[1:l.find(']')].split(',')])
    c.close()
    conn.close()
    conn = sqlite3.connect('keywords_first.db')
    c=conn.cursor()
    for i,j in enumerate(first_ids):
          for k in j:
              c.execute("UPDATE keyword  SET a_keyword =(?) where id ==(?)",(akeys[i],k))
              conn.commit()
    c.close()
    conn.close()
