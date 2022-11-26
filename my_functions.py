from ast import keyword
from itertools import groupby
import os
import csv
import sys
import collections
from difflib import SequenceMatcher
import pandas as pd
from fuzzywuzzy import fuzz
import sqlite3
import re


csv.field_size_limit(sys.maxsize)


f= open('country_name_full.txt','r')
l=f.readlines()
country_name= [i.strip().lower() for i in l]



def similar(a, b):
    return SequenceMatcher(None, a, b).ratio()

def touch(fname):
    open(fname, 'a').close()
    os.utime(fname, None)
    
     


def r_csv(nfile):
    faulty_row=[]
    with open(nfile, newline='') as f:
        reader= csv.reader(f)
        count=1
        for row in reader:
            if len(row)!=54:
                faulty_row.append(count)
                count+=1
            else:
                count+=1
    return faulty_row                

def file_name(name):
    s=name[name.find('.'):].strip()
    if s=='.csv':
        return True
    else:
        return False


def dashboard(records):
    n_record=len(records)
    
    n_author=len([j.strip() for i in records for j in str(i[2]).split(';')[:-1]])
    u_author=len(collections.Counter([j for i in records for j in i[2].split(';')[:-1]]).keys())
    na_author= sum([True for i in records if i[2]=='[No author id available]'])
    u_author= u_author-na_author
    n_author= n_author-na_author
    na_abst= sum([True for i in records if i[17]=='[No abstract available]'])
    n_source= len(collections.Counter([i[5] for i in records]).keys())
    na_source= sum([True for i in records if str(i[5])=='None'])
    n_source= n_source-na_source

    n_akeyword=len([j.strip().lower() for i in records for j in str(i[18]).split(';')])
    na_akeyword= collections.Counter([j.strip().lower() for i in records for j in str(i[18]).split(';')])['none']
    u_akeyword = len(collections.Counter([j.strip().lower() for i in records for j in str(i[18]).split(';')]).keys())
    n_akeyword= n_akeyword-na_akeyword
    u_akeyword=u_akeyword-na_akeyword
    return (n_record, n_author, u_author, na_author, n_source, na_source,n_akeyword, u_akeyword, na_akeyword, na_abst)

def dashboard_analysis(record):
    #id,auth,citation,journal_name
    data=[[i[0],f"{i[1].split(',')[0]}@{i[4]}",i[12],i[5]] for i in record]
    return data


def year_data(records,l_year=0):
       
       year= list(set([int(i[4]) for i in records]))
       year.sort(reverse=True)
       if l_year!=0:
             year=year[:year.index(l_year)]
             year_dic=collections.Counter([int(i[4]) for i in records if int(i[4]) in year])
             hold=[year_dic[y] for y in year]
             return (year,hold)

       else:
            year_dic=collections.Counter([int(i[4]) for i in records])
            hold=[year_dic[y] for y in year]
            return (year,hold)

def journal_analysis(records, top=50):
    df=pd.DataFrame(records)

    journal= collections.Counter([i[5] for i in records]).most_common()
    journal_name= [i[0] for i in journal]
    journal_top = [i[0] for i in journal[:50]]  
    
    journal_dic=collections.Counter([i[5] for i in records])
    hold1=[journal_dic[j] for j in journal_top]
    hold2=[journal_dic[j] for j in journal_name]
    issn_top=[]
    for i in journal_top:
        test=[]
        for j in df.groupby([5]).groups[i]:
            test.append(df.iloc[j][44])
        issn_top.append(list(set(test)))
    issn=[]
    for i in journal_name:
        test=[]
        for j in df.groupby([5]).groups[i]:
            test.append(df.iloc[j][44])
        issn.append(list(set(test)))        

    return (journal_top,hold1,journal_name,hold2,issn_top,issn)

def journal_annual_trend(word,records,plots):
    row_data=[]
    for i,j in enumerate(records):
        row_data.append([j[5],i])
    if plots==0:    
        try:
            df=pd.DataFrame(row_data,columns=None)
            df.columns= ['j_name', 'record_id']
            touch('journal_first.db')
            conn = sqlite3.connect('journal_first.db')
            c = conn.cursor()
            c.execute('''CREATE TABLE journal(id INTEGER PRIMARY KEY AUTOINCREMENT,
                    'j_name' text, 'record_id' int)''')
            df.to_sql('journal', conn, if_exists='append', index = False)
            record_id=c.execute("SELECT record_id FROM journal where j_name=(?)",(word,)).fetchall()
            record_ids=[j for i in record_id for j in i]
            c.close()
            conn.close()
            conn = sqlite3.connect('record.db')
            c = conn.cursor()
            records=c.execute('''SELECT * FROM record''').fetchall()
            years=[int(records[i][4]) for i in record_ids]
            c.close()
            conn.close()
            year_= list(set(years))
            year_.sort(reverse=True)
            year_dic=collections.Counter(years)
            hold=[year_dic[y] for y in year_]
            return (year_,hold)

        except:
            return (0,0)

    else:
        try:
            conn = sqlite3.connect('journal_first.db')
            c = conn.cursor()
            record_id=c.execute("SELECT record_id FROM journal where j_name=(?)",(word,)).fetchall()
            record_ids=[j for i in record_id for j in i]
            c.close()
            conn.close()
            conn = sqlite3.connect('record.db')
            c = conn.cursor()
            records=c.execute('''SELECT * FROM record''').fetchall()
            years=[int(records[i][4]) for i in record_ids]
            c.close()
            conn.close()
            year_= list(set(years))
            year_.sort(reverse=True)
            year_dic=collections.Counter(years)
            hold=[year_dic[y] for y in year_]
            return (year_,hold)

        except:
             return (0,0)                   




def country_analysis(records, top=50, frac=True):  
    c_name_na=0
    c_name_dic={}
    for i in records:
             c_name=[]
             for j in i[16].split(';'):
                 if j[j.rindex(',')+1:].strip().lower() in country_name:
                     c_name.append(j[j.rindex(',')+1:].strip().lower())
                 elif len(j[j.rindex(',')+1:].strip().lower())>4:
                      for c_names in country_name:
                         if similar(j[j.rindex(',')+1:].strip().lower(),c_names)>0.7:
                                c_name.append(c_names)
                                break   
             if len(c_name)==0:
                c_name_na+=1
             else:
                for c in list(set(c_name)):
                    if c in c_name_dic.keys():
                        c_name_dic[c][0]+=1
                        c_name_dic[c][1]+=c_name.count(c)/len(c_name)
                    else:
                        c_name_dic.update({c:[1,1]})
    c_name_dic={k: v for k, v in sorted(c_name_dic.items(), key=lambda item: item[1], reverse=True)}               
    return (c_name_dic.keys(),c_name_dic.values(), c_name_na)     

def country_annual_trend(word,records,plots):
    if plots==0:
        try:
            c_data=[]
            for index,i in enumerate(records):
                c_name=[]
                for j in i[16].split(';'):
                    if j[j.rindex(',')+1:].strip().lower() in country_name:
                        c_name.append(j[j.rindex(',')+1:].strip().lower())
                    elif len(j[j.rindex(',')+1:].strip().lower())>4:
                        for c_names in country_name:
                            if similar(j[j.rindex(',')+1:].strip().lower(),c_names)>0.7:
                                c_name.append(c_names)
                                break
                if len(c_name)!=0:
                    for c in list(set(c_name)):
                        c_data.append([c,index])
            df=pd.DataFrame(c_data,columns=None)
            df.columns= ['c_name', 'record_id']
            touch('country_first.db')
            conn = sqlite3.connect('country_first.db')
            c = conn.cursor()
            c.execute('''CREATE TABLE country(id INTEGER PRIMARY KEY AUTOINCREMENT,
                        'c_name' text, 'record_id' int)''')
            df.to_sql('country', conn, if_exists='append', index = False)
            record_id=c.execute("SELECT record_id FROM country where c_name=(?)",(word,)).fetchall()
            record_ids=[j for i in record_id for j in i]
            c.close()
            conn.close()
            conn = sqlite3.connect('record.db')
            c = conn.cursor()
            records=c.execute('''SELECT * FROM record''').fetchall()
            years=[int(records[i][4]) for i in record_ids]
            c.close()
            conn.close()
            year_= list(set(years))
            year_.sort(reverse=True)
            year_dic=collections.Counter(years)
            hold=[year_dic[y] for y in year_]
            return (year_,hold)
        except:
              return (0,0)  
    else:
        try:
            conn = sqlite3.connect('country_first.db')
            c = conn.cursor()
            record_id=c.execute("SELECT record_id FROM country where c_name=(?)",(word,)).fetchall()
            record_ids=[j for i in record_id for j in i]
            c.close()
            conn.close()
            conn = sqlite3.connect('record.db')
            c = conn.cursor()
            records=c.execute('''SELECT * FROM record''').fetchall()
            years=[int(records[i][4]) for i in record_ids]
            c.close()
            conn.close()
            year_= list(set(years))
            year_.sort(reverse=True)
            year_dic=collections.Counter(years)
            hold=[year_dic[y] for y in year_]
            return (year_,hold)

        except:
             return (0,0)               




                 
def author_analysis(records):
    author_dic={}
    author_err=0
    for i in records:
        if len(i[1].split(','))==len(i[2].split(';'))-1:
            if len(i[16].split(';'))==len(i[1].split(',')):
                for j,k,l in zip(i[1].split(','),i[2].split(';')[:-1],i[16].split(';')):
                    if k in author_dic.keys():
                        author_dic[k][0]+=1
                    else:
                        author_dic.update({k:[1,j,l]})
            else:
                author_err+=1            
        elif len(i[1].split(';'))== len(i[2].split(';'))-1:
            if len(i[16].split(';'))== len(i[2].split(';'))-1:
                for j,k,l in zip(i[1].split(';'),i[2].split(';')[:-1],i[16].split(';')):
                    if k in author_dic.keys():      
                        author_dic[k][0]+=1
                    else:
                        author_dic.update({k:[1,j,l]})
            else:
                author_err+=1            
        else:
                author_err+=1
    author_dic_sorted={k: v for k, v in sorted(author_dic.items(), key=lambda item: item[1], reverse=True)}

    return (author_dic_sorted.values(),author_err)

def keywords_analysis(records):
    keyword_dic={}
    keyword_err=0
    for i in records:
       try:
            for j in i[18].split(';'):
                if j.strip().lower() in keyword_dic.keys():
                    keyword_dic[j.strip().lower()]+=1
                else:
                    keyword_dic.update({j.strip().lower():1})
       except:
            keyword_err+=1             
    keyword_dic_sorted={k: v for k, v in sorted(keyword_dic.items(), key=lambda item: item[1], reverse=True)}
    return (keyword_dic_sorted.keys(), keyword_dic_sorted.values(),keyword_err)

def keywords_analysis_again(a_keywords):
    keyword_dic={}
    for i in a_keywords:
        for j in i:
         try:
             if j.strip().lower() in keyword_dic.keys():
                keyword_dic[j.strip().lower()]+=1
             else:
                keyword_dic.update({j.strip().lower():1})
         except:
            pass
    keyword_dic_sorted={k: v for k, v in sorted(keyword_dic.items(), key=lambda item: item[1], reverse=True)}
    return (keyword_dic_sorted.keys(), keyword_dic_sorted.values())               



def keywords_process(records):
    keyword_dic={}
    keyword_err=0
    for i in records:
       try:
            for j in i[18].split(';'):
                if j.strip().lower() in keyword_dic.keys():
                    keyword_dic[j.strip().lower()]+=1
                else:
                    keyword_dic.update({j.strip().lower():1})
       except:
            keyword_err+=1  
    keyword_dic_sorted={k: v for k, v in sorted(keyword_dic.items(), key=lambda item: item[1], reverse=True)}
    sim_keywords=[]
    sim_err=0
    
    for i in range(len(keyword_dic.keys())):
        try:
            local_sim=[[list(keyword_dic.keys())[i],list(keyword_dic.values())[i]]]
            for j in range(i+1, len(keyword_dic.keys())):
                if fuzz.ratio(list(keyword_dic.keys())[i],list(keyword_dic.keys())[j])>95:
                    local_sim.append([list(keyword_dic.keys())[j],list(keyword_dic.values())[j]])
            if len(local_sim)>1:
                sim_keywords.append(local_sim)
        except:
            sim_err+=1
    
    key_= list(keyword_dic_sorted.keys())
    value_ = list(keyword_dic_sorted.values())
    for i in range(len(key_)):
        local_sim=[(key_[i],value_[i])]
        for j in range(i+1,len(key_)):
            if fuzz.ratio(key_[i],key_[j])>95:
                local_sim.append((key_[j],value_[j]))
        if len(local_sim)>1:
              sim_keywords.append(local_sim)      
      
    return (sim_keywords,sim_err)

def keywords_database(records):
     row_data=[]
     for i in range(len(records)):
       try:
         for j,k in enumerate(records[i][18].split(';')):
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

def keywords_database(records):
     row_data=[]
     for i in range(len(records)):
       try:
         for j,k in enumerate(records[i][18].split(';')):
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

def keyword_primary(test,level):
    if level==0:
        if os.path.exists('sim_keywords.db'):
             conn = sqlite3.connect('sim_keywords.db')
             c = conn.cursor()
             data=c.execute("SELECT id,sim_record_id, a_keyword, key_freq,status  FROM sim_keyword").fetchall()
             c.close()
             conn.close()
             df=pd.DataFrame(data,columns=None)
             df.columns= ['id','sim_record_id', 'a_keyword', 'key_freq','status']
             g=[]
             for i in df.groupby(by=['sim_record_id']):
                 local=[]
                 for k,l,m,n,o in zip(i[1]['id'].values, i[1]['sim_record_id'].values,i[1]['a_keyword'].values,i[1]['key_freq'].values,i[1]['status'].values):
                    if o!=1:
                        local.append([k,l,m,n,o])
                 if len(local)>1:       
                    g.append(local)
             return g
        else:    
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
            data=c.execute("SELECT id,sim_record_id, a_keyword, key_freq,status  FROM sim_keyword").fetchall()
            c.close()
            conn.close()
            df=pd.DataFrame(data,columns=None)
            df.columns= ['id','sim_record_id', 'a_keyword', 'key_freq','status']
            g=[]
            for i in df.groupby(by=['sim_record_id']):
                local=[]
                for k,l,m,n,o in zip(i[1]['id'].values, i[1]['sim_record_id'].values,i[1]['a_keyword'].values,i[1]['key_freq'].values,i[1]['status'].values):
                    if o!=1:
                       local.append([k,l,m,n,o])
                if len(local)>1:       
                   g.append(local)
            return g
    elif level==1:
        if os.path.exists('sim_primary.db'):
             conn = sqlite3.connect('sim_primary.db')
             c = conn.cursor()
             data=c.execute("SELECT id,sim_record_id, a_keyword, key_freq,status  FROM sim_keyword").fetchall()
             c.close()
             conn.close()
             df=pd.DataFrame(data,columns=None)
             df.columns= ['id','sim_record_id', 'a_keyword', 'key_freq','status']
             g=[]
             for i in df.groupby(by=['sim_record_id']):
                 local=[]
                 for k,l,m,n,o in zip(i[1]['id'].values, i[1]['sim_record_id'].values,i[1]['a_keyword'].values,i[1]['key_freq'].values,i[1]['status'].values):
                    if o!=1:
                        local.append([k,l,m,n,o])
                 if len(local)>1:       
                    g.append(local)
             return g
        else:    
            test_id= [i[0] for i in test]
            test_keyword=[i[1] for i in test]
            test_freq=[len(i[2][1:i[2].find(']')].split(',')) for i in test]
            sim_keywords=[]
            for i in range(len(test_id)):
                    local_sim=[(test_id[i],test_keyword[i],test_freq[i])]
                    for j in range(i+1,len(test_id)):
                        if fuzz.ratio(test_keyword[i],test_keyword[j])>90:
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
            touch('sim_primary.db')
            conn = sqlite3.connect('sim_primary.db')
            c = conn.cursor()
            c.execute('''CREATE TABLE sim_keyword(id INTEGER PRIMARY KEY AUTOINCREMENT,'sim_record_id' int, 'a_keyword' text, 'key_freq' int, 'status' int, 'table_second_id' int)''')
            df.to_sql('sim_keyword', conn, if_exists='append', index = False)
            data=c.execute("SELECT id,sim_record_id, a_keyword, key_freq,status  FROM sim_keyword").fetchall()
            c.close()
            conn.close()
            df=pd.DataFrame(data,columns=None)
            df.columns= ['id','sim_record_id', 'a_keyword', 'key_freq','status']
            g=[]
            for i in df.groupby(by=['sim_record_id']):
                local=[]
                for k,l,m,n,o in zip(i[1]['id'].values, i[1]['sim_record_id'].values,i[1]['a_keyword'].values,i[1]['key_freq'].values,i[1]['status'].values):
                    local.append([k,l,m,n,o])
                g.append(local)
            return g            
    elif level==2:
        if os.path.exists('sim_secondary.db'):
             conn = sqlite3.connect('sim_secondary.db')
             c = conn.cursor()
             data=c.execute("SELECT id,sim_record_id, a_keyword, key_freq,status  FROM sim_keyword").fetchall()
             c.close()
             conn.close()
             df=pd.DataFrame(data,columns=None)
             df.columns= ['id','sim_record_id', 'a_keyword', 'key_freq','status']
             g=[]
             for i in df.groupby(by=['sim_record_id']):
                 local=[]
                 for k,l,m,n,o in zip(i[1]['id'].values, i[1]['sim_record_id'].values,i[1]['a_keyword'].values,i[1]['key_freq'].values,i[1]['status'].values):
                    if o!=1:
                       local.append([k,l,m,n,o])
                 if len(local)>1:
                    g.append(local)
             return g
        else:    
            test_id= [i[0] for i in test]
            test_keyword=[i[1] for i in test]
            test_freq=[len(i[2][1:i[2].find(']')].split(',')) for i in test]
            sim_keywords=[]
            for i in range(len(test_id)):
                    local_sim=[(test_id[i],test_keyword[i],test_freq[i])]
                    for j in range(i+1,len(test_id)):
                        if fuzz.ratio(test_keyword[i],test_keyword[j])>85:
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
            touch('sim_secondary.db')
            conn = sqlite3.connect('sim_secondary.db')
            c = conn.cursor()
            c.execute('''CREATE TABLE sim_keyword(id INTEGER PRIMARY KEY AUTOINCREMENT,'sim_record_id' int, 'a_keyword' text, 'key_freq' int, 'status' int, 'table_second_id' int)''')
            df.to_sql('sim_keyword', conn, if_exists='append', index = False)
            data=c.execute("SELECT id,sim_record_id, a_keyword, key_freq,status  FROM sim_keyword").fetchall()
            c.close()
            conn.close()
            df=pd.DataFrame(data,columns=None)
            df.columns= ['id','sim_record_id', 'a_keyword', 'key_freq','status']
            g=[]
            for i in df.groupby(by=['sim_record_id']):
                local=[]
                for k,l,m,n,o in zip(i[1]['id'].values, i[1]['sim_record_id'].values,i[1]['a_keyword'].values,i[1]['key_freq'].values,i[1]['status'].values):
                    local.append([k,l,m,n,o])
                g.append(local)
            return g         
    elif level==3:
        if os.path.exists('sim_tertiary.db'):
             conn = sqlite3.connect('sim_tertiary.db')
             c = conn.cursor()
             data=c.execute("SELECT id,sim_record_id, a_keyword, key_freq,status  FROM sim_keyword").fetchall()
             c.close()
             conn.close()
             df=pd.DataFrame(data,columns=None)
             df.columns= ['id','sim_record_id', 'a_keyword', 'key_freq','status']
             g=[]
             for i in df.groupby(by=['sim_record_id']):
                 local=[]
                 for k,l,m,n,o in zip(i[1]['id'].values, i[1]['sim_record_id'].values,i[1]['a_keyword'].values,i[1]['key_freq'].values,i[1]['status'].values):
                    if o!=1:
                       local.append([k,l,m,n,o])
                 if len(local)>1:
                    g.append(local)
             return g
        else:    
            test_id= [i[0] for i in test]
            test_keyword=[i[1] for i in test]
            test_freq=[len(i[2][1:i[2].find(']')].split(',')) for i in test]
            sim_keywords=[]
            for i in range(len(test_id)):
                    local_sim=[(test_id[i],test_keyword[i],test_freq[i])]
                    for j in range(i+1,len(test_id)):
                        if fuzz.partial_ratio(test_keyword[i],test_keyword[j])==100:
                            local_sim.append((test_id[j],test_keyword[j],test_freq[j]))
                        elif fuzz.partial_ratio(test_keyword[j],test_keyword[i])==100:
                             local_sim.append((test_id[j],test_keyword[j],test_freq[j]))
                        else:
                            pass        
                    if len(local_sim)>1:
                        sim_keywords.append(local_sim)
            row_data=[]
            for i,j in enumerate(sim_keywords):
                    for k in j:
                        row_data.append([i,k[0],k[1],k[2],0])
            df=pd.DataFrame(row_data,columns=None)
            df.columns= ['sim_record_id', 'table_second_id','a_keyword', 'key_freq','status']
            df.sort_values(by = ['sim_record_id', 'key_freq'], ascending = [True, False],inplace=True)
            touch('sim_tertiary.db')
            conn = sqlite3.connect('sim_tertiary.db')
            c = conn.cursor()
            c.execute('''CREATE TABLE sim_keyword(id INTEGER PRIMARY KEY AUTOINCREMENT,'sim_record_id' int, 'a_keyword' text, 'key_freq' int, 'status' int, 'table_second_id' int)''')
            df.to_sql('sim_keyword', conn, if_exists='append', index = False)
            data=c.execute("SELECT id,sim_record_id, a_keyword, key_freq,status  FROM sim_keyword").fetchall()
            c.close()
            conn.close()
            df=pd.DataFrame(data,columns=None)
            df.columns= ['id','sim_record_id', 'a_keyword', 'key_freq','status']
            g=[]
            for i in df.groupby(by=['sim_record_id']):
                local=[]
                for k,l,m,n,o in zip(i[1]['id'].values, i[1]['sim_record_id'].values,i[1]['a_keyword'].values,i[1]['key_freq'].values,i[1]['status'].values):
                    local.append([k,l,m,n,o])
                g.append(local)
            return g
    elif level==4:
        if os.path.exists('sim_fourth.db'):
             conn = sqlite3.connect('sim_fourth.db')
             c = conn.cursor()
             data=c.execute("SELECT id,sim_record_id, a_keyword, key_freq,status  FROM sim_keyword").fetchall()
             c.close()
             conn.close()
             df=pd.DataFrame(data,columns=None)
             df.columns= ['id','sim_record_id', 'a_keyword', 'key_freq','status']
             g=[]
             for i in df.groupby(by=['sim_record_id']):
                 local=[]
                 for k,l,m,n,o in zip(i[1]['id'].values, i[1]['sim_record_id'].values,i[1]['a_keyword'].values,i[1]['key_freq'].values,i[1]['status'].values):
                    if o!=1:
                       local.append([k,l,m,n,o])
                 if len(local)>1:
                    g.append(local)
             return g
        else:    
            test_id= [i[0] for i in test]
            test_keyword=[i[1] for i in test]
            test_freq=[len(i[2][1:i[2].find(']')].split(',')) for i in test]
            sim_keywords=[]
            for i in range(len(test_id)):
                    local_sim=[(test_id[i],test_keyword[i],test_freq[i])]
                    for j in range(i+1,len(test_id)):
                        if fuzz.token_sort_ratio(test_keyword[i],test_keyword[j])>90:
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
            touch('sim_fourth.db')
            conn = sqlite3.connect('sim_fourth.db')
            c = conn.cursor()
            c.execute('''CREATE TABLE sim_keyword(id INTEGER PRIMARY KEY AUTOINCREMENT,'sim_record_id' int, 'a_keyword' text, 'key_freq' int, 'status' int, 'table_second_id' int)''')
            df.to_sql('sim_keyword', conn, if_exists='append', index = False)
            data=c.execute("SELECT id,sim_record_id, a_keyword, key_freq,status  FROM sim_keyword").fetchall()
            c.close()
            conn.close()
            df=pd.DataFrame(data,columns=None)
            df.columns= ['id','sim_record_id', 'a_keyword', 'key_freq','status']
            g=[]
            for i in df.groupby(by=['sim_record_id']):
                local=[]
                for k,l,m,n,o in zip(i[1]['id'].values, i[1]['sim_record_id'].values,i[1]['a_keyword'].values,i[1]['key_freq'].values,i[1]['status'].values):
                    local.append([k,l,m,n,o])
                g.append(local)
            return g
    elif level==5:
        if os.path.exists('sim_fifth.db'):
             conn = sqlite3.connect('sim_fifth.db')
             c = conn.cursor()
             data=c.execute("SELECT id,sim_record_id, a_keyword, key_freq,status  FROM sim_keyword").fetchall()
             c.close()
             conn.close()
             df=pd.DataFrame(data,columns=None)
             df.columns= ['id','sim_record_id', 'a_keyword', 'key_freq','status']
             g=[]
             for i in df.groupby(by=['sim_record_id']):
                 local=[]
                 for k,l,m,n,o in zip(i[1]['id'].values, i[1]['sim_record_id'].values,i[1]['a_keyword'].values,i[1]['key_freq'].values,i[1]['status'].values):
                    if o!=1:
                       local.append([k,l,m,n,o])
                 if len(local)>1:
                    g.append(local)
             return g
        else:    
            test_id= [i[0] for i in test]
            test_keyword=[i[1] for i in test]
            test_freq=[len(i[2][1:i[2].find(']')].split(',')) for i in test]
            sim_keywords=[]
            for i in range(len(test_id)):
                    local_sim=[(test_id[i],test_keyword[i],test_freq[i])]
                    for j in range(i+1,len(test_id)):
                        if fuzz.token_set_ratio(test_keyword[i],test_keyword[j])>90:
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
            touch('sim_fifth.db')
            conn = sqlite3.connect('sim_fifth.db')
            c = conn.cursor()
            c.execute('''CREATE TABLE sim_keyword(id INTEGER PRIMARY KEY AUTOINCREMENT,'sim_record_id' int, 'a_keyword' text, 'key_freq' int, 'status' int, 'table_second_id' int)''')
            df.to_sql('sim_keyword', conn, if_exists='append', index = False)
            data=c.execute("SELECT id,sim_record_id, a_keyword, key_freq,status  FROM sim_keyword").fetchall()
            c.close()
            conn.close()
            df=pd.DataFrame(data,columns=None)
            df.columns= ['id','sim_record_id', 'a_keyword', 'key_freq','status']
            g=[]
            for i in df.groupby(by=['sim_record_id']):
                local=[]
                for k,l,m,n,o in zip(i[1]['id'].values, i[1]['sim_record_id'].values,i[1]['a_keyword'].values,i[1]['key_freq'].values,i[1]['status'].values):
                    local.append([k,l,m,n,o])
                g.append(local)
            return g                        
    else:
        pass

def keyword_primary_update(level):
     if level==0:
           conn = sqlite3.connect('keywords_first.db')
           c=conn.cursor()
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
           os.remove('keywords_second.db')
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
           os.remove('sim_keywords.db')
           touch('sim_keywords.db')
           conn = sqlite3.connect('sim_keywords.db')
           c = conn.cursor()
           c.execute('''CREATE TABLE sim_keyword(id INTEGER PRIMARY KEY AUTOINCREMENT,'sim_record_id' int, 'a_keyword' text, 'key_freq' int, 'status' int, 'table_second_id' int)''')
           df.to_sql('sim_keyword', conn, if_exists='append', index = False)
           data=c.execute("SELECT id,sim_record_id, a_keyword, key_freq,status  FROM sim_keyword").fetchall()
           c.close()
           conn.close()
           df=pd.DataFrame(data,columns=None)
           df.columns= ['id','sim_record_id', 'a_keyword', 'key_freq','status',]
           g=[]
           for i in df.groupby(by=['sim_record_id']):
                local=[]
                for k,l,m,n,o in zip(i[1]['id'].values, i[1]['sim_record_id'].values,i[1]['a_keyword'].values,i[1]['key_freq'].values,i[1]['status'].values):
                    local.append([k,l,m,n,o])
                g.append(local)
           return g
     elif level==1:
           conn = sqlite3.connect('keywords_first.db')
           c=conn.cursor()
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
           os.remove('keywords_second.db')
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
                    if fuzz.ratio(test_keyword[i],test_keyword[j])>90:
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
           os.remove('sim_primary.db')
           touch('sim_primary.db')
           conn = sqlite3.connect('sim_primary.db')
           c = conn.cursor()
           c.execute('''CREATE TABLE sim_keyword(id INTEGER PRIMARY KEY AUTOINCREMENT,'sim_record_id' int, 'a_keyword' text, 'key_freq' int, 'status' int, 'table_second_id' int)''')
           df.to_sql('sim_keyword', conn, if_exists='append', index = False)
           data=c.execute("SELECT id,sim_record_id, a_keyword, key_freq,status  FROM sim_keyword").fetchall()
           c.close()
           conn.close()
           df=pd.DataFrame(data,columns=None)
           df.columns= ['id','sim_record_id', 'a_keyword', 'key_freq','status',]
           g=[]
           for i in df.groupby(by=['sim_record_id']):
                local=[]
                for k,l,m,n,o in zip(i[1]['id'].values, i[1]['sim_record_id'].values,i[1]['a_keyword'].values,i[1]['key_freq'].values,i[1]['status'].values):
                    local.append([k,l,m,n,o])
                g.append(local)
           return g
     elif level==2:
           conn = sqlite3.connect('keywords_first.db')
           c=conn.cursor()
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
           os.remove('keywords_second.db')
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
                    if fuzz.ratio(test_keyword[i],test_keyword[j])>85:
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
           os.remove('sim_secondary.db')
           touch('sim_secondary.db')
           conn = sqlite3.connect('sim_secondary.db')
           c = conn.cursor()
           c.execute('''CREATE TABLE sim_keyword(id INTEGER PRIMARY KEY AUTOINCREMENT,'sim_record_id' int, 'a_keyword' text, 'key_freq' int, 'status' int, 'table_second_id' int)''')
           df.to_sql('sim_keyword', conn, if_exists='append', index = False)
           data=c.execute("SELECT id,sim_record_id, a_keyword, key_freq,status  FROM sim_keyword").fetchall()
           c.close()
           conn.close()
           df=pd.DataFrame(data,columns=None)
           df.columns= ['id','sim_record_id', 'a_keyword', 'key_freq','status',]
           g=[]
           for i in df.groupby(by=['sim_record_id']):
                local=[]
                for k,l,m,n,o in zip(i[1]['id'].values, i[1]['sim_record_id'].values,i[1]['a_keyword'].values,i[1]['key_freq'].values,i[1]['status'].values):
                    local.append([k,l,m,n,o])
                g.append(local)
           return g           
     elif level==3:
           conn = sqlite3.connect('keywords_first.db')
           c=conn.cursor()
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
           os.remove('keywords_second.db')
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
                    if fuzz.partial_ratio(test_keyword[i],test_keyword[j])==100:
                            local_sim.append((test_id[j],test_keyword[j],test_freq[j]))
                    elif fuzz.partial_ratio(test_keyword[j],test_keyword[i])==100:
                            local_sim.append((test_id[j],test_keyword[j],test_freq[j]))
                    else:
                            pass
               if len(local_sim)>1:
                    sim_keywords.append(local_sim)
           row_data=[]
           for i,j in enumerate(sim_keywords):
                 for k in j:
                     row_data.append([i,k[0],k[1],k[2],0])
           df=pd.DataFrame(row_data,columns=None)
           df.columns= ['sim_record_id', 'table_second_id','a_keyword', 'key_freq','status']
           df.sort_values(by = ['sim_record_id', 'key_freq'], ascending = [True, False],inplace=True)
           os.remove('sim_tertiary.db')
           touch('sim_tertiary.db')
           conn = sqlite3.connect('sim_tertiary.db')
           c = conn.cursor()
           c.execute('''CREATE TABLE sim_keyword(id INTEGER PRIMARY KEY AUTOINCREMENT,'sim_record_id' int, 'a_keyword' text, 'key_freq' int, 'status' int, 'table_second_id' int)''')
           df.to_sql('sim_keyword', conn, if_exists='append', index = False)
           data=c.execute("SELECT id,sim_record_id, a_keyword, key_freq,status  FROM sim_keyword").fetchall()
           c.close()
           conn.close()
           df=pd.DataFrame(data,columns=None)
           df.columns= ['id','sim_record_id', 'a_keyword', 'key_freq','status',]
           g=[]
           for i in df.groupby(by=['sim_record_id']):
                local=[]
                for k,l,m,n,o in zip(i[1]['id'].values, i[1]['sim_record_id'].values,i[1]['a_keyword'].values,i[1]['key_freq'].values,i[1]['status'].values):
                    local.append([k,l,m,n,o])
                g.append(local)
           return g
     elif level==4:
           conn = sqlite3.connect('keywords_first.db')
           c=conn.cursor()
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
           os.remove('keywords_second.db')
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
                        if fuzz.token_sort_ratio(test_keyword[i],test_keyword[j])>90:
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
           os.remove('sim_fourth.db')
           touch('sim_fourth.db')
           conn = sqlite3.connect('sim_fourth.db')
           c = conn.cursor()
           c.execute('''CREATE TABLE sim_keyword(id INTEGER PRIMARY KEY AUTOINCREMENT,'sim_record_id' int, 'a_keyword' text, 'key_freq' int, 'status' int, 'table_second_id' int)''')
           df.to_sql('sim_keyword', conn, if_exists='append', index = False)
           data=c.execute("SELECT id,sim_record_id, a_keyword, key_freq,status  FROM sim_keyword").fetchall()
           c.close()
           conn.close()
           df=pd.DataFrame(data,columns=None)
           df.columns= ['id','sim_record_id', 'a_keyword', 'key_freq','status',]
           g=[]
           for i in df.groupby(by=['sim_record_id']):
                local=[]
                for k,l,m,n,o in zip(i[1]['id'].values, i[1]['sim_record_id'].values,i[1]['a_keyword'].values,i[1]['key_freq'].values,i[1]['status'].values):
                    local.append([k,l,m,n,o])
                g.append(local)
           return g
     elif level==5:
           conn = sqlite3.connect('keywords_first.db')
           c=conn.cursor()
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
           os.remove('keywords_second.db')
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
                    if fuzz.token_set_ratio(test_keyword[i],test_keyword[j])>90:
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
           os.remove('sim_fifth.db')
           touch('sim_fifth.db')
           conn = sqlite3.connect('sim_fifth.db')
           c = conn.cursor()
           c.execute('''CREATE TABLE sim_keyword(id INTEGER PRIMARY KEY AUTOINCREMENT,'sim_record_id' int, 'a_keyword' text, 'key_freq' int, 'status' int, 'table_second_id' int)''')
           df.to_sql('sim_keyword', conn, if_exists='append', index = False)
           data=c.execute("SELECT id,sim_record_id, a_keyword, key_freq,status  FROM sim_keyword").fetchall()
           c.close()
           conn.close()
           df=pd.DataFrame(data,columns=None)
           df.columns= ['id','sim_record_id', 'a_keyword', 'key_freq','status',]
           g=[]
           for i in df.groupby(by=['sim_record_id']):
                local=[]
                for k,l,m,n,o in zip(i[1]['id'].values, i[1]['sim_record_id'].values,i[1]['a_keyword'].values,i[1]['key_freq'].values,i[1]['status'].values):
                    local.append([k,l,m,n,o])
                g.append(local)
           return g
     else:
        pass



def record_update(a_keys,level):
    ids=[i[1] for i in a_keys]
    akeys=[i[0] for i in a_keys]
    if level==0:
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
    elif level==1:
            conn = sqlite3.connect('sim_primary.db')
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

    elif level==2:
            conn = sqlite3.connect('sim_secondary.db')
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
    elif level==3:
            conn = sqlite3.connect('sim_tertiary.db')
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
    elif level==4:
            conn = sqlite3.connect('sim_fourth.db')
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
    elif level==5:
            conn = sqlite3.connect('sim_fifth.db')
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
    else:
        pass
def keyword_annual_trend(word):
        conn = sqlite3.connect('keywords_first.db')
        c=conn.cursor()
        record_id=c.execute("SELECT record_id FROM keyword where a_keyword=(?)",(word,)).fetchall()
        record_ids=[j for i in record_id for j in i]
        c.close()
        conn.close()
        conn = sqlite3.connect('record.db')
        c = conn.cursor()
        records=c.execute('''SELECT * FROM record''').fetchall()
        years=[int(records[i][4]) for i in record_ids]
        c.close()
        conn.close()
        year_= list(set(years))
        year_.sort(reverse=True)
        year_dic=collections.Counter(years)
        hold=[year_dic[y] for y in year_]
        return (year_,hold)    


def seperate(s,r='.,' ):
    i = s.find(r)
    while i != -1:
        yield i
        i = s.find(r, i+1)
def individual(s):
    p=re.compile(r"(\(\d{4}\))")
    q=re.compile(r"(\d{4})")
    s=s.lower()
    try:
        if type(p.search(s))==re.Match:
                year=p.search(s)[0][1:-1]
                y=p.search(s).span()[0]
                upper=s[:y]
                if len(upper.strip())!=0:
                        sep_index=[i for i in seperate(upper)]
                        if len(sep_index)==1 or sep_index[0]>35:
                            if sep_index[0]>35:
                                sep_index=[i for i in seperate(upper,r=',')]
                                if len(sep_index)==1:
                                    if sep_index[0]>35:
                                            u=-1
                                    else:    
                                            u=sep_index[0]
                                   
                                else:
                                    u=sep_index[-1]
                                    for i in range(len(sep_index)-1):
                                        if sep_index[0]>35:
                                            u=-1
                                        elif (sep_index[i+1]-sep_index[i])>35:
                                            if i==0:
                                                u=sep_index[0]
                                                break
                                            else:
                                                u=sep_index[i-1]
                                                break
                            else:    
                                u=sep_index[0]
                        else:
                            u=sep_index[-1]
                            for i in range(len(sep_index)-1):
                                if sep_index[0]>35:
                                    u=-1
                                elif (sep_index[i+1]-sep_index[i])>35:
                                    if i==0:
                                        u=sep_index[0]
                                        break
                                    else:
                                        u=sep_index[i-1]
                                        break
                        if u!=-1:        
                            author=upper[:u+1]
                            title=upper[u+2:]
                            remain=s[y+6:]
                            source= remain[:remain.find(',')]
                            remain=remain[remain.find(',')+1:]
                            if sum([True if k in s else False for k in ['www','http','doi','coi']])!=0:
                                hold=[len(remain)]
                                for l in ['www','http','doi','coi']:
                                    if remain.find(l)>0:
                                        hold.append(remain.find(l))
                                remain=remain[:min(hold)]    
                            return [0,author,title, year, source,remain]
                        else:
                            return [5]
                else:
                    return [2, year]
        else:
            if '.,' not in s:
                    if sum([True if k in s else False for k in ['www','http','phd','patent','thesis','dissertation']])==0:
                        if type(q.search(s))==re.Match:
                                  return [8]
                        else:
                                  return [3]
                    else:
                                  return [6]
            else:
                    if sum([True if k in s else False for k in ['www','http','phd','patent','thesis','dissertation']])==0:
                        if type(q.search(s))==re.Match:
                            return [8]
                        else:
                            return [4]
                    else:
                         return [6]
            
    except:
        try:
                if type(p.search(s))==re.Match:
                            year=p.search(s)[0][1:-1]
                            y=p.search(s).span()[0]
                            upper=s[:y]
                            if len(upper.strip())!=0 and '.,' not in upper:
                                if ',' in upper:
                                    sep_index=[i for i in seperate(upper,r=',')]
                                    if len(sep_index)==1:
                                        if sep_index[0]>20:
                                            u=-1
                                        else:    
                                            u=sep_index[0]
                                    else:
                                        u=sep_index[-1]
                                        for i in range(len(sep_index)-1):
                                            if sep_index[0]>20:
                                                u=-1
                                                break
                                            elif (sep_index[i+1]-sep_index[i])>20:
                                                if i==0:
                                                    u=sep_index[0]
                                                    break
                                                else: 
                                                    u=sep_index[i-1]
                                                    break
                                        
                                    if u!=-1:
                                        author=upper[:u+1]
                                        title=upper[u+2:]
                                        remain=s[y+6:]
                                        source= remain[:remain.find(',')]
                                        remain=remain[remain.find(',')+1:]
                                        if sum([True if k in s else False for k in ['www','http','doi','coi']])!=0:
                                                hold=[len(remain)]
                                                for l in ['www','http','doi','coi']:
                                                    if remain.find(l)>0:
                                                        hold.append(remain.find(l))
                                                remain=remain[:min(hold)]  
                                        return [1,author,title, year,source, remain]
                                    else:
                                        return [5]
                                else:
                                    return [7]
                            else:
                                    if len(upper.strip())==0:
                                        return [2, year]

                elif '.,' not in s:
                          if sum([True if k in s else False for k in ['www','http','phd','patent','thesis','dissertation']])==0:
                            if type(q.search(s))==re.Match:
                                  return [8]
                            else:
                                  return [3]
                                
                          else:
                                  return [6]
                else:
                    if sum([True if k in s else False for k in ['www','http','phd','patent','thesis','dissertation']])==0:
                        if type(q.search(s))==re.Match:
                            return [8]
                        else:
                            return [4]
                    else:
                         return [6]
        except:
            return [5]


def reference_analysis(record):
    ref=[i[35] for i in record]
    ref_=[]
    for index,j in enumerate(ref):
        j_split=j.split(';')
        for i in range(len(j_split)):
            ref_.append([j_split[i],index,i])
    test=[]
    for i in ref_:
        hold=individual(i[0])
        test.append([i,hold[0]])
    correct=collections.Counter([i[1] for i in test])[0]
    correct+=collections.Counter([i[1] for i in test])[1]
    incorrect=len(ref_)-correct
    return (len(ref_),correct, incorrect)

def reference_analysis2(record):
    id=[i[0] for i in record]
    auth= [f"{i[1].split(',')[0]}@{i[4]}"  for i in record]
    ref_format1=[]
    ref_format2=[]
    ref_format3=[]
    for i in record:
        local=[]
        for j in i[35].split(';'):
            try:
                local.append(individual(j)[0])
            except:
                local.append(-1)    
        correct=collections.Counter(local)[0]
        correct+=collections.Counter(local)[1]
        incorrect=len(local)-correct
        ref_format1.append(len(local))
        ref_format2.append(correct)
        ref_format3.append(incorrect)
    citation= [i[12] for i in record]
    data=[]
    for i in range(len(auth)):
          data.append([auth[i],citation[i],ref_format1[i],ref_format2[i],ref_format3[i],id[i]])  
    return data









