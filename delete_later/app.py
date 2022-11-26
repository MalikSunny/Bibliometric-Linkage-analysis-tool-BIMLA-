import sqlite3
from flask import Flask, render_template,request, url_for
import pandas as pd
from my_functions import keywords_database, touch , record_update
import os 
app =Flask(__name__)


@app.route('/', methods=['POST', 'GET'])


def index():
     return render_template("index.html")

@app.route('/keyword/<int:page>', methods=['POST', 'GET'])

def keyword_process(page):
   if request.method =='POST':
        if len(request.form.getlist('checkbox_name'))>0:
            conn = sqlite3.connect('sim_keywords.db')
            c = conn.cursor()
            for i in request.form.getlist('checkbox_name'):
                 c.execute("UPDATE sim_keyword  SET a_keyword =(?), status=1 where sim_record_id ==(?)",(request.form.get(f'keyword_name_{i}'),request.form.get(f'simid_name_{i}')))
                 conn.commit()
            data=c.execute('''SELECT id,sim_record_id, a_keyword, key_freq,status  FROM sim_keyword''').fetchall()
            c.close()
            conn.close() 
            a_keys=[(request.form.get(f'keyword_name_{i}'),request.form.get(f'simid_name_{i}')) for i in request.form.getlist('checkbox_name')]
            record_update(a_keys)
            df=pd.DataFrame(data,columns=None)
            df.columns= ['id','sim_record_id', 'a_keyword', 'key_freq','status']
            g=[]
            for i in df.groupby(by=['sim_record_id']):
               local=[]
               for k,l,m,n,o in zip(i[1]['id'].values, i[1]['sim_record_id'].values,i[1]['a_keyword'].values,i[1]['key_freq'].values,i[1]['status'].values):
                   if o!=1:
                      local.append([k,l,m,n,o])
               if len(local)>0:
                  g.append(local)
            pages=[p for p in range(0,len(g),25)]
            if page>-1 and page < len(pages): 
                   g=g[pages[page]:pages[page+1]] 
            elif page<0:
                       page=0
                       g=g[:25]
            elif page> len(pages):
                       page=len(pages)
                       g=g[pages[-1]:]
            else:
                pass
            return  render_template('meta_analysis.html', data=g,page=page)

        else:
           conn = sqlite3.connect('sim_keywords.db')
           c = conn.cursor()
           #c.execute("UPDATE sim_keyword  SET a_keyword =(?), status=1 where sim_record_id ==(?)",(request.form["keyword_name"],request.form["simId"]))
           #conn.commit()
           data=c.execute('''SELECT id,sim_record_id, a_keyword, key_freq,status  FROM sim_keyword''').fetchall()
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
               if len(local)>0:
                  g.append(local)
           pages=[p for p in range(0,len(g),25)]
           if page>-1 and page < len(pages): 
                   g=g[pages[page]:pages[page+1]]
           elif page<0:
               page=0
               g=g[:25]
           elif page> len(pages):
                page=len(pages)
                g=g[pages[-1]:]
           else:
               pass
           return  render_template('meta_analysis.html', data=g, page=page)
   else:
       if os.path.exists('sim_keywords.db'):
             conn = sqlite3.connect('sim_keywords.db')
             c = conn.cursor()
             data=c.execute('''SELECT id,sim_record_id, a_keyword, key_freq,status  FROM sim_keyword''').fetchall()
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
                 if len(local)>0:
                    g.append(local)
             pages=[p for p in range(0,len(g),25)]
             if page>-1 and page < len(pages):
                g=g[pages[page]:pages[page+1]]
             elif page<0:
                  page=0
                  g=g[:25]
             elif page> len(pages):
                  page=len(pages)
                  g=g[pages[-1]:]
             else:
                  pass
             return render_template('meta_analysis.html', data=g, page=page)
       else:
           conn = sqlite3.connect('record.db')
           c = conn.cursor()
           records=c.execute('''SELECT id,Author_Keywords FROM record''').fetchall()
           c.close()
           conn.close()
           g=keywords_database(records)
           conn = sqlite3.connect('sim_keywords.db')
           c = conn.cursor()
           data=c.execute('''SELECT id,sim_record_id, a_keyword, key_freq,status  FROM sim_keyword''').fetchall()
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
                 if len(local)>0:
                    g.append(local)
           pages=[p for p in range(0,len(g),25)]
           if page>-1 and page < len(pages):
                g=g[pages[page]:pages[page+1]]
           elif page<0:
                  page=0
                  g=g[:25]
           elif page> len(pages):
                  page=len(pages)
                  g=g[pages[-1]:]
           else:
                  pass
           return render_template('meta_analysis.html', data=g, page=page)
           






if __name__== "__main__":
    app.run(debug=True)
