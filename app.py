from ast import keyword
from email import header
import mimetypes
import sqlite3
from crypt import methods
from curses import flash
from tkinter.tix import IMAGE
from flask import Flask, render_template, url_for, request, redirect, flash, send_from_directory, Response, send_file
import pandas as pd
import numpy as np
import matplotlib
matplotlib.use('Agg')
from matplotlib.backends.backend_agg import FigureCanvasAgg as FigureCanvas
from matplotlib.figure import Figure
import matplotlib.pyplot as plt
import seaborn as sns
from flask_sqlalchemy import SQLAlchemy
from datetime import datetime
import os
import io
import shutil
import glob
from my_functions import dashboard_analysis, touch, r_csv, file_name, dashboard, year_data, journal_analysis, country_analysis, author_analysis, keyword_primary_update
from my_functions import keyword_annual_trend,keywords_analysis, keywords_database, record_update,keywords_process, keywords_analysis_again, keyword_primary
from my_functions import reference_analysis, reference_analysis2, individual, dashboard_analysis, journal_annual_trend,country_annual_trend
from io import BytesIO
import base64


IMAGE_UPLOAD = '/home/sunny/extra_learning/flask/oct5/flask/virt/bimla/static/images/'
UPLOAD_FOLDER= '/home/sunny/extra_learning/flask/oct5/flask/virt/bimla/projects/'
MULTI_FILES= '/home/sunny/extra_learning/flask/oct5/flask/virt/bimla/multi_files/'
ALLOWED_EXTENSIONS = {'csv'}

app =Flask(__name__)

app.config['SQLALCHEMY_DATABASE_URI']= 'sqlite:///record.db'
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER
app.config['IMAGE_UPLOAD'] = IMAGE_UPLOAD
app.config['SECRET_KEY'] = 'bimla secret key'

db= SQLAlchemy(app)

class record(db.Model):
    id=db.Column(db.Integer, primary_key=True)
    Authors= db.Column(db.String(), nullable=True)
    Author_ID= db.Column(db.String(), nullable=True)
    Title= db.Column(db.String(), nullable=True)
    Year= db.Column(db.Integer, nullable=True)
    Source_title= db.Column(db.String(), nullable=True)
    Volume= db.Column(db.String(), nullable=True)
    Issue= db.Column(db.String(), nullable=True)
    Art_No = db.Column(db.String(), nullable=True)
    Page_start= db.Column(db.String(), nullable=True)
    Page_end= db.Column(db.String(), nullable=True)
    Page_count= db.Column(db.String(), nullable=True)
    Cited_by= db.Column(db.String(), nullable=True)
    DOI= db.Column(db.String(), nullable=True)
    Link= db.Column(db.String(), nullable=True)
    Affiliations= db.Column(db.String(), nullable=True)
    Authors_with_affiliations= db.Column(db.String(), nullable=True)
    Abstract= db.Column(db.String(), nullable=True)
    Author_Keywords= db.Column(db.String(), nullable=True)
    Index_Keywords= db.Column(db.String(), nullable=True)
    Molecular_Sequence_Numbers= db.Column(db.String(), nullable=True)
    CAS= db.Column(db.String(), nullable=True)
    Tradenames= db.Column(db.String(), nullable=True)
    Manufacturers= db.Column(db.String(), nullable=True)
    Funding_Details= db.Column(db.String(), nullable=True)
    Funding_Text_1= db.Column(db.String(), nullable=True)
    Funding_Text_2= db.Column(db.String(), nullable=True)
    Funding_Text_3= db.Column(db.String(), nullable=True)
    Funding_Text_4= db.Column(db.String(), nullable=True)
    Funding_Text_5= db.Column(db.String(), nullable=True)
    Funding_Text_6= db.Column(db.String(), nullable=True)
    Funding_Text_7= db.Column(db.String(), nullable=True)
    Funding_Text_8= db.Column(db.String(), nullable=True)
    Funding_Text_9 = db.Column(db.String(), nullable=True)
    Funding_Text_10= db.Column(db.String(), nullable=True)
    References= db.Column(db.String(), nullable=True)
    Correspondence_Address= db.Column(db.String(), nullable=True)
    Editors= db.Column(db.String(), nullable=True)
    Sponsors= db.Column(db.String(), nullable=True)
    Publisher= db.Column(db.String(), nullable=True)
    Conference_name= db.Column(db.String(), nullable=True)
    Conference_date= db.Column(db.String(), nullable=True)
    Conference_location= db.Column(db.String(), nullable=True)
    Conference_code= db.Column(db.String(), nullable=True)
    ISSN= db.Column(db.String(), nullable=True)
    ISBN= db.Column(db.String(), nullable=True)
    CODEN= db.Column(db.String(), nullable=True)
    PubMed_ID= db.Column(db.String(), nullable=True)
    Language_of_Original_Document= db.Column(db.String(), nullable=True)
    Abbreviated_Source_Title= db.Column(db.String(), nullable=True)
    Document_Type= db.Column(db.String(), nullable=True)
    Publication_Stage= db.Column(db.String(), nullable=True)
    Open_Access= db.Column(db.String(), nullable=True)
    Source= db.Column(db.String(), nullable=True)
    EID= db.Column(db.String(), nullable=True)

    def __repr__(self):
        return '<title %r>' % self.id





@app.route('/', methods=['POST', 'GET'])


def index():

    return render_template("index.html")

@app.route('/new_project', methods=['POST','GET'])

def new_project():

    if request.method=='POST':
        if 'file' not in request.files:
            flash('No selected file')
            return  render_template('new_project.html')
        file = request.files['file']
        project_name= request.form['project_name']
        if file.filename=='':
            return  render_template('new_project_error.html', err=1)
        path = os.path.join(app.config['UPLOAD_FOLDER'], project_name)
        if os.path.exists(path):
                folder=[name for name in os.listdir(app.config['UPLOAD_FOLDER']) if os.path.isdir(os.path.join(app.config['UPLOAD_FOLDER'], name))]
                return render_template('new_project_error.html', err=2,folder=folder)

        else:    
                os.mkdir(path)      
                file.save(os.path.join(path,file.filename))
                os.chdir(path)
                try:
                    data= pd.read_csv(os.path.join(path,file.filename))
                    df=pd.DataFrame(data,columns=None)
                    df.columns= ['Authors', 'Author_ID', 'Title', 'Year', 'Source_title', 'Volume', 'Issue', 'Art_No', 'Page_start', 'Page_end', 'Page_count', 'Cited_by', 'DOI', 'Link', 'Affiliations', 'Authors_with_affiliations', 'Abstract', 'Author_Keywords', 'Index_Keywords', 'Molecular_Sequence_Numbers', 'CAS', 'Tradenames', 'Manufacturers', 'Funding_Details', 'Funding_Text_1', 'Funding_Text_2', 'Funding_Text_3', 'Funding_Text_4', 'Funding_Text_5', 'Funding_Text_6', 'Funding_Text_7', 'Funding_Text_8', 'Funding_Text_9', 'Funding_Text_10', 'ref_full', 'Correspondence_Address', 'Editors', 'Sponsors', 'Publisher', 'Conference_name', 'Conference_date', 'Conference_location', 'Conference_code', 'ISSN', 'ISBN', 'CODEN', 'PubMed_ID', 'Language_of_Original_Document', 'Abbreviated_Source_Title', 'Document_Type', 'Publication_Stage', 'Open_Access', 'Source', 'EID']
                    touch('record.db')
                    conn = sqlite3.connect('record.db')
                    c = conn.cursor()
                    
                    c.execute('''CREATE TABLE record(id INTEGER PRIMARY KEY AUTOINCREMENT,    
                        'Authors' text, 'Author_ID' text, 'Title' text, 'Year' int, 'Source_title' text, 'Volume' text, 'Issue' text, 'Art_No' text,
                        'Page_start' text, 'Page_end' text, 'Page_count' text, 'Cited_by' text, 'DOI' text, 'Link' text, 'Affiliations' text, 'Authors_with_affiliations' text,
                         'Abstract' text, 'Author_Keywords' text, 'Index_Keywords' text, 'Molecular_Sequence_Numbers' text, 'CAS' text, 'Tradenames' text, 
                         'Manufacturers' text, 'Funding_Details' text, 'Funding_Text_1' text, 'Funding_Text_2' text, 'Funding_Text_3' text, 'Funding_Text_4' text,
                        'Funding_Text_5' text, 'Funding_Text_6' text, 'Funding_Text_7' text, 'Funding_Text_8' text, 'Funding_Text_9' text, 'Funding_Text_10' text, 
                        'ref_full' text, 'Correspondence_Address' text, 'Editors' text, 'Sponsors' text, 'Publisher' text, 'Conference_name' text, 
                        'Conference_date' text, 'Conference_location' text, 'Conference_code' text, 'ISSN' text, 'ISBN' text, 'CODEN' text, 'PubMed_ID' text,
                         'Language_of_Original_Document' text, 'Abbreviated_Source_Title' text, 'Document_Type' text, 'Publication_Stage' text, 'Open_Access' text,
                          'Source' text, 'EID' text)''')


                    df.to_sql('record', conn, if_exists='append', index = False)
                    records=c.execute('''SELECT * FROM record''').fetchall()
                    return render_template('new_project_table.html', records=records)
                except:
                     faulty_rows=r_csv(file.filename)
                     if len(faulty_rows)>0:
                            flash(f'Error in reading submitted csv file : {file.filename} ')
                            flash('Submitted .csv file should have a standard format for each column of records')
                            return   render_template('new_project_error.html', err=3, faulty_rows=faulty_rows)  

    else:
           return render_template('new_project.html') 

@app.route('/existing_projects', methods=['POST','GET'])


def existing_projects():
    os.chdir(app.config['UPLOAD_FOLDER'])
    folder=[name for name in os.listdir(app.config['UPLOAD_FOLDER']) if os.path.isdir(os.path.join(app.config['UPLOAD_FOLDER'], name))]
    #url= [ url_for(folder) for i in folder]

    return render_template('existing_project.html', folder= folder) 

@app.route('/open_project/<folder_name>', methods=['POST','GET'])

def open_project(folder_name):
        path = os.path.join(app.config['UPLOAD_FOLDER'], folder_name)
        os.chdir(path)
     
        try:
            conn = sqlite3.connect('record.db')
            c = conn.cursor()
            records=c.execute('''SELECT * FROM record''').fetchall()
            r_info=dashboard(records)
            data=dashboard_analysis(records)
            return render_template('project_deshboard.html', r_info= r_info,data=data, folder_name= folder_name)
            #return render_template('new_project_table.html', records=records)
        except:
            return " project was not created successfully"

@app.route('/delete_project/<folder_name>', methods=['POST','GET'])

def delete_project(folder_name):
    path = os.path.join(app.config['UPLOAD_FOLDER'], folder_name)
    if os.path.exists(path):
        os.chdir(app.config['UPLOAD_FOLDER'])
        shutil.rmtree(path)
        folder=[name for name in os.listdir(app.config['UPLOAD_FOLDER']) if os.path.isdir(os.path.join(app.config['UPLOAD_FOLDER'], name))]
        return render_template('existing_project.html', folder= folder)


@app.route('/multiple_files', methods= ['POST','GET'])

def multiple_files():
    if request.method=='POST':
        if 'file' not in request.files:
            flash('No selected file')
            return  render_template('new_project.html')
        files = request.files.getlist("file")
        multi=[file.filename for file in files]
        for fname in multi:
            if not file_name(fname):
                return 'file {}  is not in csv format'.format(fname)

        path = os.path.join(MULTI_FILES, 'multiple_csv_file')
        if os.path.exists(path):
                    os.chdir(MULTI_FILES)
                    shutil.rmtree(path)
                    os.mkdir(path)
                    os.chdir(path)
                    for file in files:      
                          file.save(os.path.join(path,file.filename))
                    for fname in multi:        
                        faulty_rows=r_csv(fname)
                        if len(faulty_rows)>0:
                            flash(f'Error in reading submitted csv file : {fname} ')
                            flash('Submitted .csv file should have a standard format for each column of records')
                            return   render_template('new_project_error.html', err=3, faulty_rows=faulty_rows)  

                    csv_files = glob.glob(os.path.join(path, "*.csv"))
                    df_from_each_file = (pd.read_csv(f) for f in csv_files)
                    concatenated_df   = pd.concat(df_from_each_file, ignore_index=True)
                    concatenated_df.drop_duplicates()
                    concatenated_df.to_csv('combine_file.csv', index=False)
                    return send_from_directory(path,'combine_file.csv')      
        else:
                os.mkdir(path)
                os.chdir(path)
                for file in files:      
                        file.save(os.path.join(path,file.filename))

                for fname in multi:        
                     faulty_rows=r_csv(fname)
                     if len(faulty_rows)>0:
                            flash(f'Error in reading submitted csv file : {fname} ')
                            flash('Submitted .csv file should have a standard format for each column of records')
                            return   render_template('new_project_error.html', err=3, faulty_rows=faulty_rows)  
                csv_files = glob.glob(os.path.join(path, "*.csv"))
                df_from_each_file = (pd.read_csv(f) for f in csv_files)
                concatenated_df   = pd.concat(df_from_each_file, ignore_index=True)
                concatenated_df.drop_duplicates()
                concatenated_df.to_csv('combine_file.csv', index=False)
                return send_from_directory(path,'combine_file.csv') 
                        


        
    else:
        return render_template('multiple_csv.html')    

@app.route('/projects/<folder_name>/year',methods= ['POST','GET'])

def year(folder_name):
    path = os.path.join(app.config['UPLOAD_FOLDER'], folder_name)
    os.chdir(path)
    
    try:
            
            img = BytesIO()
            conn = sqlite3.connect('record.db')
            c = conn.cursor()
            records=c.execute('''SELECT * FROM record''').fetchall()
            d=year_data(records)       
            plt.plot(d[0],d[1])
            plt.savefig(img, format='png')
            plt.close()
            img.seek(0)
            plot_url = base64.b64encode(img.getvalue()).decode('utf8')
            data= [[i,j] for i,j in zip(d[0],d[1])]
            return render_template('meta_analysis.html', plot_url=plot_url, data=data, plot=1, folder_name=folder_name)

    except:
                return 'Some columns of year data are either empty or not a valid year value'


@app.route('/year_datas/<folder_name>',methods= ['POST','GET'])

def year_datas(folder_name):
           path = os.path.join(app.config['UPLOAD_FOLDER'], folder_name)
           os.chdir(path)
           try:
                conn = sqlite3.connect('record.db')
                c = conn.cursor()
                records=c.execute('''SELECT * FROM record''').fetchall()
                d=year_data(records)
                data= [[i,j] for i,j in zip(d[0],d[1])]
                df=pd.DataFrame(data, columns=['Year', 'Number of publication'])
                df.to_csv('year_publication.csv')
                return send_from_directory(path,'year_publication.csv')
           except:
                 return ' Error in downloading the data '  


@app.route('/projects/<folder_name>/journal',methods= ['POST','GET'])

def journal(folder_name):
    path = os.path.join(app.config['UPLOAD_FOLDER'], folder_name)
    os.chdir(path)
    
    try:
            
            img = BytesIO()
            conn = sqlite3.connect('record.db')
            c = conn.cursor()
            records=c.execute('''SELECT * FROM record''').fetchall()
            j=journal_analysis(records)
            data= [[i,k,l] for i,k,l in zip(j[2],j[3],j[5])]
            fig, ax = plt.subplots(figsize=(12, 8))
            people = [data[i][0] for i in range(10)]
            y_pos = np.arange(len(people))
            performance = [data[i][1] for i in range(10)]
            ax.barh(y_pos, performance, align='center')
            ax.set_yticks(y_pos, labels=people)
            ax.invert_yaxis()  # labels read top-to-bottom
            ax.set_xlabel('# of publications')
            ax.set_title('Top 10 Journals based on # of publication')
            plot_name= 'journal_count_{}.png'.format(folder_name)
            fig.savefig(os.path.join(app.config['IMAGE_UPLOAD'],plot_name),bbox_inches='tight')
            plt.close()
            
            #return render_template('meta_analysis.html',data=data, plot=2, folder_name=folder_name)
            return render_template('journal_table.html',data=data, folder_name=folder_name)
            

    except:
                return 'this is fucked up'

@app.route('/projects/<folder_name>/journal_datas',methods= ['POST','GET'])

def journal_datas(folder_name):
           path = os.path.join(app.config['UPLOAD_FOLDER'], folder_name)
           os.chdir(path)
           try:
                conn = sqlite3.connect('record.db')
                c = conn.cursor()
                records=c.execute('''SELECT * FROM record''').fetchall()
                j=journal_analysis(records)
                data= [[i,l,k] for i,l,k in zip(j[2],j[3],j[5])]
                df=pd.DataFrame(data, columns=['Name of the journal', 'Number of publication','ISSN'])
                df.to_csv('journal_publication.csv')
                return send_from_directory(path,'journal_publication.csv')
           except:
                 return ' Error in downloading the data ' 

@app.route('/projects/<folder_name>/country',methods= ['POST','GET'])

def country(folder_name):
    path = os.path.join(app.config['UPLOAD_FOLDER'], folder_name)
    os.chdir(path)
    
    try:
            
            conn = sqlite3.connect('record.db')
            c = conn.cursor()
            records=c.execute('''SELECT * FROM record''').fetchall()
            cou=country_analysis(records)
            data= [[i,j[0],j[1]] for i,j in zip(cou[0],cou[1])]
            fig, ax = plt.subplots(figsize=(12, 8))
            people = [data[i][0] for i in range(10)]
            y_pos = np.arange(len(people))
            performance = [data[i][1] for i in range(10)]

            ax.barh(y_pos, performance, align='center')
            ax.set_yticks(y_pos, labels=people)
            ax.invert_yaxis()  # labels read top-to-bottom
            ax.set_xlabel('# of publications')
            ax.set_title('Top 10 countries based on # of publication')
            plot_name= 'country_count_{}.png'.format(folder_name)
            fig.savefig(os.path.join(app.config['IMAGE_UPLOAD'],plot_name),bbox_inches='tight')
            plt.close()
            
            
            #return render_template('meta_analysis.html', data=data, plot=3, folder_name=folder_name)
            return render_template('country_table.html', data=data, folder_name=folder_name)
            

    except:
                return 'this is fucked up' 

@app.route('/projects/<folder_name>/country_datas',methods= ['POST','GET'])

def country_datas(folder_name):
           path = os.path.join(app.config['UPLOAD_FOLDER'], folder_name)
           os.chdir(path)
           try:
                conn = sqlite3.connect('record.db')
                c = conn.cursor()
                records=c.execute('''SELECT * FROM record''').fetchall()
                cou=country_analysis(records)
                data= [[i,j[0],j[1]] for i,j in zip(cou[0],cou[1])]
                df=pd.DataFrame(data, columns=['Name of the country', '# of publication','# of publication (fractional)'])
                df.to_csv('country_publication.csv')
                return send_from_directory(path,'country_publication.csv')
           except:
                 return ' Error in downloading the data ' 

@app.route('/projects/<folder_name>/author',methods= ['POST','GET'])


def author(folder_name):
    path = os.path.join(app.config['UPLOAD_FOLDER'], folder_name)
    os.chdir(path)
    
    try:
        conn = sqlite3.connect('record.db')
        c = conn.cursor()
        records=c.execute('''SELECT * FROM record ORDER BY Year DESC''').fetchall()
        author_name=author_analysis(records)
        print(author_name[1])
        data= [[i[0],i[1],i[2]] for i in author_name[0]]
        #if len(data)>50:
        #   data=[data[d] for d in range(50)]
        fig, ax = plt.subplots(figsize=(12, 8))
        people = [data[i][1] for i in range(10)]
        y_pos = np.arange(len(people))
        performance = [data[i][0] for i in range(10)]

        ax.barh(y_pos, performance, align='center')
        ax.set_yticks(y_pos, labels=people)
        ax.invert_yaxis()  # labels read top-to-bottom
        ax.set_xlabel('# of publications')
        ax.set_title('Top 10 Authors based on # of publication')
        plot_name= 'author_count_{}.png'.format(folder_name)
        fig.savefig(os.path.join(app.config['IMAGE_UPLOAD'],plot_name),bbox_inches='tight')
        plt.close()    
        #return render_template('meta_analysis.html',plot=4,data=data, folder_name=folder_name)
        return render_template('author_table.html',data=data, folder_name=folder_name)
            
    except:
         return 'fucked happended'    

@app.route('/projects/<folder_name>/author_datas',methods= ['POST','GET'])

def author_datas(folder_name):
           path = os.path.join(app.config['UPLOAD_FOLDER'], folder_name)
           os.chdir(path)
           try:
                conn = sqlite3.connect('record.db')
                c = conn.cursor()
                records=c.execute('''SELECT * FROM record ORDER BY Year DESC''').fetchall()
                author_name=author_analysis(records)
                data= [[i[0],i[1],i[2]] for i in author_name[0]]
                
                df=pd.DataFrame(data, columns=['Name of the Author', '# of publication','Affiliation'])
                df.to_csv('author_publication.csv')
                return send_from_directory(path,'author_publication.csv')
           except:
                 return ' Error in downloading the data ' 

@app.route('/projects/<folder_name>/author_keywords',methods= ['POST','GET'])

def author_keywords(folder_name):
    path = os.path.join(app.config['UPLOAD_FOLDER'], folder_name)
    os.chdir(path)
    
    try:
        if os.path.exists('keywords_first.db'):
            conn = sqlite3.connect('keywords_first.db')
            c = conn.cursor()
            a_keywords=c.execute('''SELECT a_keyword FROM keyword''').fetchall()
            c.close()
            conn.close()
            words=keywords_analysis_again(a_keywords)
            data= [[i,j] for i,j in zip(words[0],words[1])]
            """
            if len(data)>100:
                data=[data[d] for d in range(100)]
            return render_template('meta_analysis.html',data=data, plot=5,folder_name=folder_name)
            """
            return render_template('author_keyword_table.html',data=data, plot=5,folder_name=folder_name)
        else:
             conn = sqlite3.connect('record.db')
             c = conn.cursor()
             records=c.execute('''SELECT * FROM record''').fetchall()
             c.close()
             conn.close()
             g=keywords_database(records)
             conn = sqlite3.connect('keywords_first.db')
             c = conn.cursor()
             a_keywords=c.execute('''SELECT a_keyword FROM keyword''').fetchall()
             c.close()
             conn.close()
             words=keywords_analysis_again(a_keywords)
             data= [[i,j] for i,j in zip(words[0],words[1])]
             if len(data)>100:
                data=[data[d] for d in range(100)]
             return render_template('meta_analysis.html',data=data, plot=5,folder_name=folder_name)

    except:
         return 'fucked happended'  



@app.route('/projects/<folder_name>/keywords_processing',methods= ['POST','GET'])


def keyword_process(folder_name):
    path = os.path.join(app.config['UPLOAD_FOLDER'], folder_name)
    os.chdir(path)
    
    try:
    
        conn = sqlite3.connect('record.db')
        c = conn.cursor()
        records=c.execute('''SELECT * FROM record''').fetchall()
        keyword_sim=keywords_process(records)
        c.close()
        conn.close()
        print(keyword_sim[1])
        return render_template('meta_analysis.html',data=keyword_sim[0], plot=6,folder_name=folder_name)
    except:
        return 'fucked happened'    

@app.route('/projects/<folder_name>/keyword_manuals/<int:page>',methods=['POST', 'GET'])

def keyword_manual(page,folder_name):
   path = os.path.join(app.config['UPLOAD_FOLDER'], folder_name)
   os.chdir(path)
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
            return  render_template('meta_analysis.html', data=g,page=page, plot=6,folder_name=folder_name)
        else:
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
           return  render_template('meta_analysis.html', data=g, page=page , plot=6,folder_name=folder_name)
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
             elif page> len(pages):
                  page=len(pages)
                  g=g[pages[-1]:]
             else:
                  pass
             return render_template('meta_analysis.html', data=g, page=page, plot=6, folder_name=folder_name)
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
           return render_template('meta_analysis.html', data=g, page=page, plot=6, folder_name=folder_name)

@app.route('/projects/<folder_name>/keyword_advances/<int:level>/update/<int:page>',methods=['POST', 'GET'])

def keyword_advance(folder_name,page,level):
    path = os.path.join(app.config['UPLOAD_FOLDER'], folder_name)
    os.chdir(path)
    if request.method =='POST':
        if level==0:
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
                    record_update(a_keys,level)
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
                    return  render_template('meta_analysis.html', data=g,page=page, plot=7,folder_name=folder_name,level=level)
                else:
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
                    return  render_template('meta_analysis.html', data=g, page=page,plot=7,folder_name=folder_name,level=level)            
        elif level==1:
                if len(request.form.getlist('checkbox_name'))>0:
                    conn = sqlite3.connect('sim_primary.db')
                    c = conn.cursor()
                    for i in request.form.getlist('checkbox_name'):
                         c.execute("UPDATE sim_keyword  SET a_keyword =(?), status=1 where sim_record_id ==(?)",(request.form.get(f'keyword_name_{i}'),request.form.get(f'simid_name_{i}')))
                         conn.commit()
                    data=c.execute('''SELECT id,sim_record_id, a_keyword, key_freq,status  FROM sim_keyword''').fetchall()
                    c.close()
                    conn.close() 
                    a_keys=[(request.form.get(f'keyword_name_{i}'),request.form.get(f'simid_name_{i}')) for i in request.form.getlist('checkbox_name')]
                    record_update(a_keys,level)
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
                    return  render_template('meta_analysis.html', data=g,page=page, plot=7,folder_name=folder_name,level=level)
                else:
                    conn = sqlite3.connect('sim_primary.db')
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
                    return  render_template('meta_analysis.html', data=g, page=page,plot=7,folder_name=folder_name,level=level)        
        elif level==2:
                if len(request.form.getlist('checkbox_name'))>0:
                    conn = sqlite3.connect('sim_secondary.db')
                    c = conn.cursor()
                    for i in request.form.getlist('checkbox_name'):
                         c.execute("UPDATE sim_keyword  SET a_keyword =(?), status=1 where sim_record_id ==(?)",(request.form.get(f'keyword_name_{i}'),request.form.get(f'simid_name_{i}')))
                         conn.commit()
                    data=c.execute('''SELECT id,sim_record_id, a_keyword, key_freq,status  FROM sim_keyword''').fetchall()
                    c.close()
                    conn.close() 
                    a_keys=[(request.form.get(f'keyword_name_{i}'),request.form.get(f'simid_name_{i}')) for i in request.form.getlist('checkbox_name')]
                    record_update(a_keys,level)
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
                    return  render_template('meta_analysis.html', data=g,page=page, plot=7,folder_name=folder_name,level=level)
                else:
                    conn = sqlite3.connect('sim_secondary.db')
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
                    return  render_template('meta_analysis.html', data=g, page=page,plot=7,folder_name=folder_name,level=level)                                                  
        elif level==3:
                if len(request.form.getlist('checkbox_name'))>0:
                    conn = sqlite3.connect('sim_tertiary.db')
                    c = conn.cursor()
                    for i in request.form.getlist('checkbox_name'):
                         c.execute("UPDATE sim_keyword  SET a_keyword =(?), status=1 where sim_record_id ==(?)",(request.form.get(f'keyword_name_{i}'),request.form.get(f'simid_name_{i}')))
                         conn.commit()
                    data=c.execute('''SELECT id,sim_record_id, a_keyword, key_freq,status  FROM sim_keyword''').fetchall()
                    c.close()
                    conn.close() 
                    a_keys=[(request.form.get(f'keyword_name_{i}'),request.form.get(f'simid_name_{i}')) for i in request.form.getlist('checkbox_name')]
                    record_update(a_keys,level)
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
                    return  render_template('meta_analysis.html', data=g,page=page, plot=7,folder_name=folder_name,level=level)
                else:
                    conn = sqlite3.connect('sim_tertiary.db')
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
                    return  render_template('meta_analysis.html', data=g, page=page,plot=7,folder_name=folder_name,level=level)
        elif level==4:
                if len(request.form.getlist('checkbox_name'))>0:
                    conn = sqlite3.connect('sim_fourth.db')
                    c = conn.cursor()
                    for i in request.form.getlist('checkbox_name'):
                         c.execute("UPDATE sim_keyword  SET a_keyword =(?), status=1 where sim_record_id ==(?)",(request.form.get(f'keyword_name_{i}'),request.form.get(f'simid_name_{i}')))
                         conn.commit()
                    data=c.execute('''SELECT id,sim_record_id, a_keyword, key_freq,status  FROM sim_keyword''').fetchall()
                    c.close()
                    conn.close() 
                    a_keys=[(request.form.get(f'keyword_name_{i}'),request.form.get(f'simid_name_{i}')) for i in request.form.getlist('checkbox_name')]
                    record_update(a_keys,level)
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
                    return  render_template('meta_analysis.html', data=g,page=page, plot=7,folder_name=folder_name,level=level)
                else:
                    conn = sqlite3.connect('sim_fourth.db')
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
                    return  render_template('meta_analysis.html', data=g, page=page,plot=7,folder_name=folder_name,level=level)
        elif level==5:
                if len(request.form.getlist('checkbox_name'))>0:
                    conn = sqlite3.connect('sim_fifth.db')
                    c = conn.cursor()
                    for i in request.form.getlist('checkbox_name'):
                         c.execute("UPDATE sim_keyword  SET a_keyword =(?), status=1 where sim_record_id ==(?)",(request.form.get(f'keyword_name_{i}'),request.form.get(f'simid_name_{i}')))
                         conn.commit()
                    data=c.execute('''SELECT id,sim_record_id, a_keyword, key_freq,status  FROM sim_keyword''').fetchall()
                    c.close()
                    conn.close() 
                    a_keys=[(request.form.get(f'keyword_name_{i}'),request.form.get(f'simid_name_{i}')) for i in request.form.getlist('checkbox_name')]
                    record_update(a_keys,level)
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
                    return  render_template('meta_analysis.html', data=g,page=page, plot=7,folder_name=folder_name,level=level)
                else:
                    conn = sqlite3.connect('sim_fifth.db')
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
                    return  render_template('meta_analysis.html', data=g, page=page,plot=7,folder_name=folder_name,level=level)                                        
        else:
            pass  
    else:    
        if level==0:
            if os.path.exists('keywords_first.db'):
                conn = sqlite3.connect('keywords_second.db')
                c=conn.cursor()
                test=c.execute('''SELECT id,a_keyword, table_first_ids FROM keyword''').fetchall()
                c.close()
                conn.close()
                g=keyword_primary(test,level)
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
                return render_template('meta_analysis.html', data=g, page=page, plot=7, folder_name=folder_name,level=level)
            else:
                 conn = sqlite3.connect('record.db')
                 c = conn.cursor()
                 records=c.execute('''SELECT id,Author_Keywords FROM record''').fetchall()
                 c.close()
                 conn.close()
                 g=keywords_database(records)
                 conn = sqlite3.connect('keywords_second.db')
                 c=conn.cursor()
                 test=c.execute('''SELECT id,a_keyword, table_first_ids FROM keyword''').fetchall()
                 c.close()
                 conn.close()
                 g=keyword_primary(test,level)
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
                 return render_template('meta_analysis.html', data=g, page=page, plot=7, folder_name=folder_name,level=level)   
        elif level==1:
            if os.path.exists('keywords_first.db'):
                conn = sqlite3.connect('keywords_second.db')
                c=conn.cursor()
                test=c.execute('''SELECT id,a_keyword, table_first_ids FROM keyword''').fetchall()
                c.close()
                conn.close()
                g=keyword_primary(test,level)
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
                return render_template('meta_analysis.html', data=g, page=page, plot=7, folder_name=folder_name,level=level)
            else:
                 conn = sqlite3.connect('record.db')
                 c = conn.cursor()
                 records=c.execute('''SELECT id,Author_Keywords FROM record''').fetchall()
                 c.close()
                 conn.close()
                 g=keywords_database(records)
                 conn = sqlite3.connect('keywords_second.db')
                 c=conn.cursor()
                 test=c.execute('''SELECT id,a_keyword, table_first_ids FROM keyword''').fetchall()
                 c.close()
                 conn.close()
                 g=keyword_primary(test,level)
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
                 return render_template('meta_analysis.html', data=g, page=page, plot=7, folder_name=folder_name,level=level)
        elif level==2:
            if os.path.exists('keywords_first.db'):
                conn = sqlite3.connect('keywords_second.db')
                c=conn.cursor()
                test=c.execute('''SELECT id,a_keyword, table_first_ids FROM keyword''').fetchall()
                c.close()
                conn.close()
                g=keyword_primary(test,level)
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
                return render_template('meta_analysis.html', data=g, page=page, plot=7, folder_name=folder_name,level=level)
            else:
                 conn = sqlite3.connect('record.db')
                 c = conn.cursor()
                 records=c.execute('''SELECT id,Author_Keywords FROM record''').fetchall()
                 c.close()
                 conn.close()
                 g=keywords_database(records)
                 conn = sqlite3.connect('keywords_second.db')
                 c=conn.cursor()
                 test=c.execute('''SELECT id,a_keyword, table_first_ids FROM keyword''').fetchall()
                 c.close()
                 conn.close()
                 g=keyword_primary(test,level)
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
                 return render_template('meta_analysis.html', data=g, page=page, plot=7, folder_name=folder_name,level=level)                      
        elif level==3:
            if os.path.exists('keywords_first.db'):
                conn = sqlite3.connect('keywords_second.db')
                c=conn.cursor()
                test=c.execute('''SELECT id,a_keyword, table_first_ids FROM keyword''').fetchall()
                c.close()
                conn.close()
                g=keyword_primary(test,level)
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
                return render_template('meta_analysis.html', data=g, page=page, plot=7, folder_name=folder_name,level=level)
            else:
                 conn = sqlite3.connect('record.db')
                 c = conn.cursor()
                 records=c.execute('''SELECT id,Author_Keywords FROM record''').fetchall()
                 c.close()
                 conn.close()
                 g=keywords_database(records)
                 conn = sqlite3.connect('keywords_second.db')
                 c=conn.cursor()
                 test=c.execute('''SELECT id,a_keyword, table_first_ids FROM keyword''').fetchall()
                 c.close()
                 conn.close()
                 g=keyword_primary(test,level)
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
                 return render_template('meta_analysis.html', data=g, page=page, plot=7, folder_name=folder_name,level=level)
        elif level==4:
            if os.path.exists('keywords_first.db'):
                conn = sqlite3.connect('keywords_second.db')
                c=conn.cursor()
                test=c.execute('''SELECT id,a_keyword, table_first_ids FROM keyword''').fetchall()
                c.close()
                conn.close()
                g=keyword_primary(test,level)
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
                return render_template('meta_analysis.html', data=g, page=page, plot=7, folder_name=folder_name,level=level)
            else:
                 conn = sqlite3.connect('record.db')
                 c = conn.cursor()
                 records=c.execute('''SELECT id,Author_Keywords FROM record''').fetchall()
                 c.close()
                 conn.close()
                 g=keywords_database(records)
                 conn = sqlite3.connect('keywords_second.db')
                 c=conn.cursor()
                 test=c.execute('''SELECT id,a_keyword, table_first_ids FROM keyword''').fetchall()
                 c.close()
                 conn.close()
                 g=keyword_primary(test,level)
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
                 return render_template('meta_analysis.html', data=g, page=page, plot=7, folder_name=folder_name,level=level)
        elif level==5:
            if os.path.exists('keywords_first.db'):
                conn = sqlite3.connect('keywords_second.db')
                c=conn.cursor()
                test=c.execute('''SELECT id,a_keyword, table_first_ids FROM keyword''').fetchall()
                c.close()
                conn.close()
                g=keyword_primary(test,level)
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
                return render_template('meta_analysis.html', data=g, page=page, plot=7, folder_name=folder_name,level=level)
            else:
                 conn = sqlite3.connect('record.db')
                 c = conn.cursor()
                 records=c.execute('''SELECT id,Author_Keywords FROM record''').fetchall()
                 c.close()
                 conn.close()
                 g=keywords_database(records)
                 conn = sqlite3.connect('keywords_second.db')
                 c=conn.cursor()
                 test=c.execute('''SELECT id,a_keyword, table_first_ids FROM keyword''').fetchall()
                 c.close()
                 conn.close()
                 g=keyword_primary(test,level)
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
                 return render_template('meta_analysis.html', data=g, page=page, plot=7, folder_name=folder_name,level=level)
        else:              
                 return 'else happend'      

@app.route('/projects/<folder_name>/keyword_advances/<int:level>/database_update/<int:page>',methods=['POST', 'GET'])

def keyword_advance_update(folder_name,page,level):
        path = os.path.join(app.config['UPLOAD_FOLDER'], folder_name)
        os.chdir(path)
        g=keyword_primary_update(level)
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
        return render_template('meta_analysis.html', data=g, page=page, plot=7, folder_name=folder_name,level=level)

@app.route('/projects/<folder_name>/keyword_year_trend/<word>',methods= ['POST','GET'])

def keyword_trend(word,folder_name):
    path = os.path.join(app.config['UPLOAD_FOLDER'], folder_name)
    os.chdir(path)
    print(word)
    data= keyword_annual_trend(word)
    fig, ax = plt.subplots(figsize=(12, 8))
    plt.plot(data[0],data[1])
    plot_name= 'keyword_trend_{}.png'.format(folder_name)
    fig.savefig(os.path.join(app.config['IMAGE_UPLOAD'],plot_name),bbox_inches='tight')
    plt.close()
    return render_template('meta_analysis.html', plot=9,folder_name=folder_name)
      
@app.route('/projects/<folder_name>/references',methods= ['POST','GET'])

def reference(folder_name):
    path = os.path.join(app.config['UPLOAD_FOLDER'], folder_name)
    os.chdir(path)
    try:
            conn = sqlite3.connect('record.db')
            c = conn.cursor()
            records=c.execute('''SELECT * FROM record''').fetchall()
            c.close()
            conn.close()
            data1=reference_analysis(records)
            print(data1)
            data2=reference_analysis2(records)
            print(data2[0])
            return render_template('reference_table.html',data1=data1,data2=data2,folder_name=folder_name)
    except:
         return 'fuck happend'        

@app.route('/projects/<folder_name>/records/<int:record_number>', methods=['POST','GET'])

def individual_record(folder_name,record_number):
    path = os.path.join(app.config['UPLOAD_FOLDER'], folder_name)
    os.chdir(path)
    try:
          conn = sqlite3.connect('record.db')
          c = conn.cursor()
          rec=c.execute("SELECT * FROM record where id=={}".format(record_number)).fetchone()
          c.close()
          conn.close()
          try:
              aff=rec[15].split(';')
          except:
              aff=''
          try:        
              kword=rec[18].split(';')
          except:
              kword=''
          try:        
              ref=[[i,individual(i)[0]] for i in rec[35].split(';')]
          except:
              ref=''
          ref_full=rec[35]    
          pattern=[i for i in range(len(ref_full)) if ref_full.startswith(';', i)]
          pattern.insert(0, 0)
          r_pattern=[(pattern[i],pattern[i+1]) for i in range(len(pattern)-1)]
          return render_template('individual_record.html',data=rec,aff=aff,kword=kword,ref=ref,ref_full=ref_full,r_pattern=r_pattern,folder_name=folder_name,record_number=record_number)

    except:     
          return 'shit happened'

@app.route('/projects/<folder_name>/reference_update/<int:record_number>/ref/<int:ref_number>', methods=['POST','GET'])

def ref_update(folder_name,record_number,ref_number):
    path = os.path.join(app.config['UPLOAD_FOLDER'], folder_name)
    os.chdir(path)
    if request.method =='POST':
        conn = sqlite3.connect('record.db')
        c = conn.cursor()
        c.execute("UPDATE record  SET ref_full=(?) where id==(?)",(str(request.form.get('ref_message_name')),record_number))
        conn.commit()
        rec=c.execute("SELECT * FROM record where id=={}".format(record_number)).fetchone()
        c.close()
        conn.close()
        try:
              aff=rec[15].split(';')
        except:
              aff=''
        try:        
              kword=rec[18].split(';')
        except:
              kword=''
        try:        
              ref=[[i,individual(i)[0]] for i in rec[35].split(';')]
        except:
              ref=''
        ref_full=rec[35]    
        pattern=[i for i in range(len(ref_full)) if ref_full.startswith(';', i)]
        pattern.insert(0, 0)
        r_pattern=[(pattern[i],pattern[i+1]) for i in range(len(pattern)-1)]
        return render_template('individual_record.html',data=rec,aff=aff,kword=kword,ref=ref,ref_full=ref_full,r_pattern=r_pattern,folder_name=folder_name,record_number=record_number)

    else:    
        try:
            conn = sqlite3.connect('record.db')
            c = conn.cursor()
            rec=c.execute("SELECT * FROM record where id=={}".format(record_number)).fetchone()
            c.close()
            conn.close()
            ref_full=rec[35]
            pattern=[i for i in range(len(ref_full)) if ref_full.startswith(';', i)]
            pattern.insert(0, 0)
            r_pattern=(pattern[ref_number],pattern[ref_number+1])
            return render_template('reference_update.html',ref_full=ref_full,r_pattern=r_pattern,folder_name=folder_name,record_number=record_number)
        except:
            return 'shit happened'


@app.route('/projects/<folder_name>/yearly_trend/<int:num>/index/<string:word>', methods=['POST','GET'])

def year_trend(folder_name,num,word):
    path = os.path.join(app.config['UPLOAD_FOLDER'], folder_name)
    os.chdir(path)
    try:
        if num==1:
            if os.path.exists('journal_first.db'):
                conn = sqlite3.connect('record.db')
                c = conn.cursor()
                records=c.execute('''SELECT * FROM record''').fetchall()
                c.close()
                conn.close()
                data= journal_annual_trend(word,records,plots=1)
                fig, ax = plt.subplots(figsize=(12, 8))
                ax.plot(data[0],data[1])
                ax.set_xlabel('Year',fontsize=14)
                ax.set_ylabel('Frequency',fontsize=14)
                ax.set_title(f'Annual trend for journal \n {word}',fontsize=18)
                plot_name= 'yearly_trend_{}.png'.format(folder_name)
                fig.savefig(os.path.join(app.config['IMAGE_UPLOAD'],plot_name),bbox_inches='tight')
                plt.close()
                return render_template('meta_year_trend.html', plot=1,folder_name=folder_name,word=word)
            else:
                conn = sqlite3.connect('record.db')
                c = conn.cursor()
                records=c.execute('''SELECT * FROM record''').fetchall()
                c.close()
                conn.close()
                data= journal_annual_trend(word,records,plot=0)
                fig, ax = plt.subplots(figsize=(12, 8))
                ax.plot(data[0],data[1])
                ax.set_xlabel('Year',fontsize=14)
                ax.set_ylabel('Frequency',fontsize=14)
                ax.set_title(f'Annual trend for journal \n {word}',fontsize=18)
                plot_name= 'yearly_trend_{}.png'.format(folder_name)
                fig.savefig(os.path.join(app.config['IMAGE_UPLOAD'],plot_name),bbox_inches='tight')
                plt.close()
                return render_template('meta_year_trend.html', plot=1,folder_name=folder_name,word=word)
        elif num==2:
                data= keyword_annual_trend(word)
                fig, ax = plt.subplots(figsize=(12, 8))
                ax.plot(data[0],data[1])
                ax.set_xlabel('Year',fontsize=14)
                ax.set_ylabel('Frequency',fontsize=14)
                ax.set_title(f'Annual trend for keyword \n {word}',fontsize=18)
                plot_name= 'yearly_trend_{}.png'.format(folder_name)
                fig.savefig(os.path.join(app.config['IMAGE_UPLOAD'],plot_name),bbox_inches='tight')
                plt.close()
                return render_template('meta_year_trend.html', plot=2,folder_name=folder_name,word=word)
        elif num==3:
            if os.path.exists('country_first.db'):
                conn = sqlite3.connect('record.db')
                c = conn.cursor()
                records=c.execute('''SELECT * FROM record''').fetchall()
                c.close()
                conn.close()
                data= country_annual_trend(word,records,plots=1)
                fig, ax = plt.subplots(figsize=(12, 8))
                ax.plot(data[0],data[1])
                ax.set_xlabel('Year',fontsize=14)
                ax.set_ylabel('Frequency',fontsize=14)
                ax.set_title(f'Annual trend for country \n {word}',fontsize=18)
                plot_name= 'yearly_trend_{}.png'.format(folder_name)
                fig.savefig(os.path.join(app.config['IMAGE_UPLOAD'],plot_name),bbox_inches='tight')
                plt.close()
                return render_template('meta_year_trend.html', plot=3,folder_name=folder_name,word=word)
            else:
                conn = sqlite3.connect('record.db')
                c = conn.cursor()
                records=c.execute('''SELECT * FROM record''').fetchall()
                c.close()
                conn.close()
                data= country_annual_trend(word,records,plots=0)
                fig, ax = plt.subplots(figsize=(12, 8))
                ax.plot(data[0],data[1])
                ax.set_xlabel('Year',fontsize=14)
                ax.set_ylabel('Frequency',fontsize=14)
                ax.set_title(f'Annual trend for journal \n {word}',fontsize=18)
                plot_name= 'yearly_trend_{}.png'.format(folder_name)
                fig.savefig(os.path.join(app.config['IMAGE_UPLOAD'],plot_name),bbox_inches='tight')
                plt.close()
                return render_template('meta_year_trend.html', plot=3,folder_name=folder_name,word=word) 
                        
        else:
                return 'else hit'        
            

    except:
        return 'shit happend'


@app.route('/projects/<folder_name>/yearly_trend/<int:num>/index/<string:word>/data_download', methods=['POST','GET'])

def year_trend_data(folder_name,num,word):
    path = os.path.join(app.config['UPLOAD_FOLDER'], folder_name)
    os.chdir(path)
    try:
        if num==1:
            conn = sqlite3.connect('record.db')
            c = conn.cursor()
            records=c.execute('''SELECT * FROM record''').fetchall()
            c.close()
            conn.close()
            data= journal_annual_trend(word,records,plots=1)
            data1=[[i,j] for i,j in zip(data[0],data[1])]
            df=pd.DataFrame(data1, columns=['Year', '# of occurences'])
            df.to_csv('year_trend_data.csv',index=False)
            return send_from_directory(path,'year_trend_data.csv')
        elif num==2:
            data= keyword_annual_trend(word)
            data1=[[i,j] for i,j in zip(data[0],data[1])]
            df=pd.DataFrame(data1, columns=['Year', '# of occurences'])
            df.to_csv('year_trend_data.csv',index=False)
            return send_from_directory(path,'year_trend_data.csv')
        elif num==3:
            conn = sqlite3.connect('record.db')
            c = conn.cursor()
            records=c.execute('''SELECT * FROM record''').fetchall()
            c.close()
            conn.close()
            data= country_annual_trend(word,records,plots=1)
            data1=[[i,j] for i,j in zip(data[0],data[1])]
            df=pd.DataFrame(data1, columns=['Year', '# of occurences'])
            df.to_csv('year_trend_data.csv',index=False)
            return send_from_directory(path,'year_trend_data.csv')   
        else:
            return 'else hit'       
    except:
            return ' Error in downloading the data '      



@app.route('/projects/<folder_name>/table_tutorial/', methods=['POST','GET'])

def table_try(folder_name):
    path = os.path.join(app.config['UPLOAD_FOLDER'], folder_name)
    os.chdir(path)
    try:
          conn = sqlite3.connect('record.db')
          c = conn.cursor()
          rec=c.execute("SELECT * FROM record").fetchall()
          c.close()
          conn.close()
          ref_full=rec[10][35]
          pattern=[i for i in range(len(ref_full)) if ref_full.startswith(';', i)]
          pattern.insert(0, 0)
          r_pattern=[(pattern[i],pattern[i+1]) for i in range(len(pattern)-1)]
          print(r_pattern)
          return render_template('table_try.html',data=ref_full,r_pattern=r_pattern,we=10)

    except:     
          return 'shit happened'

if __name__== "__main__":
    app.run(debug=True)
