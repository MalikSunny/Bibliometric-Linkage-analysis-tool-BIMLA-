import pandas as pd
import sqlite3
from my_functions import touch

data=pd.read_csv('plastic_3088_after_preprocessing.csv')
df= pd.DataFrame(data)

touch('record.db')
conn = sqlite3.connect('record.db')
c = conn.cursor()
 

c.execute('''CREATE TABLE record (record_id INTEGER PRIMARY KEY AUTOINCREMENT,
             'Authors' text, 'Author(s) ID' text, 'Title' text, 'Year' int, 'Source title' text, 'Volume' text,
       'Issue' text, 'Art. No.' text, 'Page start' text, 'Page end' text, 'Page count' text, 'Cited by' text,
       'DOI' text, 'Link' text, 'Affiliations' text, 'Authors with affiliations' text, 'Abstract' text,
       'Author Keywords' text, 'Index Keywords' text, 'Molecular Sequence Numbers' text,
       'Chemicals/CAS' text, 'Tradenames' text, 'Manufacturers' text, 'Funding Details' text,
       'Funding Text 1' text, 'Funding Text 2' text, 'Funding Text 3' text, 'Funding Text 4' text,
       'Funding Text 5' text, 'Funding Text 6' text, 'Funding Text 7' text, 'Funding Text 8' text,
       'Funding Text 9' text, 'Funding Text 10' text, 'References' text,
       'Correspondence Address' text, 'Editors' text, 'Sponsors' text, 'Publisher' text,
       'Conference name' text, 'Conference date' text, 'Conference location' text,
       'Conference code' text, 'ISSN' text, 'ISBN' text, 'CODEN' text, 'PubMed ID' text,
       'Language of Original Document' text, 'Abbreviated Source Title' text,
       'Document Type' text, 'Publication Stage' text, 'Open Access' text, 'Source' text, 'EID' text)''')


df.to_sql('record', conn, if_exists='append', index = False)

print(len(c.execute('''SELECT * FROM record''').fetchall())) 

