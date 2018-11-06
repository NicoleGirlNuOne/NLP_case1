import pymysql
import pymssql
import pandas as pd
import pickle
import re

db_text_conf = {
    'host': '172.16.20.158',
    'port': 3308,
    'user': 'JRJ_ztext',
    'passwd': 'CQEt3lus3V7q920LsECw',
    'db': 'ztext',
    'charset': 'utf8mb4'
}

db_pgGen_conf = {
   'server': '172.16.198.11',
   'user': 'JRJ_PG',
   'password': '47sTXSLIQiXPG9pY2SYh',
   'database': 'PGenius',
   'charset': 'cp936', #这里不是utf-8
}
def mysql_data_get(mysql,db_conf):
    db = pymysql.connect(**db_conf)
    db_cur = db.cursor()
    db_cur.execute(mysql)
    resList = db_cur.fetchall()
    db_cur.close()
    return resList

def sql_data_get(sql,db_conf):
    db = pymssql.connect(**db_conf)
    db_cur = db.cursor()
    db_cur.execute(sql)
    resList = db_cur.fetchall()
    db_cur.close()
    return resList


mysql_ques_stock = """
SELECT stock_name, question, ctime as time,keywords
FROM sec_board_questions
"""
sql_indus_stock = """
SELECT ir.indu_code as section_code, s.section_name, st.stockcode as stock_code, st.stocksname as stock_name
FROM PGenius.dbo.pub_section_code s 
JOIN PGenius.dbo.pub_indu_ref ir on ir.INNER_CODE = s.section_code 
JOIN PGenius.dbo.pub_section_rel r on r.section_code = s.section_code
JOIN PGenius.dbo.stk_code st on st.inner_code = r.inner_code 
WHERE s.sys_code = 17 and s.section_level = 4
"""
sql_concept_stock = """
SELECT s.section_code, s.section_name, st.stockcode as stock_code, st.stocksname as stock_name
FROM PGenius.dbo.pub_section_code s 
JOIN PGenius.dbo.pub_section_rel r on r.section_code = s.section_code
JOIN PGenius.dbo.stk_code st on st.inner_code = r.inner_code 
WHERE s.sys_code = 5 and s.section_level in (3, 4)
"""

def text_data_get():
    text_data_list = list(mysql_data_get(mysql_ques_stock,db_text_conf)) #查询后返回的是touple类型，
    text_data = pd.DataFrame(text_data_list,columns = ['stock','question','time','keywords'])
    stock_list = text_data.stock.tolist()
    name_code_lst = []
    for stock in stock_list:
        lst = re.split('\(|\)',stock)
        while '' in lst:
            lst.remove('')
        name_code_lst.append(lst)
    name_code_df = pd.DataFrame(name_code_lst,columns = ['stock_code','stock_name','empty']) #不知道为什么这里还会多一列空值
    name_code_df.drop('empty',axis = 1,inplace = True)
    text_data = text_data.ix[:,['question','time','keywords']]
    text_data_df = pd.concat([name_code_df,text_data], axis = 1)
    text_data_df.time = text_data_df.time.apply(lambda s: s.strftime('%Y-%m-%d')) \
        .apply(lambda x: (''.join(x.split('-')))) \
        .astype(int)
    output = open('Data/cru_text_data.pkl','wb')
    pickle.dump(text_data_df,output)
    return text_data_df

def indus_data_get():
    indus_data_list = list(sql_data_get(sql_indus_stock,db_pgGen_conf))
    indus_data_df = pd.DataFrame(indus_data_list,columns = ['indus_code','indus_name','stock_code','stock_name'])
    output = open('Data/cru_indus_data.pkl','wb')
    pickle.dump(indus_data_df,output)
    return indus_data_df

def concept_data_get():
    concept_data_list = list(sql_data_get(sql_concept_stock,db_pgGen_conf))
    concept_data_df = pd.DataFrame(concept_data_list,columns = ['concep_code','concep_name','stock_code','stock_name'])
    output = open('Data/cru_concept_data.pkl','wb')
    pickle.dump(concept_data_df,output)
    return concept_data_df

def data_get():
    text_data_df = text_data_get()
    indus_data_df = indus_data_get()
    concept_data_df = concept_data_get()
    print(text_data_df.head())
    print(indus_data_df.head())
    print(concept_data_df.head())

# text = text_data_get()

# text= pd.read_pickle('Data/cru_text_data.pkl')
# def word_num(sen):
#     i = 0
#     for idx ,row in text.iterrows():
#         question = row['question']
#         s = re.compile(sen)
#         if re.search(s,question):
#             i += 1
#     return i
# word_list = ['2017','2018','股东人数','上市公司','公司股价','基金重仓股','蓝丰生化','预盈预增','IPO']
#
# for key in word_list:
#     num = word_num(key)
#     print(key)
#     print(num)

text_data = text_data_get()
print(text_data.head())









