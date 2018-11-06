import pickle
import pandas as pd
import re

def text_data_get():
    text_data_df = pd.read_pickle('Data/cru_text_data.pkl')
    text_data_df.sort_values(by = ['time'],ascending = 0,inplace = True)
    latestdate = text_data_df.time.tolist()[0]
    earliestdate = text_data_df.time.tolist()[0]-4
    i = 0
    for idx,row in text_data_df.iterrows():
        if row['time']>=earliestdate:
            i += 1
    text_data_df = text_data_df[0:i].reset_index() \
                   .drop(['index'],axis = 1)
    return text_data_df

def sentence_split(data_df):
    sence_list = []
    for idx in data_df.index:
        sen = data_df.ix[idx,'question'].replace(' ','')
        stock_code = data_df.ix[idx,'stock_code']
        stock_name = data_df.ix[idx, 'stock_name']
        str_list = re.split('。|？|！|\?|\!|；',sen)
        while '' in str_list:
            str_list.remove('')
        for str in str_list:
            if len(str)<=3:
                continue
            itemrow = []
            itemrow.append(stock_code)
            itemrow.append(stock_name)
            itemrow.append(str)
            sence_list.append(itemrow)
    sence_df = pd.DataFrame(sence_list,columns = ['stock_name','stock_code','question'])
    output = open('Data/sence_df.pkl','wb')
    pickle.dump(sence_df,output)
    return sence_df


def keywordsInSql():
    text_data_df = text_data_get()
    # sence_df = sentence_split(text_data_df)
    keywordsList = text_data_df.keywords.tolist()
    keywords_lst = []
    for keywords in keywordsList:
        list = keywords.split('|')
        keywords_lst.extend(list)
    while '' in keywords_lst:
        remove('')
    output = open('Data/sql_keywords_list','wb')
    pickle.dump(keywords_lst,output)

    item_lst = []
    columns_list = []
    for idx,row in text_data_df.iterrows():
        ques = row['question']
        row_com_lst = []
        for keyword in keywords_lst:
            if len(keyword)>=4:
                columns_list.append(keyword)
                if re.search(re.compile(keyword),ques):
                    row_com_lst.append(1)
                else:
                    row_com_lst.append(0)
        item_lst.append(row_com_lst)
    keyword_matrix = pd.DataFrame(item_lst, columns=columns_list)
    output = open('Data/matrix_df.pkl', 'wb')
    pickle.dump(keyword_matrix, output)
    return keyword_matrix











