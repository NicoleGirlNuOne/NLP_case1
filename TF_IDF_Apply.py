import pandas as pd
import pickle
import jieba
import jieba.analyse
import re

jieba.load_userdict('Data/userdict.txt') #加载自己定义的词典
#创建停用词列表
def stop_list(filepath):
    stoplist = [line.strip() for line in open(filepath,'rb').readlines()]
    return stoplist

#获得关键词列表
def keywords_get(text_df):
    ques_text_lst = []
    stopwords = stop_list('Data/stopwords.txt')
    text_list = text_df.question.tolist()
    for sen in text_list:
        out_str = ''
        sen_lst = list(jieba.cut(sen))
        no_list = ['2017','2018','股东人数','上市公司','公司股价']
        for s in sen_lst:
            if s not in stopwords and len(s)>3 and s not in no_list:
                out_str += s
                out_str += ' '
        ques_text_lst.append(out_str)
    output = open('Data/ques_text_lst.pkl','wb')
    pickle.dump(ques_text_lst,output)
    keywords_lst = []
    for content in ques_text_lst:
        #注意：这个包有自己的超参，后面可能根据场景需要我们自己的超参
        keywords = jieba.analyse.extract_tags(content,topK = 2,withWeight = False,allowPOS = ())
        keywords_lst.extend(keywords)
    output= open('Data/keywords.pkl','wb')
    pickle.dump(keywords_lst,output)
    return keywords_lst

#形成关键词特征矩阵
def keyword_matrix(text_df,keywords):
    ques_lst = pd.read_pickle('Data/ques_text_lst.pkl')
    ques_ser = pd.DataFrame(ques_lst,columns=['ques_spl'])
    text_df_c = pd.concat([text_df,ques_ser],axis = 1)
    item_lst = []
    for idx,row in text_df_c.iterrows():
        ques = row['question']
        stock_code = row['stock_code']
        stock_name = row['stock_name']
        ques_spl = row['ques_spl']
        row_com_lst = []
        row_key_lst = [1.0 if re.search(re.compile(key),ques) else 0.0 for key in keywords]
        row_com_lst.append(ques)
        row_com_lst.append(stock_code)
        row_com_lst.append(stock_name)
        row_com_lst.extend(row_key_lst)
        item_lst.append(row_com_lst)
    columns_list =['question','stock_code', 'stock_name']
    columns_list.extend(keywords)
    keyword_matrix = pd.DataFrame(item_lst,columns = columns_list)
    output = open('Data/matrix_df.pkl','wb')
    pickle.dump(keyword_matrix,output)
    return keyword_matrix

def get_matrix_one():
    text_df = pd.read_pickle('Data/sence_df.pkl')
    keywords = keywords_get(text_df)
    # keywords = pd.read_pickle('Data/keywords.pkl')
    # keywords = keywords[:5]
    matrix_df = keyword_matrix(text_df,keywords)
    # matrix_df = pd.read_pickle('Data/matrix_df.pkl')
    return matrix_df

#获取申万二级行业列表,'indus_code','indus_name','stock_code','stock_name'
def get_matrix_two():
    indus_df = pd.read_pickle('Data/cru_indus_data.pkl')
    indus_new_df = indus_df.loc[:,['indus_name','stock_code','stock_name']] \
                   .assign(flag = 1) \
                   .pivot_table(index = ['stock_code','stock_name'],columns = 'indus_name',values = 'flag') \
                   .reset_index() \
                   .fillna(0)
    return indus_new_df

#获取概念列表，'concep_code','concep_name','stock_code','stock_name'
def get_matrix_three():
    concep_df = pd.read_pickle('Data/cru_concept_data.pkl')
    concep_new_df = concep_df.loc[:,['concep_name','stock_code','stock_name']] \
                    .assign(flag = 1) \
                    .pivot_table(index = ['stock_code','stock_name'],columns = 'concep_name',values = 'flag') \
                    .reset_index() \
                    .fillna(0)
    return concep_new_df

#分块得到 股票-问题 ，股票-行业 和 股票-概念 矩阵，然后按照 股票(包括名称和代码)进行合并
#然后重新整理表结构，使股票作为列
def matrix_merge():
    stock_ques = get_matrix_one()
    print(stock_ques.head())
    # stock_indus = get_matrix_two()
    # stock_concept = get_matrix_three()
    # matrix_mer_first = pd.merge(stock_ques,stock_indus,on=['stock_code','stock_name'])
    # matrix_mer_second = pd.merge(matrix_mer_first,stock_concept,on=['stock_code','stock_name'])
    # matrix_mer_second.dropna(inplace=True)
    # matrix_four = matrix_mer_second.assign(flag_two=1) \
    #             .pivot_table(index = ['question'],columns = 'stock_name',values = 'flag_two') \
    #             .reset_index() \
    #             .fillna(0)
    # matrix_mer_final = pd.merge(matrix_mer_second,matrix_four,on=['question'],how = 'left')
    # matrix_mer_final.dropna(inplace=True)
    # matrix_mer_final = matrix_mer_final.ix[:,3:]
    output = open('Data/matrix_mer_final.pkl','wb')
    # pickle.dump(matrix_mer_final,output)
    pickle.dump(stock_ques, output)
    # return matrix_mer_final
    return stock_ques

data = matrix_merge()
print(data.head())


























