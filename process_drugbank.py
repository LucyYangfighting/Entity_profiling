#!/usr/bin/env python  
# -*- coding:utf-8 -*-
# coding=utf-8
__author__ = 'yangqingqing'
__time__ = '2018/10/17 下午3:41'

import sqlite3
import logging
from collections import defaultdict
import random
import os
import nltk
from nltk.corpus import stopwords
import sys
from collections import Counter
import codecs
db_path='/Users/yangqingqing/Documents/Entity_Profiling/drugbank.sqlite'

db_path_user='/Users/yangqingqing/Downloads/S/S.sqlite'

logging.basicConfig(format='%(asctime)s : %(levelname)s : %(message)s', level=logging.INFO,
                    filename='process_bank.log',
                    filemode='w')
def remove_type_desc():
    connection = sqlite3.connect(db_path)
    cursor = connection.cursor()
    cursor.execute("select subject_id,predicate_id, object_id from triples_all")
    alltriples = cursor.fetchall()

    cursor.execute("select types_id from types_node")
    value = cursor.fetchall()
    types = [m[0] for m in value]

    object_id_type=[13,19,24,33,26]#宾语语是www.org组织的,26是drugbank.R4，指向数据集grugbankR4的意思

    alltriples_new=[]
    for t in alltriples:
        if t[0] in types or t[2] in object_id_type:
            pass
        else:
            alltriples_new.append(t)
    alltriples_new=list(set(alltriples_new))
    cursor.execute("CREATE TABLE triples_all_new (id INTEGER PRIMARY KEY,subject_id INTEGER,predicate_id INTEGER,object_id INTEGER)")
    cursor.executemany("INSERT INTO triples_all_new(subject_id,predicate_id,object_id) VALUES(?,?,?)", alltriples_new)
    connection.commit()  # 建立node节点和Matrix_A的关系表
    cursor.close()
    connection.close()
def update_nodes_type():
    '''
    剔除掉了nodes_type中的[13, 19, 24, 33]，www.org
    :return:
    '''
    connection = sqlite3.connect(db_path)
    cursor = connection.cursor()
    cursor.execute("select node_id,type_id from nodes_type")
    nodetype = cursor.fetchall()

    cursor.execute("select types_id from types_node")
    value = cursor.fetchall()
    types = [m[0] for m in value]

    object_id_type = [13, 19, 24, 33]
    nodetype_new=[]
    for t in nodetype:
        #去掉头node_id是类型的，去掉是type_id是www的
        if t[1] in object_id_type or t[0] in types:
            pass
        else:
            nodetype_new.append(t)

    cursor.execute("CREATE TABLE nodes_type_new (id INTEGER PRIMARY KEY,node_id INTEGER,type_id INTEGER)")
    cursor.executemany("INSERT INTO nodes_type_new(node_id,type_id) VALUES(?,?)", nodetype_new)
    connection.commit()
    cursor.close()
    connection.close()
def type_test():
    connection = sqlite3.connect(db_path)
    cursor = connection.cursor()
    #cursor.execute("select node_id,type_id from nodes_type_new where node_id in (select node_id from nodes_type_new where type_id=22)")
    cursor.execute("select node_id,type_id from nodes_type_new")
    value = cursor.fetchall()

    dict_nodes = defaultdict(list)
    for v in value:
        dict_nodes[v[0]].append(v[1])
    write_txt('../data/type_test.txt',dict_nodes)
    c1 =c2 = c3 = c4 = 0
    f = open('../data/type_test.txt', "r")

    lines = f.readlines()
    for line in lines:
        m = line.strip().split(' ')
        y=int(m[0])
        if  y== 1:
            c1 += 1
        if y == 2:
            c2 += 1
        if y == 3:
            c3 += 1
        if y > 3:
            c4 += 1
    f.close()

    print(c1,c2,c3,c4)
def write_txt(name, list):
    with open(name, "w") as f:
        for li in list.keys():
            # for l in list[li]:
            #     s = s + str(l) + ' '
            s=str(len(list[li]))+ '\n'
            f.writelines(s)
    f.close()
def update_node_type2():
    connection = sqlite3.connect(db_path)
    cursor = connection.cursor()
    cursor.execute("select node_id,type_id from nodes_type_new")
    value = cursor.fetchall()
    # nodes=set([m[0] for m in value])

    dict_nodes = defaultdict(list)
    for v in value:
        dict_nodes[v[0]].append(v[1])
    nodetype_new=value

    for li in dict_nodes.keys():

        if len(dict_nodes[li]) == 2 and (22 in dict_nodes[li]):
                print((li, 22))
                nodetype_new.remove((li, 22))

    cursor.execute("CREATE TABLE nodes_type_new1 (id INTEGER PRIMARY KEY,node_id INTEGER,type_id INTEGER)")
    cursor.executemany("INSERT INTO nodes_type_new1(node_id,type_id) VALUES(?,?)", nodetype_new)
    connection.commit()
    cursor.close()
    connection.close()
def build_relation_triples_table():
    connection = sqlite3.connect(db_path)
    cursor = connection.cursor()
    cursor.execute("SELECT subject_id, predicate_id, object_id FROM triples_all_new")
    triples = cursor.fetchall()

    cursor.execute("select node_id,type_id from nodes_type_new1")
    node_type= cursor.fetchall()
    dict_nodes = defaultdict(list)
    for v in node_type:
        dict_nodes[v[0]].append(v[1])

    res=[]
    for t in triples:
        if len(dict_nodes[t[0]])==0:
            continue
        if len(dict_nodes[t[2]])==0:
            continue
        subject_id_type=dict_nodes[t[0]]
        object_id_type=dict_nodes[t[2]]

        for s in subject_id_type:
            for o in object_id_type:
                res.append((t[0],t[1],t[2],s,o))

    cursor.execute("CREATE TABLE relation_triples (id INTEGER PRIMARY KEY,entity1_id INTEGER,predicate_id INTEGER,entity2_id INTEGER,entity1_type_id INTEGER,entity2_type_id INTEGER)")
    cursor.executemany("INSERT INTO relation_triples(entity1_id,predicate_id,entity2_id,entity1_type_id,entity2_type_id) VALUES(?,?,?,?,?)", res)
    connection.commit()
    cursor.close()
    connection.close()
def build_property_triples_table():
    connection = sqlite3.connect(db_path)
    cursor = connection.cursor()
    cursor.execute("SELECT subject_id, predicate_id, object_id FROM triples_all_new WHERE  (SELECT string_type_id FROM mapping WHERE id=triples_all_new.object_id )!=1")
    triples = cursor.fetchall()
    '''
    find entity_type_id
    '''
    cursor.execute("select node_id,type_id from nodes_type_new1")
    node_type= cursor.fetchall()
    dict_nodes = defaultdict(list)
    for v in node_type:
        dict_nodes[v[0]].append(v[1])
    '''
    find object_type_id
    '''
    dict_mapping={}
    cursor.execute("SELECT id,string_type_id FROM mapping")
    value = cursor.fetchall()
    for v in value:
        dict_mapping[v[0]]=v[1]

    res=[]
    for t in triples:
        if len(dict_nodes[t[0]])==0:
            continue
        subject_id_type=dict_nodes[t[0]]
        object_id_type=dict_mapping[t[2]]

        for s in subject_id_type:
                res.append((t[0],t[1],t[2],s,object_id_type))

    cursor.execute("CREATE TABLE property_triples (id INTEGER PRIMARY KEY,entity_id INTEGER,predicate_id INTEGER,object_id INTEGER,entity_type_id INTEGER,object_type_id INTEGER)")
    cursor.executemany("INSERT INTO property_triples(entity_id,predicate_id,object_id,entity_type_id,object_type_id) VALUES(?,?,?,?,?)", res)
    connection.commit()
    cursor.close()
    connection.close()
def add_type_id():
    connection = sqlite3.connect(db_path)
    cursor = connection.cursor()
    dict_mapping = {}
    cursor.execute("SELECT id,content FROM mapping")
    value = cursor.fetchall()
    for v in value:
        dict_mapping[v[1]] = v[0]
    cursor.close()
    connection.close()


    connection = sqlite3.connect(db_path_user)
    cursor = connection.cursor()
    cursor.execute("SELECT entity_type From relation")
    value1 = cursor.fetchall()
    data=[]

    for v in value1:
        data.append((dict_mapping[v[0]],))

    q=[]
    for i in range(len(value1)):
        q.append((str(data[i][0]),str(value1[i][0]),))


    cursor.executemany("UPDATE relation set entity_type_id=? where entity_type=?",q)
    connection.commit()
    cursor.close()
    connection.close()
def MergeTxt(filepath,outfile):

    k = open(filepath+outfile, 'a+')
    for parent, dirnames, filenames in os.walk(filepath):
        for filepath in filenames:
            txtPath = os.path.join(parent, filepath)  # txtpath就是所有文件夹的路径
            f = open(txtPath)
            lines=f.read()
            ##########换行写入##################
            k.write(lines+"\n")

    k.close()
def evaluation_type_labels():
    f=open('/Users/yangqingqing/Desktop/resortlabels_user.csv')
    num_type=[]
    lines=f.readlines()
    print(len(lines))
    # for line in lines:
    #     m=line.strip().split(',')
    #     num_type.append(m[0])
    f.close()

    # q=set(num_type)
    # print(q)
    # print(len(q))
def evaluation_output():
    connection = sqlite3.connect(db_path_user)
    cursor = connection.cursor()
    cursor.execute("SELECT * From evaluation")
    value = cursor.fetchall()

    write_txt('/Users/yangqingqing/Desktop/evaluation.txt',value)
def write_txt(file_path, list):
    with open(file_path, "w") as f:
        for li in list:
            s = ''
            for l in li:
                s = s + l + '\t'
            s += '\n'
            f.writelines(s)
    f.close()
def evaluation_support():
    connection = sqlite3.connect('/Users/yangqingqing/Downloads/S/S.sqlite')
    cursor = connection.cursor()
    dict_mapping = {}
    cursor.execute("SELECT id,content FROM mapping")
    value = cursor.fetchall()
    for v in value:
        dict_mapping[v[0]] = v[1]

    after_support = []
    dict_support={}

    cursor.execute("SELECT type_id,property_id,property_value,support_property_value From filter_pro_string_sup")
    value = cursor.fetchall()
    for v in value:
        after_support.append((dict_mapping[v[0]], dict_mapping[v[1]],dict_mapping[v[2]],))
        dict_support[(dict_mapping[v[0]], dict_mapping[v[1]],dict_mapping[v[2]],)]=v[3]

    cursor.execute("SELECT type_id,property_id,property_value_range,pro_value_range_support From filter_pro_numerical_sup")
    value = cursor.fetchall()
    for v in value:
        after_support.append((dict_mapping[v[0]], dict_mapping[v[1]],v[2],))
        dict_support[(dict_mapping[v[0]], dict_mapping[v[1]],v[2],)]=v[3]
    print(len(after_support))

    tag_lib= []
    cursor.execute("SELECT entity_type,property,property_value_range From tag_numerical")
    value = cursor.fetchall()
    for v in value:
        tag_lib.append((v[0], v[1],v[2],))
    cursor.execute("SELECT entity_type,property,property_value From tag_string")
    value = cursor.fetchall()
    for v in value:
        tag_lib.append((v[0], v[1],v[2],))
    print(len(tag_lib))

    remove_after_support = list(set(after_support).difference(set(tag_lib)))
    print(len(remove_after_support))

    remove_after_support_new = random.sample(remove_after_support, len(tag_lib))

    print(len(remove_after_support_new))
    result=[]
    for l in remove_after_support_new:
        l_temp=list(l)
        l_temp.append(dict_support[l])
        result.append(l_temp)
    cursor.close()
    connection.close()

    with open('/Users/yangqingqing/Desktop/re/evaluation_remove_tag_S.txt', "w") as f:
        for li in result:
            s = ''
            for l in li:
                s = s + str(l) + '\t'
            s += '\n'
            f.writelines(s)
    f.close()
def is_number(s):
    try:
        float(s)
        return True
    except ValueError:
        pass

    try:
        import unicodedata
        unicodedata.numeric(s)
        return True
    except (TypeError, ValueError):
        pass

    return False
def hhhh():
    f = open('/Users/yangqingqing/Desktop/user_evalation/P_nodes_tags.txt', 'r')
    lines=f.readlines()
    with open('/Users/yangqingqing/Desktop/user_evalation/P_nodes_tags_new.txt', 'w') as w:
     for line in lines:
        m=line.strip().split('\t')
        if is_number(m[0]):
            w.write(line)
    w.close()
def evaluation_entity():
    f = open('/Users/yangqingqing/Desktop/user_evalation/P_nodes_tags_new.txt', 'r')
    lines = f.readlines()

    dict_entity=defaultdict(list)
    result=defaultdict(list)

    for l in range(len(lines)):
        q=lines[l].strip().split('\t')
        dict_entity[q[1]].append(l)

    for k in dict_entity.keys():
        if len(dict_entity[k])>10:
            result[k]=random.sample(dict_entity[k], 10)
        else:
            result[k]=dict_entity[k]
    print(result)
    with open('/Users/yangqingqing/Desktop/user_evalation/P_evaluation_entity.txt', 'w') as w:
     for k in result.keys():
             for i in result[k]:
                w.write(lines[int(i)])
    w.close()
def evaluation_tag_numerical():
    connection = sqlite3.connect(db_path_user)
    cursor = connection.cursor()
    dict_mapping = {}
    cursor.execute("SELECT id,content FROM mapping")
    value = cursor.fetchall()
    for v in value:
        dict_mapping[v[0]] = v[1]

    cursor.execute("SELECT type_id,entity_num FROM property_mid_support")
    value = cursor.fetchall()
    dict_type_num={}
    for v in value:
        dict_type_num[dict_mapping[v[0]]]=v[1]

    cursor.execute("SELECT property_id,property_value_range,pro_value_range_support From filter_pro_numerical_sup")

    value = cursor.fetchall()
    dict_support={}
    for v in value:

        dict_support[(dict_mapping[v[0]], v[1],)] = v[2]

    cursor.execute("SELECT id,entity_type,property,property_value_range From tag_numerical")

    d1 = cursor.fetchall()

    q = []
    for i in range(len(d1)):
        q.append((str(dict_support[(d1[i][2],d1[i][3],)]), str(dict_type_num[d1[i][1]]),str(d1[i][0]),))
    cursor.executemany("UPDATE tag_numerical set support=? , num_entity=? where id=?", q)
    connection.commit()
    cursor.close()
    connection.close()
def evaluation_tag_string():
    connection = sqlite3.connect(db_path_user)
    cursor = connection.cursor()
    dict_mapping = {}
    cursor.execute("SELECT id,content FROM mapping")
    value = cursor.fetchall()
    for v in value:
        dict_mapping[v[0]] = v[1]

    cursor.execute("SELECT type_id,entity_num FROM property_mid_support")
    value = cursor.fetchall()
    dict_type_num={}
    for v in value:
        dict_type_num[dict_mapping[v[0]]]=v[1]


    cursor.execute("SELECT type_id,property_id,property_value,support_property_value From filter_pro_string_sup")
    value = cursor.fetchall()
    dict_support={}
    for v in value:
        dict_support[(dict_mapping[v[0]],dict_mapping[v[1]], dict_mapping[v[2]],)] = v[3]


    cursor.execute("SELECT id,entity_type,property,property_value From tag_string")
    d1 = cursor.fetchall()
    q = []
    for i in range(len(d1)):
        q.append((str(dict_support[(d1[i][1],d1[i][2],d1[i][3],)]), str(dict_type_num[d1[i][1]]),str(d1[i][0]),))

    cursor.executemany("UPDATE tag_string set support=? , num_entity=? where id=?", q)
    connection.commit()
    cursor.close()
    connection.close()
def evaluation_tag_relation():
    connection = sqlite3.connect(db_path_user)
    cursor = connection.cursor()
    dict_mapping = {}
    cursor.execute("SELECT id,content FROM mapping")
    value = cursor.fetchall()
    for v in value:
        dict_mapping[v[0]] = v[1]

    cursor.execute("SELECT type_id,entity_num FROM property_mid_support")
    value = cursor.fetchall()
    dict_type_num={}
    for v in value:
        dict_type_num[dict_mapping[v[0]]]=v[1]

    cursor.execute("SELECT predicate_id,entity2_id,entity2_salience From salience")

    value = cursor.fetchall()
    dict_support={}
    for v in value:

        dict_support[(dict_mapping[v[0]], dict_mapping[v[1]],)] = v[2]

    cursor.execute("SELECT id,entity_type,predicate_id,object From tag_relation")

    d1 = cursor.fetchall()
    q = []
    for i in range(len(d1)):
        q.append((str(dict_support[(d1[i][2],d1[i][3],)]), str(dict_type_num[d1[i][1]]),str(d1[i][0]),))
    cursor.executemany("UPDATE tag_relation set support=? , num_entity=? where id=?", q)
    connection.commit()
    cursor.close()
    connection.close()
def evaluation_tag_r_n():
    connection = sqlite3.connect(db_path_user)
    cursor = connection.cursor()
    dict_mapping = {}
    cursor.execute("SELECT id,content FROM mapping")
    value = cursor.fetchall()
    for v in value:
        dict_mapping[v[0]] = v[1]

    cursor.execute("SELECT type_id,entity_num FROM property_mid_support")
    value = cursor.fetchall()
    dict_type_num={}
    for v in value:
        dict_type_num[dict_mapping[v[0]]]=v[1]


    cursor.execute("SELECT property_id,property_value_range,pro_value_range_support From filter_pro_numerical_sup")
    value = cursor.fetchall()
    dict_support={}
    for v in value:

        dict_support[(dict_mapping[v[0]], v[1],)] = v[2]

    cursor.execute("SELECT id,entity_type,property,property_value_range From tag_relation_property_numerical")

    d1 = cursor.fetchall()
    q = []
    for i in range(len(d1)):
        q.append((str(dict_support[(d1[i][2],d1[i][3],)]), str(dict_type_num[d1[i][1]]),str(d1[i][0]),))

    cursor.executemany("UPDATE tag_relation_property_numerical set support=? , num_entity=? where id=?", q)
    connection.commit()
    cursor.close()
    connection.close()
def evaluation_tag_r_s():
    connection = sqlite3.connect(db_path_user)
    cursor = connection.cursor()
    dict_mapping = {}
    cursor.execute("SELECT id,content FROM mapping")
    value = cursor.fetchall()
    for v in value:
        dict_mapping[v[0]] = v[1]

    cursor.execute("SELECT type_id,entity_num FROM property_mid_support")
    value = cursor.fetchall()
    dict_type_num = {}
    for v in value:
        dict_type_num[dict_mapping[v[0]]] = v[1]

    cursor.execute("SELECT property_id,property_value,support_property_value FROM filter_pro_string_sup")
    value = cursor.fetchall()
    dict_support = {}
    for v in value:
        dict_support[(dict_mapping[v[0]], dict_mapping[v[1]],)] = v[2]

    cursor.execute("SELECT id,entity_type,property,property_value From tag_relation_property_string")
    d1 = cursor.fetchall()
    q = []
    for i in range(len(d1)):
        q.append((str(dict_support[(d1[i][2], d1[i][3],)]), str(dict_type_num[d1[i][1]]), str(d1[i][0]),))

    cursor.executemany("UPDATE tag_relation_property_string set support=? , num_entity=? where id=?", q)
    connection.commit()
    cursor.close()
    connection.close()
def transform_sqlite():
    connection = sqlite3.connect(db_path)
    cursor = connection.cursor()

    entity_profiling=[]
    f = open('/Users/yangqingqing/Desktop/论文/user_evalation/nodes_tags_new.txt', 'r')
    lines = f.readlines()
    for line in lines:
        l=line.strip().split('\t')
        m=''
        for label in l[3:]:
            m=m+label+'\t'
        entity_profiling.append((l[1],l[2],m))

    print(entity_profiling[1])

    cursor.execute("CREATE TABLE entity_labels (id INTEGER PRIMARY KEY,entitytype TEXT,entity TEXT,labels TEXT)")
    cursor.executemany("INSERT INTO entity_labels(entitytype,entity,labels) VALUES(?,?,?)",
                       entity_profiling)
    connection.commit()  # 建立node节点和Matrix_A的关系表
    cursor.close()
    connection.close()
def find_labelset():
    connection = sqlite3.connect("/Users/yangqingqing/Desktop/H_P/HSP_ALL.sqlite")
    cursor = connection.cursor()
    cursor.execute("select * FROM label_set")
    value=cursor.fetchall();
    trans=[]
    for v in value:
        a1=a2=a3=""
        if v[1] is not None:
            a1 = v[1].split('/')[-1]
        if v[2] is not None:
            a2= v[2].split('/')[-1]
        if v[3] is not None:
            a3 = v[3].split('/')[-1]
        trans.append((v[0],a1,a2,a3,v[4],v[5]))


    res=[]
    for v in trans:
        if v[0]==1 or v[0]==2 :
            res.append((v[0],v[1],v[3]+"=="+v[4],v[5]))
        if v[0] == 3 :
            res.append((v[0],v[1], v[2] + "==" + v[4],v[5]))
        if v[0]==4 or v[0]==5 :
            res.append((v[0],v[1], v[2] +"---"+v[3]+"==" + v[4],v[5]))


    cursor.execute("CREATE TABLE LabelSet (id INTEGER PRIMARY KEY,class INTEGER, entitytype TEXT,label TEXT,rank DOUBLE)")
    cursor.executemany("INSERT INTO LabelSet(class,entitytype,label,rank) VALUES(?,?,?,?)",
                       res)
    connection.commit()
    cursor.close()
    connection.close()

def spiltwords(text):
    disease_List = nltk.word_tokenize(text)
    tagged = nltk.pos_tag(disease_List)
    res = []
    english_stopwords = stopwords.words("english")

    dict_mapping={}
    for t in tagged:
        dict_mapping[t[0]]=t[1]

    english_punctuations = [',', '.', ':', ';', '?', '(', ')', '[', ']', '!', '@', '#', '%', '$', '*','en','``','\'\'','drugbank','drugbank_resource','resource','-','drugbank_vocabulary']  # 自定义英文表单符号列表

    wordclass=['IN','CC','VB','DT','WP','VBD','VBG','VBN','VBP','VBZ','WRB','WP','WDT','UH','SYM','RBS','RBR','RB','PRP$','PRP','PDT','MD','LS','EX']
    for i in disease_List:
        if dict_mapping[i] not in wordclass:
          if i.lower() not in english_stopwords:  # 过滤停用词
            if i not in english_punctuations:  # 过滤标点符号
              if not i.replace('.', '', 1).isdigit():
                       res.append(i)
    return res

def keywords():
    keywords_res=dict()
    connection = sqlite3.connect("/Users/yangqingqing/Desktop/H_P/HSP_ALL.sqlite")
    cursor = connection.cursor()
    cursor.execute("select DISTINCT(node_id) FROM nodes_type")
    value=cursor.fetchall()
    nodes=[v[0] for v in value]

    dict_mapping = {}
    cursor.execute("SELECT id,content FROM mapping")
    value = cursor.fetchall()
    for v in value:
        dict_mapping[v[0]] = v[1]

    triples = defaultdict(list)
    cursor.execute("select subject_id,predicate_id,object_id FROM triples_all where predicate_id=30 or predicate_id=10")
    value=cursor.fetchall()
    for v in value:
        triples[v[0]].append((v[1],v[2],))
    for node in nodes:
        nodecontent=dict_mapping[node]
        res=[]
        for po in triples[node]:
              res=res+spiltwords(dict_mapping[po[1]])
              if po[0]==10:
                res.append(dict_mapping[po[1]])
        res.append(nodecontent.split(':')[-1])
        keywords_res[nodecontent]=list(set(res))

    write_dict("../data/entity_keywords.txt",keywords_res)

def write_dict(file_path, dict):
    with open(file_path, "a") as f:
        for k in dict.keys():
            s = str(k)
            if dict[k]:
                for i in dict[k]:
                    s = s + '|' + str(i)
            s = s + '\n'
            f.writelines(s)
    f.close()


def keywords_sqlite():
    #connection = sqlite3.connect('/Users/yangqingqing/Desktop/H_P/HSP_ALL.sqlite')
    connection = sqlite3.connect(db_path)
    cursor = connection.cursor()

    entity_profiling = []
    f = open('../data/entity_keywords.txt', 'r')
    lines = f.readlines()
    count=1
    for line in lines:
        l = line.strip().split('|')
        m = ''
        for label in l[1:]:
            m = m + label + '|'
        entity_profiling.append((count,count,l[0], m))
        count=count+1

    cursor.execute("CREATE VIRTUAL TABLE entity_keywords USING fts4(id INTEGER PRIMARY KEY,entity TEXT,keywords TEXT);")

    #cursor.execute("CREATE TABLE entity_keywords (id INTEGER PRIMARY KEY,entitytype TEXT,entity TEXT,labels TEXT)")
    cursor.executemany("INSERT INTO entity_keywords(docid,id,entity,keywords) VALUES(?,?,?,?)",
                       entity_profiling)
    connection.commit()  # 建立node节点和Matrix_A的关系表
    cursor.close()
    connection.close()

def testDBpedia():
    connection = sqlite3.connect('/Users/yangqingqing/Desktop/dataset-rdf/dbpedia_en.sqlite')
    cursor = connection.cursor()

    cursor.execute("SELECT node_id, type_id from nodes_type")
    value=cursor.fetchall()
    dicttype=defaultdict(list)

    res=[]

    for v in value:
        dicttype[v[0]].append(v[1])

    for k in dicttype.keys():
        if len(dicttype[k])==1:
            if dicttype[k][0]!=3:
               res.append((k,dicttype[k][0]))

    cursor.execute("CREATE TABLE node_type_new (id INTEGER PRIMARY KEY,node_id INTEGER,type_id INTEGER)")
    cursor.executemany("INSERT INTO node_type_new(node_id,type_id) VALUES(?,?)",res
                       )
    connection.commit()  # 建立node节点和Matrix_A的关系表
    cursor.close()
    connection.close()




if __name__ == '__main__':
    #remove_type_desc()
    #update_nodes_type()
    #type_test()
    #update_node_type2()
    #build_relation_triples_table()
    #build_property_triples_table()
    #add_type_id()
    #MergeTxt('/Users/yangqingqing/Desktop/re/','a_r_evaluation.csv')
    #evaluation_type_labels()
    #evaluation_support()
    # evaluation_tag_numerical()
    # evaluation_tag_string()
    # evaluation_tag_relation()
    # evaluation_tag_r_n()
    # evaluation_tag_r_s()
    # hhhh()
    # evaluation_entity()
    #transform_sqlite()
    #find_labelset()
    #keywords()
    #keywords_sqlite()
    testDBpedia()




