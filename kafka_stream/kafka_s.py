# -*- coding: utf-8 -*-
# @Author : wgq
# @time   : 2020/8/10 17:47
# @File   : kafka_s.py
# Software: PyCharm
"""
新三板-基于mongoshake =>> kafka 的流处理
1.读取kafka的数据
2.读取全称表的数据生成date数据
3.将kfka数据中的简称取出 -> 在全称的date匹配对应的全称 -> 将全称保存一个变量
4.利用全称进入行业表中进行关联查询 -> 获取行业信息保存变量
5.将分类表中的信息读出 -> 转成dateframe格式
6.利用kafka中的title信息 -> 和dateframe数据进行匹配 -> 获取到分类信息-情感分值-重要度分值 -> 利用分值匹配名称
                                                   ↓
                                                    →在每一次匹配时-都要检测分类表是否发生变化-发生变化-更新dateframe
7.将以保存的变量整合成需要的列表 -> 进行保存
"""
from kafka import KafkaConsumer
import json
from mysql_yi import mysql_kafka_sq
from kafka.structs import TopicPartition
from bson import BSON
import pandas as pd
from mysql_yi.mysql_pool import PymysqlPool
import uuid
import pymysql
import time, datetime
import pymongo

class Kafka_consumer_s():
    def __init__(self):
        """
        self.consumer：kafka-连接对象
        self.page：kafka-消费偏移量
        self.emo_label：情感分值-标签
        self.imp_label：重要程度分值-标签
        self.full_name_date：全称表的数据
        self.df：分类表的数据
        self.companyName：全称
        self.mysql_full_name：行业数据
        self.title：标题
        self.srcUrl：网源链接
        self.pubTime：发布时间
        self.cmpCode：简称代码
        self.yqid：唯一标识
        self.srcType：数据类型
        self.webname：来源
        self.cmpShortName：简称
        self.emoConf：置信度 A股信息没有
        self.firstLevelCode：一级分类编码
        self.firstLevelName：一级分类名称
        self.secondLevelCode：二级分类编码
        self.secondLevelName：二级分类名称
        self.threeLevelCode：三级分类编码
        self.threeLevelName：三级分类名称
        self.fourLevelCode：四级分类编码
        self.fourLevelName：司机分类名称
        self.eventCode：舆情事件分类编码
        self.eventName：舆情事件分类名称
        self.emoScore：情感分值
        self.emolabel：情感标签
        self.impScore：重要程度分值
        self.impLabel：重要程度标签
        self.comp_info：行业分级字典
        self.onlyId：mongo主键
        self.mydict : 储存的mongo数据
        """
        self.consumer = KafkaConsumer("xin_san_ban_add",bootstrap_servers = ["192.168.1.172:9092"],auto_offset_reset='earliest')
        self.myclient = pymongo.MongoClient(
            "mongodb://root:shiye1805A@192.168.1.125:10011,192.168.1.126:10011,192.168.1.127:10011/admin")
        self.page = 0
        self.emo_label = {'1': '正向', '-1': '负向', '0': '中性'}
        self.imp_label = {'1': '相对不重要', '2': '相对不重要', '3': '相对不重要', '4': '重要', '5': '非常重要'}
        self.full_name_date = {}
        self.df = ""
        self.companyName = ""
        self.mysql_full_name = ()
        self.title = ""
        self.srcUrl = ""
        self.pubTime = ""
        self.cmpCode = ""
        self.yqid = ""
        self.srcType = ""
        self.webname = ""
        self.cmpShortName = ""
        self.emoConf = ""
        self.firstLevelCode = ""
        self.secondLevelCode = ""
        self.secondLevelName = ""
        self.threeLevelCode = ""
        self.threeLevelName = ""
        self.fourLevelCode = ""
        self.fourLevelName = ""
        self.eventCode = ""
        self.eventName = ""
        self.emoScore = ""
        self.emolabel = ""
        self.impScore = ""
        self.impLabel = ""
        self.comp_info = {}
        self.onlyId = ""
        self.mydict = {}
    def mysql_client(self):
        return PymysqlPool('129')
    def mysql_l(self):
        """
        连接行业表获取行业数据
        :param cmpShortName:
        :return:
        """
        if self.companyName:
            conn = PymysqlPool('industry')
            sql = "SELECT A.compName, A.categoryCode, B.constValueDesc, B.constCode FROM(SELECT * FROM seeyii_assets_database.sy_cd_ms_ind_comp_gm WHERE compName = '{}') AS A INNER JOIN ( SELECT * FROM seeyii_assets_database.sy_cd_mt_sys_const WHERE constCode IN ( 3, 4, 5 ) ) AS B ON A.categoryCode = B.cValue".format(
                self.companyName)
            counts, infos = conn.getAll(sql)
            return infos
        else:
            return ""

    def mysq_related_query_z(self):
        """
        连接全称表
        连接A_stock_code_name_fyi获取信息
        :return: 需要的date数据
        """
        if self.full_name_date:
            pass
        else:
            print("第一次构件名称字典")
            conn = self.mysql_client()
            commpany_map = {}
            sql = "select * from EI_BDP.A_stock_code_name_fyi;"
            count, infos = conn.getAll(sql)
            for info in infos:
                commpany_map[info['all_name']] = info['short_name']
            conn.dispose()
            self.full_name_date = commpany_map
    def date_c(self,get_value):
        """
        将全称表做成date
        :param get_value: 数据
        :return: date
        """
        if get_value in self.full_name_date.values():
            self.companyName = list(self.full_name_date.keys())[list(self.full_name_date.values()).index(get_value)]
            print(self.companyName)
        else:
            print(self.companyName)
    def kafka_take_out(self):
        """
        连接kafka获取数据 将数据装换成字符串
        :return:
        """

        for each in self.consumer:
            # try:
                kafa_str = BSON.decode(each.value)
                if kafa_str:
                    self.kafka_data_processing(kafa_str)
            # except:
            #     print(each.value, "错误的数据")

    def kafka_data_processing(self,kafka_json):
        """
        处理数据  比对两个表
        :param kafka_json: 字典
        :return:
        """

        if kafka_json.get("o"):
            kafka_set = kafka_json.get("o")
            if kafka_set.get("$set"):
                pass
            else:
                self.page += 1
                print(self.page,"+++++++++++++++++++++++++++++++++++++++++++++++++")
                kafka_of = kafka_json.get("o")
                cmpShortName = kafka_of.get("st_name")
                cmpCode = kafka_of.get("st_code")
                title = kafka_of.get("title")
                pubTime = kafka_of.get("publish_date").strftime("%Y-%m-%d %H:%M:%S")
                srcUrl = kafka_of.get("url")
                self.title = title
                self.srcUrl = srcUrl
                self.pubTime = pubTime
                self.cmpCode = cmpCode
                self.cmpShortName = cmpShortName
                print(self.cmpShortName)
                "----------------------"
                self.mysq_related_query_z()
                "----------------------"
                self.date_c(cmpShortName)
                "----------------------"
                mysql_full_name = self.mysql_l()
                self.mysql_full_name = mysql_full_name
                "----------------------"
                self.pd_dataframe(title)
    def logs_dateframe(self):
        """
        检测分类表数据是否发生变化
        :return: 状态 False：有变化  True：无变化
        """
        with open("/shiye_kf3/gonggao/kafka_stream/logs/log_s.log","r") as r:
            date_time =  r.read()
        print(date_time)
        conn = self.mysql_client()
        sql = "SELECT count(id) FROM sy_yq_raw.sy_yq_lvl_rules_code WHERE modifyTime >= '{}'".format(date_time)
        count, infos = conn.getAll(sql)
        conn.dispose()
        dateArray = datetime.datetime.fromtimestamp(time.time())
        otherStyleTime = dateArray.strftime("%Y-%m-%d %H:%M:%S")
        page = infos[0]["count(id)"]
        print(page,"每次查询的分类表变化数量")
        with open("/shiye_kf3/gonggao/kafka_stream/logs/log_s.log","w") as w:
            w.write(otherStyleTime)
        if page > 0:
            return False
        else:
            return True
    def pd_dataframe(self,test):
        """
        获取dateframe数据进行处理
        :param test:
        :return:
        """
        if test:
            if len(self.df):
                pass
            else:
                self.pandsa()
                print("第一次进入")
            bool_if = self.logs_dateframe()
            if bool_if:
                pass
                print("分类表数据无变化")
            else:
                self.pandsa()
                print("分类表存在变化--对dateframe进行修改")
            print(self.len_list)
            for i in range(self.len_list):
                inRules_list = [self.df.loc[i,"inRules"]][0]
                filterRules_list = [self.df.loc[i, "filterRules"]][0]
                in_list = [rule.strip() for rule in inRules_list.split('、') if inRules_list]
                in_lists = [rule.split('&') for rule in in_list]
                filter_rules = [[rule.strip()] for rule in filterRules_list.split('、') if filterRules_list]
                if_csv = self.list_if(in_lists,filter_rules,test)
                if if_csv:
                    print("需要存储")
                    self.pands_dateframe_csv(i)
                else:
                    pass
    def list_if(self,in_lists,filter_rules,test):

        """
        对传传入的数据进项判断
        :param in_lists: 符合要求的规则   判断test中的数据是否符合要求
        :param filter_rules: 过滤的规则   判断test中的数据是否不符合要求
        :param test: 标题
        :return:
        """
        is_match = False
        for words in in_lists:
            result = self.pandas_dataframe_if(words, test)
            if result == words:
                is_match = True
                break
        if filter_rules and is_match:
            for fwords in filter_rules:
                filter_result = self.pandas_dataframe_if(fwords, test)
                if filter_result == fwords:
                    is_match = False
                    break
        return is_match
    def pandas_dataframe_if(self,words,test):
        """
        处理已被切割的数据 讲判断结果返回调用方
        :param words:
        :param test:
        :return:
        """
        result = []
        for word in words:
            if word in test:
                result.append(word)
        return result
    def pandsa(self):
        """
        将从分类表中的数据取出 转换成dateframe
        :return:
        """
        conn = self.mysql_client()
        sql = "SELECT id,firstLevelCode,firstLevelName,secondLevelCode,secondLevelName,threeLevelCode,threeLevelName,fourLevelCode,fourLevelName,cfEventCode,eventCode,eventName,inRules,filterRules,emoScore,impScore,isChange,isValid,dataStatus FROM sy_yq_raw.sy_yq_lvl_rules_code WHERE inRules != '' and inRules IS NOT NULL"
        count, infos = conn.getAll(sql)
        conn.dispose()
        self.len_list = len(infos)
        df = pd.DataFrame(data=infos,columns=["id","firstLevelCode","firstLevelName","secondLevelCode","secondLevelName","threeLevelCode","threeLevelName","fourLevelCode","fourLevelName","cfEventCode","eventCode","eventName","inRules","filterRules","emoScore","impScore","isChange","isValid","dataStatus"])
        self.df = df
    def mysql_full_name_data(self):
        self.comp_info = {}
        if self.mysql_full_name:
            for sin in self.mysql_full_name:
                if sin.get("constCode") == 3:
                    self.comp_info['firstIndustry'] = sin.get("constValueDesc")
                    self.comp_info['firstIndustryCode'] = str(sin.get("categoryCode")) + "##" + str(sin.get("constCode"))
                elif sin.get("constCode") == 4:
                    self.comp_info['secondIndustry'] = sin.get("constValueDesc")
                    self.comp_info['secondIndustryCode'] = str(sin.get("categoryCode")) + "##" + str(sin.get("constCode"))
                elif sin.get("constCode") == 5:
                    self.comp_info['threeIndustry'] = sin.get("constValueDesc")
                    self.comp_info['threeIndustryCode'] = str(sin.get("categoryCode")) + "##" + str(sin.get("constCode"))
    def pands_dateframe_csv(self,i):
        """
        将结果保存
        :param i: dateframe数据定位
        :return:
        """
        self.webname = "巨潮资讯网"
        self.srcType = "新三板公告"
        print(self.title)
        if self.title:
            self.mysql_full_name_data()
            self.yqid = self.add_uuid(self.title + self.srcUrl + str(self.pubTime))
            self.firstLevelCode = self.df.loc[i, "firstLevelCode"]
            self.firstLevelName = self.df.loc[i, "firstLevelName"]
            self.secondLevelCode = self.df.loc[i, "secondLevelCode"]
            self.secondLevelName = self.df.loc[i, "secondLevelName"]
            self.threeLevelCode = self.df.loc[i, "threeLevelCode"]
            self.threeLevelName = self.df.loc[i, "threeLevelName"]
            self.fourLevelCode = self.df.loc[i, "fourLevelCode"]
            if self.df.loc[i, "fourLevelName"]:
                self.fourLevelName = self.df.loc[i, "fourLevelName"]
            else:
                self.fourLevelName = ""
            self.eventCode = self.df.loc[i, "eventCode"]
            self.eventName = self.df.loc[i, "eventName"]
            self.emoScore = self.df.loc[i, "emoScore"]
            self.impScore = self.df.loc[i, "impScore"]
            list_g = self.list_mysql_g_gao()
            list_yu = self.list_mysql_u_s()
            print(list_g)
            print("``````````````")
            print(list_yu)
            self.mysql_insert_g_gao(list_g)
            self.mysql_insert_u_yuqing(list_yu)
            self.mongo_insert()
    def list_mysql_g_gao(self):
        """
        将数据组合 组合成公告表需要的结构
        :return:
        """
        list_mysql_g_s = []
        list_mysql_g = []
        list_mysql_g.append(self.yqid)
        list_mysql_g.append(self.title)
        list_mysql_g.append(self.webname)
        list_mysql_g.append(self.companyName)
        list_mysql_g.append(self.cmpShortName)
        list_mysql_g.append(self.cmpCode)
        list_mysql_g.append("")
        list_mysql_g.append("")
        list_mysql_g.append("")
        list_mysql_g.append(self.comp_info.get("firstIndustry",""))
        list_mysql_g.append(self.comp_info.get("firstIndustryCode",""))
        list_mysql_g.append(self.comp_info.get("secondIndustry",""))
        list_mysql_g.append(self.comp_info.get("secondIndustryCode",""))
        list_mysql_g.append(self.comp_info.get("threeIndustry",""))
        list_mysql_g.append(self.comp_info.get("threeIndustryCode",""))
        list_mysql_g.append(self.firstLevelCode)
        list_mysql_g.append(self.firstLevelName)
        list_mysql_g.append(self.secondLevelCode)
        list_mysql_g.append(self.secondLevelName)
        list_mysql_g.append(self.threeLevelCode)
        list_mysql_g.append(self.threeLevelName)
        list_mysql_g.append(self.fourLevelCode)
        list_mysql_g.append(self.fourLevelName)
        list_mysql_g.append(self.eventCode)
        list_mysql_g.append(self.eventName)
        list_mysql_g.append(self.emoScore)
        self.emolabel = self.emoLabel_i()
        list_mysql_g.append(self.emolabel)
        list_mysql_g.append(self.emoConf)
        list_mysql_g.append(self.impScore)
        self.impLabel = self.impLabel_i()
        list_mysql_g.append(self.impLabel)
        list_mysql_g.append(self.srcType)
        list_mysql_g.append(self.srcUrl)
        list_mysql_g.append(self.pubTime)
        list_mysql_g_s.append(list_mysql_g)
        return list_mysql_g_s
    def list_mysql_u_s(self):
        """
        将数据组合成 舆情表需要的结构
        :return:
        """
        transScore = ""
        relPath = ""
        personName = ""
        relType = "直接关联"
        summary = ""
        keyword = ""
        content = ""
        relScore = ""
        relLabel = ""
        list_mysql_u_s = []
        list_mysql_u = []
        list_mysql_u.append(self.yqid)
        list_mysql_u.append(self.companyName)
        list_mysql_u.append(self.cmpShortName)
        list_mysql_u.append(self.cmpCode)
        list_mysql_u.append(self.companyName)
        list_mysql_u.append(transScore)
        list_mysql_u.append(relPath)
        list_mysql_u.append(relType)
        list_mysql_u.append(relScore)
        list_mysql_u.append(relLabel)
        list_mysql_u.append(personName)
        list_mysql_u.append(self.eventCode)
        list_mysql_u.append(self.eventName)
        list_mysql_u.append(self.firstLevelCode)
        list_mysql_u.append(self.firstLevelName)
        list_mysql_u.append(self.secondLevelCode)
        list_mysql_u.append(self.secondLevelName)
        list_mysql_u.append(self.threeLevelCode)
        list_mysql_u.append(self.threeLevelName)
        list_mysql_u.append(self.fourLevelCode)
        list_mysql_u.append(self.fourLevelName)
        self.emolabel = self.emoLabel_i()
        list_mysql_u.append(self.emolabel)
        list_mysql_u.append(self.emoScore)
        list_mysql_u.append(self.emoConf)
        list_mysql_u.append(self.impScore)
        self.impLabel = self.impLabel_i()
        list_mysql_u.append(self.impLabel)
        list_mysql_u.append(self.pubTime)
        list_mysql_u.append(self.title)
        list_mysql_u.append(summary)
        list_mysql_u.append(keyword)
        list_mysql_u.append(self.srcUrl)
        list_mysql_u.append(self.srcType)
        list_mysql_u.append(self.webname)
        list_mysql_u.append(content)
        list_mysql_u_s.append(list_mysql_u)
        return list_mysql_u_s
    def impLabel_i(self):
        """
        重要度处理
        :param impScore: 重要度分值
        :return: 重要度标签
        """
        imp_label = self.imp_label.get(str(self.impScore))
        return imp_label
    def emoLabel_i(self):
        """
        情感处理
        :param emoScore: 情感分值
        :return: 情感标签
        """
        emo_label = self.emo_label.get(str(self.emoScore))
        return emo_label
    def add_uuid(self,data):
        """
        对字符串进行加密
        :return: 加密字符串
        """
        data = uuid.uuid3(uuid.NAMESPACE_DNS, data)
        data = str(data)
        result_data = data.replace('-', '')
        return result_data

    def mysql_insert_g_gao(self,result):
        """
        储存数据  公告表
        :param result:
        :return:
        """
        conn = self.mysql_client()
        sql = """INSERT INTO sy_project_raw.aa_dws_ggyq_search_add (yqid,
                                                                    title,
                                                                    webname,
                                                                    companyName,
                                                                    cmpShortName,
                                                                    cmpCode,
                                                                    bondFull,
                                                                    bondAbbr,
                                                                    bondCode,
                                                                    firstIndustry,
                                                                    firstIndustryCode,
                                                                    secondIndustry,
                                                                    secondIndustryCode,
                                                                    threeIndustry,
                                                                    threeIndustryCode,
                                                                    firstLevelCode,
                                                                    firstLevelName,
                                                                    secondLevelCode,
                                                                    secondLevelName,
                                                                    threeLevelCode,
                                                                    threeLevelName,
                                                                    fourLevelCode,
                                                                    fourLevelName,
                                                                    eventCode,
                                                                    eventName,
                                                                    emoScore,
                                                                    emoLabel,
                                                                    emoConf,
                                                                    impScore,
                                                                    impLabel,
                                                                    srcType,
                                                                    srcUrl,
                                                                    pubTime) VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);"""
        conn.insertMany(sql, result)
        conn.dispose()
        print("已存入--aa_dws_ggyq_search_add")
    def mysql_insert_u_yuqing(self,result):
        """
        储存数据  舆情表
        :param result:
        :return:
        """
        conn = self.mysql_client()
        sql = """INSERT INTO sy_project_raw.dwa_me_yq_search_add (yqid,
                                                                    objName,
                                                                    companyShortName,
                                                                    companyCode,
                                                                    indirectObjName,
                                                                    transScore,
                                                                    relPath,
                                                                    relType,
                                                                    relScore,
                                                                    relLabel,
                                                                    personName,
                                                                    eventCode,
                                                                    eventName,
                                                                    firstLevelCode,
                                                                    firstLevelName,
                                                                    secondLevelCode,
                                                                    secondLevelName,
                                                                    thirdLevelCode,
                                                                    thirdLevelName,
                                                                    fourthLevelCode,
                                                                    fourthLevelName,
                                                                    emoLabel,
                                                                    emoScore,
                                                                    emoConf,
                                                                    impScore,
                                                                    impLabel,
                                                                    pubTime,
                                                                    title,
                                                                    summary,
                                                                    keyword,
                                                                    srcUrl,
                                                                    srcType,
                                                                    source,
                                                                    content) VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);"""
        conn.insertMany(sql, result)
        conn.dispose()
        print("已存入--dwa_me_yq_search_add")
    def mongo_date(self):
        """
        组成mongo所需要的的字典
        :return:
        """
        dateArray = datetime.datetime.fromtimestamp(time.time())
        otherStyleTime = dateArray.strftime("%Y-%m-%d %H:%M:%S")
        transScore = ""
        relPath = ""
        personName = ""
        relType = "直接关联"
        summary = ""
        keyword = ""
        content = ""
        relScore = ""
        relLabel = ""
        self.emolabel = self.emoLabel_i()
        self.impLabel = self.impLabel_i()
        self.mydict["onlyId"] = self.onlyId
        self.mydict["yqid"] = self.yqid
        self.mydict["objName"] = self.companyName
        self.mydict["companyShortName"] = self.cmpShortName
        self.mydict["companyCode"] = self.cmpCode
        self.mydict["indirectObjName"] = self.companyName
        self.mydict["transScore"] = transScore
        self.mydict["relPath"] = relPath
        self.mydict["relType"] = relType
        self.mydict["relScore"] = relScore
        self.mydict["relLabel"] = relLabel
        self.mydict["personName"] = personName
        self.mydict["eventCode"] = self.eventCode
        self.mydict["eventName"] = self.eventName
        self.mydict["firstLevelCode"] = self.firstLevelCode
        self.mydict["firstLevelName"] = self.firstLevelName
        self.mydict["secondLevelCode"] = self.secondLevelCode
        self.mydict["secondLevelName"] = self.secondLevelName
        self.mydict["thirdLevelCode"] = self.threeLevelCode
        self.mydict["thirdLevelName"] = self.threeLevelName
        self.mydict["fourthLevelCode"] = self.fourLevelCode
        self.mydict["fourthLevelName"] = self.fourLevelName
        self.mydict["emoLabel"] = self.emolabel
        self.mydict["emoScore"] = self.emoScore
        self.mydict["emoConf"] = self.emoConf
        self.mydict["impScore"] = self.impScore
        self.mydict["impLabel"] = self.impLabel
        self.mydict["pubTime"] = self.pubTime
        self.mydict["title"] = self.title
        self.mydict["summary"] = summary
        self.mydict["keyword"] = keyword
        self.mydict["srcUrl"] = self.srcUrl
        self.mydict["srcType"] = self.srcType
        self.mydict["source"] = self.webname
        self.mydict["content"] = content
        self.mydict["isValid"] = 1
        self.mydict["dataStatus"] = 1
        self.mydict["createTime"] = otherStyleTime
        self.mydict["modifyTime"] = otherStyleTime
    def mongo_insert(self):
        """
        将数据储存在mongo中
        :return:
        """
        mydb = self.myclient["sy_project_raw"]
        mycol = mydb["dwa_me_yq_search"]
        self.onlyId = self.add_uuid(self.yqid+self.companyName+str(self.eventCode))
        self.mongo_date()
        print(self.mydict)
        my_dict_new = {}
        try:
            my_dict_new.update(self.mydict)
            mycol.insert(my_dict_new)
        except:
            print("重复")
        print("保存mongo数据一份")
def main():
    """
    启动方法并处理准备参数
    :return:
    """
    kafka_losd = Kafka_consumer_s()
    kafka_losd.kafka_take_out()
if __name__ == '__main__':
    main()

