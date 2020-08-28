# -*- coding: utf-8 -*-
# @Author : wgq
# @time   : 2020/8/20 17:03
# @File   : mysql_kafka_sq.py
# Software: PyCharm
import pymongo
import pymysql
class mysql_name_all():
    def __init__(self):
        self.full_name = {}
    def name_date(self,tupels):
        full_name = {}
        for i in tupels:
            short_name = i[1]
            all_name = i[2]
            full_name[short_name] = all_name
        return full_name

    def mysqls_all_name(self):
        print("只有第一个进来了")
        """
        匹配全称
        :param stock_name:
        :return:
        """
        conn = pymysql.connect(
            port=3306,
            host="192.168.1.129",
            user ="batchdata_3dep", password ="shiye1805A",
            database ="EI_BDP",
            charset ="utf8")
        cursor = conn.cursor()
        mysql = "SELECT * FROM A_stock_code_name_fyi"
        cursor.execute(mysql)
        tupels = cursor.fetchall()
        conn.close()
        date = self.name_date(tupels)
        return date


def mysqls_categorycode(all_name):
        """
        匹配行业
        :param all_name:
        :return:
        """
        conn = pymysql.connect(
            port=4000,
            host="192.168.1.135",
            user ="kf3_first_capital", password ="shiye1805A",
            database ="seeyii_assets_database",
            charset ="utf8")
        sql = "SELECT A.compName, A.categoryCode, B.constValueDesc, B.constCode FROM(SELECT * FROM sy_cd_ms_ind_comp_gm WHERE compName = '{}') AS A INNER JOIN ( SELECT * FROM sy_cd_mt_sys_const WHERE constCode IN ( 3, 4, 5 ) ) AS B ON A.categoryCode = B.cValue".format(all_name)
        cursor = conn.cursor()
        cursor.execute(sql)
        conn.close()
        return cursor.fetchall()
def mysql():
    conn = pymysql.connect(
        port=3306,
        host="192.168.1.129",
        user ="batchdata_3dep", password ="shiye1805A",
        database ="sy_yq_raw",
        charset="utf8")
    sql = "SELECT id,firstLevelCode,firstLevelName,secondLevelCode,secondLevelName,threeLevelCode,threeLevelName,fourLevelCode,fourLevelName,cfEventCode,eventCode,eventName,inRules,filterRules,emoScore,impScore,isChange,isValid,dataStatus FROM sy_yq_lvl_rules_code WHERE inRules != '' and inRules IS NOT NULL"
    cursor = conn.cursor()
    cursor.execute(sql)
    conn.close()
    return cursor.fetchall()
if __name__ == '__main__':
    mysqls()
