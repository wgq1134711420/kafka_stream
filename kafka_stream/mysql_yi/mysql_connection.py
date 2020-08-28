# -*- coding: utf-8 -*-
# @Author : wgq
# @time   : 2020/8/10 17:47
# @File   : mysql_connection.py
# Software: PyCharm

import pymysql

class mysql_conn():
    def __init__(self,mysql_ms_sq):
        self.conn = pymysql.connect(
            port=4000,
            host="192.168.1.135",
            user ="kf3_first_capital", password ="shiye1805A",
            database ="seeyii_assets_database",
            charset ="utf8")

        self.mysql_ms_sq = mysql_ms_sq

    def mysq_related_query(self):
        cursor = self.conn.cursor()
        cursor.execute(self.mysql_ms_sq)
        return cursor.fetchall()



if __name__ == '__main__':
    sq_s = 'select * from sy_cd_ms_ind_comp_gm where  id < 10'
    sq_t = "select * from sy_cd_mt_sys_const where id < 10"
    mysql_ss = mysql_conn(sq_s,sq_t)
    mysql_ss.mysql_sy_cd_mt_sys_const()