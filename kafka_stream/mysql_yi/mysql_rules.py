import pymysql
import pandas as pd
class pands_mysql():
    def __init__(self):
        self.df = ""
    def mysql(self):
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
    def pandsa(self):
        data = self.mysql()
        self.len_list = len(data)
        data_list = []
        for i in data:
            data_list.append(list(i))
        df = pd.DataFrame(data=data_list,columns=["id","firstLevelCode","firstLevelName","secondLevelCode","secondLevelName","threeLevelCode","threeLevelName","fourLevelCode","fourLevelName","cfEventCode","eventCode","eventName","inRules","filterRules","emoScore","impScore","isChange","isValid","dataStatus"])
        self.df = df
    def pd_dataframe(self,test):
        if self.df:
            print(self.df)
        else:
            self.pandsa()
        print("youle")
        print(self.len_list)
        for i in range(self.len_list):
            inRules_list = [self.df.loc[i,"inRules"]][0]
            filterRules_list = [self.df.loc[i, "filterRules"]][0]
            in_list = [rule.strip() for rule in inRules_list.split('、') if inRules_list]
            in_lists = [rule.split('&') for rule in in_list]
            filter_rules = [[rule.strip()] for rule in filterRules_list.split('、') if filterRules_list]
            # print(filter_rules)
            if_csv = self.list_if(in_lists,filter_rules,test)
            if if_csv:
                self.pands_dateframe_csv(i)
            else:
                pass
    def list_if(self,in_lists,filter_rules,test):
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
        result = []
        for word in words:
            if word in test:
                result.append(word)
        return result
    def pands_dateframe_csv(self,i):
            id = self.df.loc[i, "id"]
            firstLevelCode = self.df.loc[i, "firstLevelCode"]
            firstLevelName = self.df.loc[i, "firstLevelName"]
            secondLevelCode = self.df.loc[i, "secondLevelCode"]
            secondLevelName = self.df.loc[i, "secondLevelName"]
            threeLevelCode = self.df.loc[i, "threeLevelCode"]
            threeLevelName = self.df.loc[i, "threeLevelName"]
            fourLevelCode = self.df.loc[i, "fourLevelCode"]
            fourLevelName = self.df.loc[i, "fourLevelName"]
            cfEventCode = self.df.loc[i, "cfEventCode"]
            eventCode = self.df.loc[i, "eventCode"]
            eventName = self.df.loc[i, "eventName"]
            inRules = self.df.loc[i, "inRules"]
            filterRules = self.df.loc[i, "filterRules"]
            emoScore = self.df.loc[i, "emoScore"]
            impScore = self.df.loc[i, "impScore"]
            isChange = self.df.loc[i, "isChange"]
            isValid = self.df.loc[i, "isValid"]
            dataStatus = self.df.loc[i, "dataStatus"]
            with open("data.csv", "a+") as a:
                a.write(str([str(id), str(id), str(firstLevelCode), str(firstLevelName), str(secondLevelCode),
                             str(secondLevelName), str(threeLevelCode), str(threeLevelName), str(fourLevelCode),
                             str(fourLevelName), str(cfEventCode), str(eventCode), str(eventName), str(inRules),
                             str(filterRules), str(emoScore), str(impScore), str(isChange), str(isValid),
                             str(dataStatus)]) + "\n")

if __name__ == '__main__':
    tets = "14雏鹰债：雏鹰农牧公司债券临时受托管理事务报告(2019年度第九期)"
    k = pands_mysql()
    k.pd_dataframe(tets)