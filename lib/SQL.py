import pandas as pd
import sqlite3
import pandas.io.sql as psql
import datetime
import pymysql
import sqlalchemy as sqa
import os

class SqlDataFrame():
    def __init__(self, sqliteMode=False, mysqlMode=False, sqliteFilename='Bitcoin.db'):
        self.saveDir = "./csv/"
        self.sqlDf   = pd.DataFrame(index=[], columns=["BlockHeight","BlockFileNumber","HashTransaction", "Address","InOutFlag","CreationTimestamp"])
        self.blockHeightList       = []
        self.blockFileNumberList   = []
        self.hashTransactionList   = []
        self.addressList           = []
        self.inOutFlagList         = []
        self.creationTimestampList = []
        self.sqliteCon             = ""
        self.sqliteCur             = ""
        self.mysqlCon              = ""
        self.mysqlCur              = ""
        self.mysqlUrl              = ""
        if mysqlMode:
            self.mysqlCon = pymysql.connect(
                host="localhost",
                user="root",
                db="Bitcoin",
                password="",
                charset="utf8",
                cursorclass=pymysql.cursors.DictCursor)
            self.mysqlCur = self.mysqlCon.cursor()
            self.mysqlUrl = 'mysql+pymysql://root:@localhost:3306/Bitcoin?charset=utf8'
            self.engine = sqa.create_engine(self.mysqlUrl, echo=True)
            self.mysqlCur.execute('CREATE TABLE IF NOT EXISTS BitcoinTransaction (BlockHeight int,BlockFileNumber int,HashTransaction text, Address text,InOutFlag int,CreationTimestamp int)')

    def initDf(self):
        self.sqlDf   = pd.DataFrame(index=[], columns=["BlockHeight","BlockFileNumber","HashTransaction", "Address","InOutFlag","CreationTimestamp"])


    def initLists(self):
        self.blockHeightList       = []
        self.blockFileNumberList   = []
        self.hashTransactionList   = []
        self.addressList           = []
        self.inOutFlagList         = []
        self.creationTimestampList = []

    def mergeListWithDf(self):
        sqlDict = {"BlockHeight":self.blockHeightList,"BlockFileNumber":self.blockFileNumberList,"HashTransaction":self.hashTransactionList, "Address":self.addressList,"InOutFlag":self.inOutFlagList,"CreationTimestamp":self.creationTimestampList}
        df = pd.DataFrame.from_dict(sqlDict)
        self.sqlDf = pd.concat([self.sqlDf, df],sort=False)
        self.sqlDf.reset_index(drop=True, inplace=True)
        self.initLists()

    def addList(self, blockHeight,blockFileNumber,hashTransaction, address,inOutFlag,creationTimestamp):
        self.blockHeightList.append(blockHeight)
        self.blockFileNumberList.append(blockFileNumber)
        self.hashTransactionList.append(hashTransaction.decode('utf-8'))
        self.addressList.append(address.decode('utf-8'))
        self.inOutFlagList.append(inOutFlag)
        self.creationTimestampList.append(creationTimestamp)


    def readSqlDf(self):
        print(self.sqlDf)

    def writeToCsv(self, blockNumber, saveDir="./csv/"):
        self.makeDir(saveDir)
        fileName = "sqlDf_{}.csv".format(blockNumber)
        self.sqlDf.to_csv(saveDir+fileName, index=False)

    def writeToSqlite(self, sqliteFilename):
        self.sqliteCon = sqlite3.connect(sqliteFilename)
        self.sqliteCur = self.sqliteCon.cursor()
        self.sqliteCur.execute('CREATE TABLE IF NOT EXISTS BitcoinTransaction (BlockHeight int,BlockFileNumber int,HashTransaction text, Address text,InOutFlag int,CreationTimestamp int)')
        self.sqlDf.to_sql('BitcoinTransaction', self.sqliteCon, if_exists='append', index=None)

    def writeToMysql(self):
        self.sqlDf.to_sql('BitcoinTransaction', self.mysqlUrl, if_exists='append', index=None)

    def writeToPickle(self, fileName, saveDir="./pickle/sql/"):
        self.makeDir(saveDir)
        self.sqlDf.to_pickle(saveDir+fileName)


    def makeDir(self,path):
        if not os.path.isdir(path):
            os.makedirs(path)        