import pandas as pd
import datetime
import os

class TransactionDataFrame():
    def __init__(self):
        self.saveDir = "./csv/"
        self.transaction = pd.DataFrame(index=[], columns=["BlockHeight","BlockFileNumber","LockTime","HashTransaction","Version","InputNumber","OutputNumber","TotalInputValue","TotalOutputValue","Fee","RawTx"])
        self.blockHeightList      = []
        self.blockFileNumberList  = []
        self.hashTransactionList  = []
        self.lockTimeList         = []
        self.versionList          = []
        self.inputCountList       = []
        self.outputCountList      = []
        self.totalInputValueList  = []
        self.totalOutputValueList = []
        self.feeList              = []
        self.rawTxList            = []

    def initDf(self):
        self.transaction = pd.DataFrame(index=[], columns=["BlockHeight","BlockFileNumber","LockTime","HashTransaction","Version","InputNumber","OutputNumber","TotalInputValue","TotalOutputValue","Fee","RawTx"])

    def initLists(self):
        self.blockHeightList      = []
        self.blockFileNumberList  = []
        self.hashTransactionList  = []
        self.lockTimeList         = []
        self.versionList          = []
        self.inputCountList       = []
        self.outputCountList      = []
        self.totalInputValueList  = []
        self.totalOutputValueList = []
        self.feeList              = []
        self.rawTxList            = []

    def addList(self, blockHeight, blocFilekNumber, hashTransaction, lockTime, version, inputCount, outputCount, totalInputValue, totalOutputValue, fee, rawTx):
        self.blockHeightList.append(blockHeight)
        self.blockFileNumberList.append(blocFilekNumber)
        self.hashTransactionList.append(hashTransaction.decode('utf-8'))
        self.lockTimeList.append(lockTime)
        self.versionList.append(version)
        self.inputCountList.append(inputCount)
        self.outputCountList.append(outputCount)
        self.totalInputValueList.append(totalInputValue)
        self.totalOutputValueList.append(totalOutputValue)
        self.feeList.append(fee)
        self.rawTxList.append(rawTx)
    
    def mergeListWithDf(self):
        transactionDict = {"BlockHeight":self.blockHeightList,"BlockFileNumber":self.blockFileNumberList,"LockTime":self.lockTimeList, "HashTransaction":self.hashTransactionList,"Version":self.versionList,"InputNumber":self.inputCountList,"OutputNumber":self.outputCountList,"TotalInputValue":self.totalInputValueList,"TotalOutputValue":self.totalOutputValueList,"Fee":self.feeList, "RawTx":self.rawTxList}
        df = pd.DataFrame.from_dict(transactionDict)
        self.transaction = pd.concat([self.transaction, df],sort=False)
        self.transaction.reset_index(drop=True, inplace=True)
        self.initLists()

    def addLine(self, line):
        df = pd.DataFrame.from_dict(line)
        self.transaction = pd.concat([self.transaction, df],sort=False)

    def readTransaction(self):
        print(self.transaction)

    def writeToCsv(self, blockHeight, saveDir="./csv/"):
        self.makeDir(saveDir)
        fileName = "transaction_{}.csv".format(blockHeight)
        self.transaction.to_csv(saveDir+fileName, index=False)

    def writeToPickle(fileName, saveDir="./pickle/"):
        self.makeDir(saveDir)
        self.transaction.to_pickle(saveDir+fileName)

    def makeDir(self, path):
        if not os.path.isdir(path):
            os.makedirs(path)