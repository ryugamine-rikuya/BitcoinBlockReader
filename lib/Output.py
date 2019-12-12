import pandas as pd
import datetime
import os

class OutputDataFrame():
    def __init__(self):
        self.saveDir = "./csv/"
        self.output   = pd.DataFrame(index=[], columns=["HashTransaction","Value", "ScriptLength", "ScriptSignatureRaw", "Address"])
        self.hashTransactionList     = []
        self.valueList               = []
        self.scriptLengthList        = []
        self.scriptSignatureRawList  = []
        self.addressList             = []

    def initDf(self):
        self.output = pd.DataFrame(index=[], columns=["HashTransaction","Value", "ScriptLength", "ScriptSignatureRaw", "Address"])


    def initLists(self):
        self.hashTransactionList     = []
        self.valueList               = []
        self.scriptLengthList        = []
        self.scriptSignatureRawList  = []
        self.addressList             = []

    def mergeListWithDf(self):
        outputDict = {"HashTransaction":self.hashTransactionList,"Value":self.valueList, "ScriptLength":self.scriptLengthList, "ScriptSignatureRaw":self.scriptSignatureRawList, "Address":self.addressList}
        df = pd.DataFrame.from_dict(outputDict)
        self.output = pd.concat([self.output, df],sort=False)
        self.output.reset_index(drop=True, inplace=True)
        self.initLists()

    def addList(self, value, scriptLength, scriptSignatureRaw, address):
        self.valueList.append(value)
        self.scriptLengthList.append(scriptLength)
        self.scriptSignatureRawList.append(scriptSignatureRaw)
        self.addressList.append(address.decode('utf-8'))

    def addHashTransactionList(self, hashTransaction, outputCount):
        for i in range(outputCount):
            self.hashTransactionList.append(hashTransaction)

    def addLine(self, line):
        df = pd.DataFrame.from_dict(line)
        self.output = pd.concat([self.output, df],sort=False)

    def readOutput(self):
        print(self.output)

    def writeToCsv(self, blockNumber, saveDir="./csv/"):
        self.makeDir(saveDir)
        fileName = "output_{}.csv".format(blockNumber)
        self.output.to_csv(saveDir+fileName, index=False)

    def writeToPickle(fileName, saveDir="./pickle/"):
        self.makeDir(saveDir)
        self.output.to_pickle(saveDir+fileName)

    def makeDir(self, path):
        if not os.path.isdir(path):
            os.makedirs(path)