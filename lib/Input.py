import pandas as pd
import datetime
import os

class InputDataFrame():
    def __init__(self):
        self.saveDir = "./csv/"
        self.input   = pd.DataFrame(index=[], columns=["HashTransaction","PreviousHash","OutId","ScriptLength","ScriptSignatureRaw","SeqNo","Signature","PublicKey","Address", "Witnesses"])
        self.hashTransactionList     = []
        self.previousHashList        = []
        self.outIdList               = []
        self.scriptLengthList        = []
        self.scriptSignatureRawList  = []
        self.seqNoList               = []
        self.signatureList           = []
        self.publicKeyList           = []
        self.addressList             = []
        self.witnessesList           = []

    def initDf(self):
        self.input   = pd.DataFrame(index=[], columns=["HashTransaction","PreviousHash","OutId","ScriptLength","ScriptSignatureRaw","SeqNo","Signature","PublicKey","Address", "Witnesses"])

    def initLines(self):
        self.hashTransactionList     = []
        self.previousHashList        = []
        self.outIdList               = []
        self.scriptLengthList        = []
        self.scriptSignatureRawList  = []
        self.seqNoList               = []
        self.signatureList           = []
        self.publicKeyList           = []
        self.addressList             = []
        self.witnessesList           = []

    def mergeListWithDf(self):
        inputDict = {"HashTransaction":self.hashTransactionList,"PreviousHash":self.previousHashList,"OutId":self.outIdList,"ScriptLength":self.scriptLengthList,"ScriptSignatureRaw":self.scriptSignatureRawList,"SeqNo":self.seqNoList,"Signature":self.signatureList,"PublicKey":self.publicKeyList,"Address":self.addressList, "Witnesses":self.witnessesList}
        df = pd.DataFrame.from_dict(inputDict)
        self.input = pd.concat([self.input, df],sort=False)
        self.input.reset_index(drop=True, inplace=True)
        self.initLines()

    def addList(self, previousHash, outId, scriptLength, scriptSignatureRaw, seqNo, signature, publicKey, address, witness):
        self.previousHashList.append(previousHash)
        self.outIdList.append(outId)
        self.scriptLengthList.append(scriptLength)
        self.scriptSignatureRawList.append(scriptSignatureRaw)
        self.seqNoList.append(seqNo)
        self.signatureList.append(signature)
        self.publicKeyList.append(publicKey)
        self.addressList.append(address)
        self.witnessesList.append(witness)

    def addHashTransactionList(self, hashTransaction, inputCount):
        for i in range(inputCount):
            self.hashTransactionList.append(hashTransaction)

    def addWitnesses(self, witnesses):
        self.witnessesList.append(witnesses)

    def updateWitnesses(self, witnesses, index):
        self.witnessesList[index] = witnesses

    def addAddress(self, address):
        self.addressList.append(address)

    def updateAddress(self, address, index):
        self.addressList[index] = address

    def notExistAddress(self, index):
        if self.addressList[index] == b'':
            return True
        else:
            return False

    def addLine(self, line):
        df = pd.DataFrame.from_dict(line)
        self.input = pd.concat([self.input, df],sort=False)

    def readInput(self):
        print(self.input)

    def writeToCsv(self, blockNumber, saveDir="./csv/"):
        self.makeDir(saveDir)
        fileName = "input_{}.csv".format(blockNumber)
        self.input.to_csv(saveDir+fileName, index=False)


    def makeDir(self, path):
        if not os.path.isdir(path):
            os.makedirs(path)