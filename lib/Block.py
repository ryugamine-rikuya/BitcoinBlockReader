import pandas as pd
import datetime
import os

class BlockDataFrame():
    def __init__(self):
        self.saveDir = "./csv/"
        self.block   = pd.DataFrame(index=[], columns=["BlockHeight","BlockFileNumber","MagicNumber","Blocksize","Version","PreviousHash","MerkleHash","CreationTimestamp","Time","Bits","Nonce","CountOfTransactions"])

    def initDf(self):
        self.block   = pd.DataFrame(index=[], columns=["BlockHeight","BlockFileNumber","MagicNumber","Blocksize","Version","PreviousHash","MerkleHash","CreationTimestamp","Time","Bits","Nonce","CountOfTransactions"])

    def addLine(self, blockHeight, blocFilekNumber, magicNumber, blockSize, version, previousHash, merkleHash, creationTimeTimestamp, creationTime, bits, nonce, countOfTransactions):
        lineDict = {"BlockHeight":[blockHeight], "BlocFilekNumber":[blocFilekNumber],"MagicNumber":[magicNumber],"Blocksize":[blockSize],"Version":[version],"PreviousHash":[previousHash],"MerkleHash":[merkleHash],"CreationTimestamp":[creationTimeTimestamp],"Time":[creationTime],"Bits":[bits],"Nonce":[nonce],"CountOfTransactions":[countOfTransactions]}
        df = pd.DataFrame.from_dict(lineDict)
        self.block = pd.concat([self.block, df],sort=False)

    def readBlock(self):
        print(self.block)

    def writeToCsv(self, blockFileNumber, saveDir="./csv/"):
        self.makeDir(saveDir)
        fileName = "block_"+str(blockFileNumber)+".csv"
        self.block.to_csv(saveDir+fileName, index=False)

    
    def makeDir(self, path):
        if not os.path.isdir(path):
            os.makedirs(path)