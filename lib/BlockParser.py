import tqdm
import os
import binascii
import struct
import datetime
import hashlib
import base58
import sys
import array
import traceback
import logging
from _stat import filemode
import pandas as pd
import sqlite3
import pandas.io.sql as psql
import glob
import configparser
from multiprocessing import Pool
from multiprocessing import Process
import logging
from logging import getLogger, StreamHandler, Formatter, FileHandler

from lib.SQL         import SqlDataFrame
from lib.Block       import BlockDataFrame
from lib.Transaction import TransactionDataFrame
from lib.Input       import InputDataFrame
from lib.Output      import OutputDataFrame



class BlockParser():
  def __init__(self, 
    readBlockFlag    = False,
    readTxFlag       = False,
    readInputFlag    = False,
    readOutputFlag   = False,
    readSqlFmtFlag   = False,
    saveBlockFlag    = False, 
    saveTxFlag       = False, 
    saveInputFlag    = False, 
    saveOutputFlag   = False,
    saveSqlAsMysqlFlag   = False,
    saveSqlAsSqliteFlag  = False,
    saveSqlAsCsvFlag = False):
    self.blockFile       = None
    self.blockHeight     = 0
    self.blockFilename   = None
    self.blockFileNumber = None
    self.readBlockFlag       = readBlockFlag
    self.readTxFlag          = readTxFlag
    self.readInputFlag       = readInputFlag
    self.readOutputFlag      = readOutputFlag
    self.readSqlFmtFlag      = readSqlFmtFlag
    self.saveBlockFlag       = saveBlockFlag
    self.saveTxFlag          = saveTxFlag
    self.saveInputFlag       = saveInputFlag
    self.saveOutputFlag      = saveOutputFlag
    self.saveSqlAsMysqlFlag  = saveSqlAsMysqlFlag
    self.saveSqlAsSqliteFlag = saveSqlAsSqliteFlag
    self.saveSqlAsCsvFlag    = saveSqlAsCsvFlag
    self.sqlDf               = SqlDataFrame(sqliteMode=self.saveSqlAsSqliteFlag, mysqlMode=self.saveSqlAsMysqlFlag)
    self.blockDf             = BlockDataFrame()
    self.transactionDf       = TransactionDataFrame()
    self.inputDf             = InputDataFrame()
    self.outputDf            = OutputDataFrame()

    cfg = configparser.ConfigParser()
    cfg.read("./lib/config/development.conf")
    self.APP_NAME                      = cfg["section1"]["APP_NAME"]
    self.SAVE_SQL_CSV_DIR_PATH         = cfg["section1"]["SAVE_SQL_CSV_DIR_PATH"]
    self.SAVE_SQL_SQLITE_DIR_PATH      = cfg["section1"]["SAVE_SQL_SQLITE_DIR_PATH"]
    self.SAVE_BLOCK_CSV_DIR_PATH       = cfg["section1"]["SAVE_BLOCK_CSV_DIR_PATH"]
    self.SAVE_INPUT_CSV_DIR_PATH       = cfg["section1"]["SAVE_INPUT_CSV_DIR_PATH"]
    self.SAVE_OUTPUT_CSV_DIR_PATH      = cfg["section1"]["SAVE_OUTPUT_CSV_DIR_PATH"]
    self.SAVE_TRANSACTION_CSV_DIR_PATH = cfg["section1"]["SAVE_TRANSACTION_CSV_DIR_PATH"]
    self.COINBASE_PREVIOUST_HASH       = b'0000000000000000000000000000000000000000000000000000000000000000'
    self.logger          = self.loggingSetting()
    
    
  
  def loggingSetting(self):
    logLevel = logging.DEBUG
    logger = getLogger(self.APP_NAME)
    logger.setLevel(logLevel)
    handler_format = Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    stream_handler = StreamHandler()
    stream_handler.setLevel(logLevel)
    stream_handler.setFormatter(handler_format)
    logger.addHandler(stream_handler)
    file_handler = FileHandler("{}{}.log".format("./log/" , self.APP_NAME))
    file_handler.setLevel(logLevel)
    file_handler.setFormatter(handler_format)
    logger.addHandler(file_handler)
    return logger
    


  def setBlockFileName(self, blockFilename=None):
    self.blockFilename = blockFilename
    if self.blockFilename:
      self.blockFileNumber = int(self.blockFilename.split("/")[-1].split(".")[0].split("blk")[1])
  
  def startBlockFileParse(self):
    self.logger.info("startBlockFileParse  file name is "+str(self.blockFilename))
    while True:
      try:
        #self.logger.info("block file number : ",self.blockFileNumber," ,block Height : ", self.blockHeight)
        self.readBlock()
      except Exception as e:
        self.logger.error(e)
        break
      else:
        if self.saveInputFlag:
          self.inputDf.mergeListWithDf()
          self.inputDf.writeToCsv(self.blockHeight, self.SAVE_INPUT_CSV_DIR_PATH.format(self.blockFileNumber))
          self.inputDf.initDf()
        if self.saveOutputFlag:
          self.outputDf.mergeListWithDf()
          self.outputDf.writeToCsv(self.blockHeight, self.SAVE_OUTPUT_CSV_DIR_PATH.format(self.blockFileNumber))
          self.outputDf.initDf()
        if self.saveTxFlag:
          self.transactionDf.mergeListWithDf()
          self.transactionDf.writeToCsv(self.blockHeight, self.SAVE_TRANSACTION_CSV_DIR_PATH.format(self.blockFileNumber))
          self.transactionDf.initDf()
        self.blockHeight += 1
    if self.saveSqlAsCsvFlag or self.saveSqlAsSqliteFlag or self.saveSqlAsMysqlFlag:
        self.sqlDf.mergeListWithDf()
        if self.saveSqlAsCsvFlag:
          self.sqlDf.writeToPickle(str(self.blockFileNumber)+".pickle")
        if self.saveSqlAsSqliteFlag:
          self.sqlDf.writeToSqlite(self.SAVE_SQL_SQLITE_DIR_PATH.format(str(self.blockFileNumber)+".db"))
        if self.saveSqlAsMysqlFlag:
          self.sqlDf.writeToMysql()
        self.sqlDf.initDf()
    if self.saveBlockFlag:
      self.blockDf.writeToPickle(str(self.blockFileNumber)+".pickle")
      self.blockDf.initDf()
    self.logger.info("finish startBlockFileParse  file name is "+str(self.blockFilename))

  def openBlockFile(self):
    self.logger.info("open "+str(self.blockFilename)+" file")
    if self.blockFilename:
      try:
        self.blockFile = open(self.blockFilename, "rb")
      except Exception as e:
        self.logger.error(e)
      
  def closeBlockFile(self):
    self.logger.info("close "+str(self.blockFilename)+" file")
    self.blockFile.close()

  def readBlock(self):
    magicNumber           = binascii.hexlify(self.blockFile.read(4))
    blockSize             = self.hexToInt(self.readIntLittleEndian())
    version               = self.hexToInt(self.readIntLittleEndian())
    previousHash          = binascii.hexlify(self.blockFile.read(32))
    merkleHash            = binascii.hexlify(self.blockFile.read(32))
    creationTimeTimestamp = self.hexToInt(self.readIntLittleEndian())
    creationTime          = datetime.datetime.fromtimestamp(creationTimeTimestamp).strftime('%Y-%m-%d %H:%M')
    bits                  = self.hexToInt(self.readIntLittleEndian())
    nonce                 = self.hexToInt(self.readIntLittleEndian())
    countOfTransactions   = self.readVarInt()
    rawTx                 = ""  # NOTE トランザクションを全て保存すると容量がとんでもないことになるため, ""を代入. 容量気にしない場合は利用する. 利用の仕方は実装したい人に任せます.
    self.readTransactions(countOfTransactions, creationTimeTimestamp)
    if self.saveBlockFlag:
      self.blockDf.addLine(self.blockHeight, self.blockFileNumber, magicNumber, blockSize, version, previousHash, merkleHash, creationTimeTimestamp, creationTime, bits, nonce, countOfTransactions)
    
  def readInput(self):
    previousHash         = binascii.hexlify(self.blockFile.read(32)[::-1])
    outId                = binascii.hexlify(self.readIntLittleEndian())
    scriptLength         = self.readVarInt()
    scriptSignatureRaw   = self.hexToStr(self.blockFile.read(scriptLength))
    seqNo                = binascii.hexlify(self.readIntLittleEndian())
    signature            = b""
    publicKey            = b""
    address              = b""
    witness              = b""
    # NOTE COINBASEのトランザクションは無視
    if previousHash != self.COINBASE_PREVIOUST_HASH:
      signature, publicKey = self.divideScriptSignatureRaw(scriptSignatureRaw)
      address              = self.publicKeyHashDecode(publicKey)
    if self.saveInputFlag:
      self.inputDf.addList(previousHash, outId, scriptLength, scriptSignatureRaw, seqNo, signature, publicKey, address, witness)
    return address

  def readOutput(self):
    value              = self.convertToBtc(self.hexToInt(self.readLongLittleEndian()))
    scriptLength       = self.readVarInt()
    scriptSignatureRaw = self.hexToStr(self.blockFile.read(scriptLength))
    scriptSignature    = scriptSignatureRaw
    address            = ''
    try:
      address = self.publicKeyDecode(scriptSignature)
    except Exception as e:
      print (e)
    if self.saveOutputFlag:
      self.outputDf.addList(value, scriptLength, scriptSignatureRaw, address)
    return address

  def readTransactions(self, countOfTransactions, creationTimeTimestamp):
    for transactionIndex in range(0, countOfTransactions):
      self.readTransaction(creationTimeTimestamp)

  
  def readTransaction(self, creationTimeTimestamp):
    extendedFormat = False
    beginByte      = self.blockFile.tell()
    version        = self.hexToInt(self.readIntLittleEndian())
    cutStart1      = self.blockFile.tell()
    cutEnd1        = 0
    inputCount     = self.readVarInt()
    inputAddressList  = []
    outputAddressList = []
    if self.saveSqlAsCsvFlag or self.saveSqlAsMysqlFlag or self.saveSqlAsSqliteFlag:
      # NOTE input output txのparse処理
      if inputCount == 0:
        extendedFormat = True
        flags          = ord(self.blockFile.read(1))
        cutEnd1        = self.blockFile.tell()
        if flags != 0:
          inputCount = self.readVarInt()
          for inputIndex in range(0, inputCount):
            inputAddressList.append(self.readInput())
          outputCount = self.readVarInt()
          for outputIndex in range(0, outputCount):
            outputAddressList.append(self.readOutput())
      else:
        cutStart1 = 0
        cutEnd1   = 0
        for inputIndex in range(0, inputCount):
          inputAddressList.append(self.readInput())
        outputCount = self.readVarInt()
        for outputIndex in range(0, outputCount):
          outputAddressList.append(self.readOutput())
      
      cutStart2 = 0
      cutEnd2   = 0
      if extendedFormat:
        if flags & 1:
          cutStart2 = self.blockFile.tell()
          for inputIndex in range(0, inputCount):
            countOfStackItems = self.readVarInt()
            witnesses         = []
            lastWitness       = b""
            for stackItemIndex in range(0, countOfStackItems):
              stackLength = self.readVarInt()
              stackItem   = self.blockFile.read(stackLength)[::-1]
              lastWitness = self.stringLittleEndianToBigEndian(stackItem)
              witnesses.append(self.stringLittleEndianToBigEndian(stackItem).decode('utf-8'))
            address    = self.publicKeyHashDecode(lastWitness)
            witness    = "".join(witnesses)
            inputIndex = -(inputCount-inputIndex)
            if self.saveInputFlag:
              self.inputDf.updateWitnesses(witness, inputIndex)
              if self.inputDf.notExistAddress(inputIndex):
                self.inputDf.updateAddress(address, inputIndex)
          cutEnd2 = self.blockFile.tell()

    else:
      # NOTE input output txのparse処理
      if inputCount == 0:
        extendedFormat = True
        flags          = ord(self.blockFile.read(1))
        cutEnd1        = self.blockFile.tell()
        if flags != 0:
          inputCount = self.readVarInt()
          for inputIndex in range(0, inputCount):
            self.readInput()
          outputCount = self.readVarInt()
          for outputIndex in range(0, outputCount):
            self.readOutput()
      else:
        cutStart1 = 0
        cutEnd1   = 0
        for inputIndex in range(0, inputCount):
          self.readInput()
        outputCount = self.readVarInt()
        for outputIndex in range(0, outputCount):
          self.readOutput()
      
      cutStart2 = 0
      cutEnd2   = 0
      if extendedFormat:
        if flags & 1:
          cutStart2 = self.blockFile.tell()
          for inputIndex in range(0, inputCount):
            countOfStackItems = self.readVarInt()
            witnesses         = []
            lastWitness       = b""
            for stackItemIndex in range(0, countOfStackItems):
              stackLength = self.readVarInt()
              stackItem   = self.blockFile.read(stackLength)[::-1]
              lastWitness = self.stringLittleEndianToBigEndian(stackItem)
              witnesses.append(self.stringLittleEndianToBigEndian(stackItem).decode('utf-8'))
            address    = self.publicKeyHashDecode(lastWitness)
            witness    = "".join(witnesses)
            inputIndex = -(inputCount-inputIndex)
            if self.saveInputFlag:
              self.inputDf.updateWitnesses(witness, inputIndex)
              if self.inputDf.notExistAddress(inputIndex):
                self.inputDf.updateAddress(address, inputIndex)
            if self.saveSqlAsCsvFlag or self.saveSqlAsCsvFlag or self.saveSqlAsMysqlFlag:
              inputAddressList[inputIndex] = address
          cutEnd2 = self.blockFile.tell()

    lockTime = self.hexToInt(self.readIntLittleEndian())
    endByte  = self.blockFile.tell()
    self.blockFile.seek(beginByte)
    lengthToRead = endByte - beginByte
    dataToHashForTransactionId = self.blockFile.read(lengthToRead)
    if extendedFormat and cutStart1 != 0 and cutEnd1 != 0 and cutStart2 != 0 and cutEnd2 != 0:
      dataToHashForTransactionId = dataToHashForTransactionId[:(cutStart1 - beginByte)] + dataToHashForTransactionId[(cutEnd1 - beginByte):(cutStart2 - beginByte)] + dataToHashForTransactionId[(cutEnd2 - beginByte):]
    elif extendedFormat:
      self.logger.info(cutStart1, cutEnd1, cutStart2, cutEnd2)
      quit()
    hashTransaction  = self.calcHashTransaction(dataToHashForTransactionId)

    if self.saveInputFlag:
      self.inputDf.addHashTransactionList(hashTransaction, inputCount)
    if self.saveOutputFlag:
      self.outputDf.addHashTransactionList(hashTransaction, outputCount)
    if self.saveTxFlag:
      totalInputValue  = 0  # FIXME ここで計算するのは面倒なため, 全てparseしたのちに計算する
      totalOutputValue = 0  # FIXME ここで計算するのは面倒なため, 全てparseしたのちに計算する
      fee              = 0  # FIXME ここで計算するのは面倒なため, 全てparseしたのちに計算する
      rawTx            = ""
      self.transactionDf.addList(self.blockHeight, self.blockFileNumber, hashTransaction, lockTime, version, inputCount, outputCount, totalInputValue, totalOutputValue, fee, rawTx)
    if self.saveSqlAsCsvFlag or self.saveSqlAsMysqlFlag or self.saveSqlAsSqliteFlag:
      for address in inputAddressList:
        self.sqlDf.addList(self.blockHeight,self.blockFileNumber,hashTransaction, address,0,creationTimeTimestamp)
      for address in outputAddressList:
        self.sqlDf.addList(self.blockHeight,self.blockFileNumber,hashTransaction, address,1,creationTimeTimestamp)

  def readWitness(self):
    pass

  
  def startsWithOpNCode(self, pub):
    try:
      intValue = int(pub[0:2], 16)
      if intValue >= 1 and intValue <= 75:
        return True
    except Exception as e:
      self.logger.info(e)
      pass
    return False
  
  
  def hash160(self, pub):
    h1 = hashlib.sha256(binascii.unhexlify(pub))
    h2 = hashlib.new('ripemd160', h1.digest())
    return h2.hexdigest()
  
  
  def calcCheckSum(self, byteString):
    h1 = hashlib.sha256(byteString)
    h2 = hashlib.sha256(h1.digest())
    return h2.digest()[:4]

  
  def convertToAddress(self, pub, addrHead):
    result = (addrHead) + binascii.unhexlify(pub)
    result += self.calcCheckSum(result)
    return base58.b58encode(result)

  
  def publicKeyHashDecode(self, pub):
      result = b""
      multisigHeaderList = (b"0014", b"0020", b"5121", b"5221")
      if pub == b"":  # NOTE　bech32はpublickeyがscriptsigの箇所にないためb""になる.
        return b""
      else:
        publicKeyHash = self.hash160(pub)
        # NOTE マルチシグの3から始まるアドレス処理
        if pub.lower().startswith(multisigHeaderList):
          return self.convertToAddress(publicKeyHash, b'\x05')
        # NOTE 通常の1からはじまるアドレス処理
        else:
          return self.convertToAddress(publicKeyHash, b'\x00')

  
  def publicKeyDecode(self, pub):
    pub = pub.decode()
    if pub.lower().startswith('76a914'):
      pub = pub[6:-4]
      return self.convertToAddress(pub, b'\x00')
    elif pub.lower().startswith('a9'):
      pub = pub[4:-2]
      return self.convertToAddress(pub, b'\x05')
    elif self.startsWithOpNCode(pub):
      pub = pub[2:-2]
      publicKeyHash = self.hash160(pub)
      return self.convertToAddress(publicKeyHash, b'\x00')
    return b""

  
  def stringLittleEndianToBigEndian(self, string):
    string = binascii.hexlify(string)
    n      = len(string) / 2
    fmt    = '%dh' % n
    return struct.pack(fmt, *reversed(struct.unpack(fmt, string)))

  def readShortLittleEndian(self):
    return struct.pack(">H", struct.unpack("<H", self.blockFile.read(2))[0])

  def readLongLittleEndian(self):
    return struct.pack(">Q", struct.unpack("<Q", self.blockFile.read(8))[0])

  def readIntLittleEndian(self):
    return struct.pack(">I", struct.unpack("<I", self.blockFile.read(4))[0])

  
  def hexToInt(self, value):
    return int(binascii.hexlify(value), 16)

  
  def hexToStr(self, value):
    return binascii.hexlify(value)

  def readVarInt(self):
    varInt = ord(self.blockFile.read(1))
    returnInt = 0
    if varInt < 0xfd:
      return varInt
    if varInt == 0xfd:
      returnInt = self.readShortLittleEndian()
    if varInt == 0xfe:
      returnInt = self.readIntLittleEndian()
    if varInt == 0xff:
      returnInt = self.readLongLittleEndian()
    return self.hexToInt(returnInt)

  def divideScriptSignatureRaw(self, scriptSignatureRaw):
    signature  = b""
    publicKey  = b""
    sigIndex   = 0
    scriptList = []
    while True:
      if len(scriptSignatureRaw) == 0:
        break
      elif scriptSignatureRaw[sigIndex:sigIndex+2].lower().startswith(b'4c'):  # NOTE この4cはmultisigのscriptSignatureに対して悪さをする. あった場合は読み飛ばす
        sigIndex += 2
        continue
      else:
        varInt = int(scriptSignatureRaw[sigIndex:sigIndex+2], 16) * 2
      sigIndex += varInt + 2
      scriptList.append(scriptSignatureRaw[sigIndex-varInt:sigIndex])
      if sigIndex >= len(scriptSignatureRaw):
        break
    if len(scriptList) == 1:
      if scriptList[0].lower().startswith(b"30"):
        signature = scriptList[0]
      else:
        publicKey = scriptList[0]
    elif len(scriptList) > 1:
      signature = b"".join(scriptList[:-1])
      publicKey = scriptList[-1]
    return signature, publicKey
  
  
  def convertToBtc(self, value):
    return value / 100000000.0

  
  def calcHashTransaction(self, byteString):
    firstHash        = hashlib.sha256(byteString)
    secondHash       = hashlib.sha256(firstHash.digest())
    hashLittleEndian = secondHash.hexdigest()
    hashTransaction  = self.stringLittleEndianToBigEndian(binascii.unhexlify(hashLittleEndian))
    return hashTransaction
