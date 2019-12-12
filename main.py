import logging
from logging import getLogger, StreamHandler, Formatter, FileHandler
import tqdm
import os
import glob
import configparser
import shutil
from multiprocessing import Pool
from multiprocessing import Process

from lib.BlockParser       import BlockParser

cfg = configparser.ConfigParser()
cfg.read("./config/development.conf")

APP_NAME           = cfg["section1"]["APP_NAME"]
SKIP_NUMBER        = int(cfg["section1"]["SKIP_NUMBER"])
CHECK_PATH         = cfg["section1"]["CHECK_PATH"]
MAX_PROCESS_NUMBER = int(cfg["section1"]["MAX_PROCESS_NUMBER"])
BLOCK_DIR_PATH     = cfg["section1"]["BLOCK_DIR_PATH"]

def logSetting():
  logLevel = logging.DEBUG
  logger = getLogger(APP_NAME)
  logger.setLevel(logLevel)
  handler_format = Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
  stream_handler = StreamHandler()
  stream_handler.setLevel(logLevel)
  stream_handler.setFormatter(handler_format)
  logger.addHandler(stream_handler)
  file_handler = FileHandler("{}{}.log".format("./log/" , APP_NAME))
  file_handler.setLevel(logLevel)
  file_handler.setFormatter(handler_format)
  logger.addHandler(file_handler)
  return logger



def openBlockFilenames(dirPath):
    blockFilenames = glob.glob(dirPath)
    blockFilenames.sort()
    return blockFilenames

def diskCheck():
  disk = shutil.disk_usage(CHECK_PATH)
  lastDiskGb = disk[2]/1024/1024/1024
  print("disk size is ",lastDiskGb,"GB")
  if lastDiskGb < 50:
    print(lastDiskGb)
    exit()

def blockParse(blockFilePath):
  if blockFilePath == "./blocks/blk00000.dat":
    return 0
  blockParser = BlockParser(
    saveBlockFlag       = True,
    saveSqlAsSqliteFlag = True)
  blockParser.setBlockFileName(blockFilePath)
  blockParser.openBlockFile()
  blockParser.startBlockFileParse()
  blockParser.closeBlockFile()

def multi(fileList):
  p = Pool(MAX_PROCESS_NUMBER)
  p.map(blockParse, fileList)

def main():
  logger = logSetting()
  blockFilePathes = openBlockFilenames(BLOCK_DIR_PATH)
  fileNumber = int(len(blockFilePathes)/MAX_PROCESS_NUMBER)
  blockFilePathes = blockFilePathes[SKIP_NUMBER:]
  while len(blockFilePathes) != 0:
    tmpBlockFilePathes = []
    disk = shutil.disk_usage(CHECK_PATH)
    lastDiskGb = disk[2]/1024/1024/1024
    logger.info("last disk is "+str(lastDiskGb))
    if lastDiskGb < 10:
      print(lastDiskGb)
      exit()
    for i in range(MAX_PROCESS_NUMBER):
      if len(blockFilePathes) != 0:
        tmpBlockFilePathes.append(blockFilePathes.pop(0))
    multi(tmpBlockFilePathes)
  
if __name__ == "__main__":
  main()