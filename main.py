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
import shutil
from multiprocessing import Pool
from multiprocessing import Process

from lib.BlockParser       import BlockParser

cfg = configparser.ConfigParser()
cfg.read("./config/development.conf")

CHECK_PATH = "/Users/igakishuusei/Desktop/BitcoinBlockReader"
MAX_PROCESS_NUMBER            = int(cfg["section1"]["MAX_PROCESS_NUMBER"])
BLOCK_DIR_PATH                = cfg["section1"]["BLOCK_DIR_PATH"]

def openBlockFilenames(dirPath):
    blockFilenames = glob.glob(dirPath)
    blockFilenames.sort()
    return blockFilenames

def blockParse(blockFilePath):
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
  blockFilePathes = openBlockFilenames(BLOCK_DIR_PATH)
  fileNumber = len(blockFilePathes)
  while len(blockFilePathes) != 0:
    tmpBlockFilePathes = []
    disk = shutil.disk_usage(CHECK_PATH)
    lastDiskGb = disk[2]/1024/1024/1024
    if lastDiskGb < 10:
      print(lastDiskGb)
      exit()
    for i in range(MAX_PROCESS_NUMBER):
      if len(blockFilePathes) != 0:
        tmpBlockFilePathes.append(blockFilePathes.pop(0))
    multi(tmpBlockFilePathes)
  
if __name__ == "__main__":
  main()