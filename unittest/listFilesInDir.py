import os
import sys

sys.path.append(os.path.abspath("../configuration"))
sys.path.append(os.path.abspath("../resources"))

import appConfig
import fileIO

inputDir = sys.argv[1]

fileIO.fileIO_writeToLog(appConfig.logFile, "File name\tFile size (GB)")

fileNameList = fileIO.fileIO_getFilesInDir(inputDir)

for fileName in fileNameList:
    fileSize = fileIO.fileIO_getFileSize(fileName, "GB")
    fileIO.fileIO_writeToLog(appConfig.logFile, "%s\t%.2f" %(fileName, fileSize))