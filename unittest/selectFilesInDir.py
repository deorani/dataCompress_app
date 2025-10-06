import os
import sys

sys.path.append(os.path.abspath("../configuration"))
sys.path.append(os.path.abspath("../resources"))

import appConfig
import fileIO

inputDir = sys.argv[1]
fileExtension = sys.argv[2]

fileNameList = fileIO.fileIO_selectFilesInDir(inputDir, [fileExtension])

for fileName in fileNameList:
    fileIO.fileIO_writeToLog(appConfig.logFile, fileName)
    