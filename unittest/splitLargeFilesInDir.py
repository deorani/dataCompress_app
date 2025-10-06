import os
import sys

sys.path.append(os.path.abspath("../configuration"))
sys.path.append(os.path.abspath("../resources"))

import appConfig
import fileIO
import convertUnit

inputDir            = sys.argv[1]
fileSizeLimit       = convertUnit.convert_gigabyte2byte(int(sys.argv[2]))
fileSplitChunckSize = convertUnit.convert_megabyte2byte(1024)

fileNameList = fileIO.fileIO_getFilesInDir(inputDir)

for fileName in fileNameList:
    fileIO.fileIO_splitFile(fileName, fileSizeLimit, fileSplitChunckSize)
    