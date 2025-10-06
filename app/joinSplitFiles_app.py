import os
import sys

sys.path.append(os.path.abspath("../configuration"))
sys.path.append(os.path.abspath("../resources"))
sys.path.append(os.path.abspath("../structures"))

import appConfig
import structDef
import fileIO
import convertUnit


######## CREATING THE LOG DIRECTORY
fileIO.fileIO_mkdir(appConfig.logDir)


######## READING THE DATA COMPRESSION CONFIGURATION
data = fileIO.fileIO_readJson("../configuration/dataCompressConfig.json")
DATA_COMPRESS_STRUCT = structDef.structDef_dataCompressInit(data, None, 100)


######## READING THE DATA COMPRESSION CONFIGURATION
inputData = sys.argv[1]
if (fileIO.fileIO_fileExists(inputData)):
    splitFileList = [inputData]
elif (fileIO.fileIO_dirExists(inputData)):
    splitFileList = fileIO.fileIO_selectFilesInDir(inputData, ["_split_0001"])
    
    
######## JOINING ALL THE SPLIT FILES
for splitFile in splitFileList:
    joinFile       = splitFile.split("_split_0001")[0]
    splitFileGroup = fileIO.fileIO_findSplitFilesGroup(splitFile)
    fileIO.fileIO_joinFiles(splitFileGroup, joinFile, convertUnit.convert_megabyte2byte(DATA_COMPRESS_STRUCT.fileSplitChunckSizeMB))
