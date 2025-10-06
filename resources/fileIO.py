import numpy
import os
import sys
import json
import pandas
import time
import platform
import shutil
from tqdm import tqdm

import timeUtils
import convertUnit

sys.path.append(os.path.abspath("../configuration"))

import appConfig

###############################################################################
def fileIO_writeToLog(fileName, message, printFlag = False):
    try:
        if (fileIO_fileExists(fileName) == False):
            f = open(fileName, "w")
        else:
            f = open(fileName, "a")
        timeStamp = timeUtils.timeUtils_timestamp(delimiter = "\t")
        f.write("%s\t%s\n" %(timeStamp, message))
        f.close()
        
        if (printFlag == True):
            print("%s\t%s" %(timeStamp, message))
    except:
        print("ERROR: fileIO_writeToLog. Unable to write message to log.")
###############################################################################

###############################################################################
def fileIO_writeToResourceLog(message, printFlag = False):
    try:
        f = open(appConfig.resourceMonitorFile, "a")
        timeStamp = timeUtils.timeUtils_timestamp(delimiter = "\t")
        f.write("%s\t%s\n" %(timeStamp, message))
        f.close()
        
        if (printFlag == True):
            print("%s\t%s" %(timeStamp, message))
    except:
        print("ERROR: fileIO_writeToResourceLog. Unable to write to resource monitor log.")
###############################################################################

###############################################################################
def fileIO_readJson(jsonFile):
    try:
        f = open(jsonFile, "r")
        data = json.load(f)
        return data
    except:
        fileIO_writeToLog(appConfig.logFile, "ERROR: fileIO_readJson. Unable to read json file %s." %(jsonFile), True)
###############################################################################

###############################################################################
def fileIO_readExcel(excelFile, sheetName, colNames):
    try:
        df = pandas.read_excel(excelFile, sheet_name = sheetName , names = colNames)
        df = df.dropna(axis = 0, how = "all")
        return df
    except:
        fileIO_writeToLog(appConfig.logFile, "ERROR: fileIO_readExcel. Unable to read excel file %s." %(excelFile), True)
###############################################################################

###############################################################################
def fileIO_fileExists(fileName):
    try:
        if (os.path.isfile(fileName)):
            return True
        else:
            fileIO_writeToLog(appConfig.logFile, "WARNING: fileIO_fileExists. File %s does not exist." %(fileName))
            return False
    except:
        fileIO_writeToLog(appConfig.logFile, "ERROR: fileIO_fileExists. Unable to check if file %s exists." %(fileName), True)
###############################################################################

###############################################################################
def fileIO_dirExists(dirName):
    try:
        if (os.path.exists(dirName)):
            return True
        else:
            fileIO_writeToLog(appConfig.logFile, "WARNING: fileIO_dirExists. Directory %s does not exist." %(dirName))
            return False
    except:
        fileIO_writeToLog(appConfig.logFile, "ERROR: fileIO_dirExists. Unable to check if directory %s exists." %(dirName), True)
###############################################################################

###############################################################################
def fileIO_isFile(fileName):
    try:
        if (os.path.isfile(fileName)):
            return True
        else:
            fileIO_writeToLog(appConfig.logFile, "WARNING: fileIO_isFile. %s is not a file." %(fileName))
            return False
    except:
        fileIO_writeToLog(appConfig.logFile, "ERROR: fileIO_isFile. Unable to check if %s is a file." %(fileName), True)
###############################################################################

###############################################################################
def fileIO_isDir(dirName):
    try:
        if (os.path.isdir(dirName)):
            return True
        else:
            fileIO_writeToLog(appConfig.logFile, "WARNING: fileIO_isDir. %s is not a directory." %(dirName))
            return False
    except:
        fileIO_writeToLog(appConfig.logFile, "ERROR: fileIO_isDir. Unable to check if %s is a directory." %(dirName), True)
###############################################################################

###############################################################################
def fileIO_getFileExtension(fileName):
    try:
        if ("." in fileName):
            extension = fileName.split(".")[-1]
        else:
            extension = "None"
        return extension
    except:
        fileIO_writeToLog(appConfig.logFile, "ERROR: fileIO_getFileExtension. Unable to get file extension.", True)
###############################################################################

###############################################################################
def fileIO_getFileName(fileNameWithPath):
    try:
        if (platform.system() == "Linux"):
            fileName = fileNameWithPath.split("/")[-1]
        elif (platform.system() == "Windows"):
            fileName = fileNameWithPath.split("\\")[-1]
            
        return fileName
    except:
        fileIO_writeToLog(appConfig.logFile, "ERROR: fileIO_getFileName. Unable to get file namee for %s." %(fileNameWithPath), True)
###############################################################################

###############################################################################
def fileIO_rmFile(fileName):
    try:
        if (fileIO_fileExists(fileName)):
            os.remove(fileName)
        else:
            fileIO_writeToLog(appConfig.logFile, "WARNING: fileIO_rmFile. %s does not exist" %(fileName))
    except:
        fileIO_writeToLog(appConfig.logFile, "ERROR: fileIO_rmFile. Unable to remove %s." %(fileName), True)
###############################################################################

###############################################################################
def fileIO_mkdir(dirName):
    try:
        if (fileIO_dirExists(dirName)):
            fileIO_writeToLog(appConfig.logFile, "WARNING: fileIO_mkdir. Directory %s already exists." %(dirName))
        else:
            os.makedirs(dirName)
    except:
        fileIO_writeToLog(appConfig.logFile, "ERROR: fileIO_mkdir. Unable to create directory %s." %(dirName), True)
###############################################################################

###############################################################################
def fileIO_rmDir(dirName):
    try:
        subdirList = []
        if (fileIO_dirExists(dirName)):
            for root, dirs, files in os.walk(dirName):
                for name in files:
                    fileName = os.path.join(root, name)
                    fileIO_rmFile(fileName)
                subdirList.append(root)
                
            for subdir in reversed(subdirList):
                os.rmdir(subdir)
                
            time.sleep(1)
            
            if (fileIO_dirExists(dirName) == True):
                fileIO_writeToLog(appConfig.logFile, "WARNING: fileIO_rmDir. Unable to completely delete directory %s." %(dirName))
        else:
            fileIO_writeToLog(appConfig.logFile, "WARNING: fileIO_rmDir. Directory %s does not exist." %(dirName))
    except:
        fileIO_writeToLog(appConfig.logFile, "ERROR: fileIO_rmDir. Unable to delete directory %s." %(dirName), True)
###############################################################################

###############################################################################
def fileIO_mvFile(source, destination):
    try:
        if (fileIO_fileExists(destination) == True):
            fileIO_writeToLog(appConfig.logFile, "WARNING: fileIO_mvFile. %s already exists. Deleting it." %(destination))
            fileIO_rmFile(destination)
        if (fileIO_fileExists(source) == True):
            os.rename(source, destination)
        else:
            fileIO_writeToLog(appConfig.logFile, "WARNING: fileIO_mvFile. %s does not exist." %(source))
    except:
        fileIO_writeToLog(appConfig.logFile, "ERROR: fileIO_mvFile. Unable to move %s to %s." %(source, destination), True)
###############################################################################

###############################################################################
def fileIO_cpFile(source, destination):
    try:
        if (fileIO_fileExists(destination) == True):
            fileIO_writeToLog(appConfig.logFile, "WARNING: fileIO_cpFile. %s already exists. Deleting it." %(destination))
            fileIO_rmFile(destination)
        if (fileIO_fileExists(source) == True):
            shutil.copy(source, destination)
        else:
            fileIO_writeToLog(appConfig.logFile, "WARNING: fileIO_cpFile. %s does not exist." %(source))
    except:
        fileIO_writeToLog(appConfig.logFile, "ERROR: fileIO_cpFile. Unable to copy %s to %s." %(source, destination), True)
###############################################################################

###############################################################################
def fileIO_getFilesInDir(dirName):
    try:
        fileNameList = []
        for root, dirs, files in os.walk(dirName):
            for name in files:
                fileName  = os.path.join(root, name)
                fileNameList.append(fileName)
                
        return fileNameList
    except:
        fileIO_writeToLog(appConfig.logFile, "ERROR: fileIO_getFilesInDir. %s failed." %(dirName), True)
        return None
###############################################################################

###############################################################################
def fileIO_getSubDirinDir(dirName):
    subDirNameList = []
    for root, dirs, files in os.walk(dirName):
        for dir in dirs:
            subDirName = os.path.join(root, dir)
            subDirNameList.append(subDirName)
            
    return subDirNameList
###############################################################################

###############################################################################
def fileIO_selectFilesInDir(dirName, contents):
    try:
        fileNameList = []
        for root, dirs, files in os.walk(dirName):
            for name in files:
                for content in contents:
                    if (content in name):
                        fileName  = os.path.join(root, name)
                        fileNameList.append(fileName)
                        
        return fileNameList
    except:
        fileIO_writeToLog(appConfig.logFile, "ERROR: fileIO_selectFilesInDir. %s failed." %(dirName), True)
        return None
###############################################################################

###############################################################################
def fileIO_getFileSize(fileName, units):
    try:
        if (fileIO_fileExists(fileName) == True):
            fileSize = os.path.getsize(fileName)
        else:
            fileIO_writeToLog(appConfig.logFile, "WARNING: fileIO_getFileSize. %s does not exist. Setting its size to 0." %(fileName))
            fileSize = 0
            
        if (units == "bytes"):
            pass
        elif (units == "GB"):
            fileSize = convertUnit.convert_byte2gigabyte(fileSize)
            
        return fileSize
    except:
        fileIO_writeToLog(appConfig.logFile, "ERROR: fileIO_getFileSize. Unable to calculate size of file %s." %(fileName), True)
###############################################################################

###############################################################################
def fileIO_splitFile(fileName, fileSizeLimit, fileSplitChunckSize):
    try:
        fileSize            = fileIO_getFileSize(fileName, "bytes")
        fileSizeLimit       = int(fileSizeLimit)
        fileSplitChunckSize = int(fileSplitChunckSize)
        
        if (fileSize % fileSizeLimit == 0):
            numFiles = int(fileSize / fileSizeLimit)
        else:
            numFiles = int(fileSize / fileSizeLimit) + 1
        
        if (numFiles <= 1):
            fileIO_writeToLog(appConfig.logFile, "WARNING: fileIO_splitFile. Size of file %s is %d, which is smaller than largest allowed file size (%d bytes). Unable to split." %(fileName, fileSize, fileSizeLimit))
            successStatus = True
        else:
            fileIO_writeToLog(appConfig.logFile, "fileIO_splitFile. Splitting %s into %d parts." %(fileName, numFiles), True)
            
            numChunksToRead      = int(numpy.ceil(fileSize / fileSplitChunckSize))
            numChunksInSplitFile = int(fileSizeLimit / fileSplitChunckSize)
            splitNum             = 1
            chunkNum             = 0
            
            infile  = open(fileName, "rb")
            outFile = open(fileName + "_split_" + str(splitNum).zfill(4), "wb")
            
            for i in tqdm(range(numChunksToRead)):
                chunk = infile.read(fileSplitChunckSize)
                chunkNum = chunkNum + 1
                if (chunkNum <= numChunksInSplitFile):
                    outFile.write(chunk)
                else:
                    fileIO_writeToLog(appConfig.logFile, "fileIO_splitFile. Split %d complete." %(splitNum), True)
                    
                    outFile.close()
                    
                    splitNum = splitNum + 1
                    outFile  = open(fileName + "_split_" + str(splitNum).zfill(4), "wb")
                    outFile.write(chunk)
                    chunkNum = 1
                    
            fileIO_writeToLog(appConfig.logFile, "fileIO_splitFile. Split %d complete." %(numFiles), True)
            
            outFile.close()
            infile.close()
            
            fileIO_rmFile(fileName)
            successStatus = True
    except:
        fileIO_writeToLog(appConfig.logFile, "ERROR: fileIO_splitFile. Unable to split file %s." %(fileName), True)
        successStatus = False
        
    return successStatus
###############################################################################

###############################################################################
def fileIO_getFileDirectory(fileNameWithPath):
    try:
        fileDir = ""
        
        if (platform.system() == "Linux"):
            tempList = fileNameWithPath.split("/")
            for temp in tempList[:-1]:
                fileDir = fileDir + temp + "/"
                
        elif (platform.system() == "Windows"):
            tempList = fileNameWithPath.split("\\")
            for temp in tempList[:-1]:
                fileDir = fileDir + temp + "\\"
                
        fileIO_writeToLog(appConfig.logFile, "fileIO_getFileDirectory. Parent directory of file %s is %s." %(fileNameWithPath, fileDir))
        return fileDir
    except:
        fileIO_writeToLog(appConfig.logFile, "ERROR: fileIO_getFileDirectory. Unable to get parent directory of file %s." %(fileNameWithPath), True)
###############################################################################

############################################################
def fileIO_findSplitFilesGroup(firstSplitFile):
    try:
        path = fileIO_getFileDirectory(firstSplitFile)
        
        fileName      = firstSplitFile.split("_split_0001")[0] + "_split_"
        dirFileList   = os.listdir(path)
        splitFileGroupList = []
        
        for dirFile in dirFileList:
            if (fileName in (path + dirFile)):
                splitFileGroupList.append(path + dirFile)
        splitFileGroupList = list(numpy.sort(splitFileGroupList, kind = "mergesort"))
        
        string = ""
        for splitFileGroup in splitFileGroupList:
            string = string + splitFileGroup + " "
            
        fileIO_writeToLog(appConfig.logFile, "fileIO_findSplitFilesGroup. Split file %s is in the group %s." %(firstSplitFile, string))
        return splitFileGroupList
    except:
        fileIO_writeToLog(appConfig.logFile, "ERROR: fileIO_findSplitFilesGroup. Unable to find split file group for %s." %(firstSplitFile), True)
############################################################

############################################################
def fileIO_joinFiles(splitFileNameList, joinFileName, fileSplitChunckSize):
    try:
        fileSplitChunckSize = int(fileSplitChunckSize)
        
        outFile = open(joinFileName, "wb")
        for splitFileName in splitFileNameList:
            fileIO_writeToLog(appConfig.logFile, "fileIO_joinFiles. Writing %s to %s." %(splitFileName, joinFileName), True)
            inFile = open(splitFileName, "rb")
            while (1):
                chunk = inFile.read(fileSplitChunckSize)
                if not chunk:
                    break
                outFile.write(chunk)
            inFile.close()
        outFile.close()
        
        for splitFileName in splitFileNameList:
            fileIO_rmFile(splitFileName)
            
    except:
        fileIO_writeToLog(appConfig.logFile, "ERROR: fileIO_joinFiles. Unable to join split file %s." %(joinFileName), True)
############################################################