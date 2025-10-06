import os
import sys
import numpy
import dropbox
import platform
import time

sys.path.append(os.path.abspath("../configuration"))

import appConfig
import fileIO
import convertUnit

###############################################################################
def dbxAPI_connectToDBX(DROPBOX_BATCH_UPLOAD_STRUCT):
    try:
        fileIO.fileIO_writeToLog(appConfig.logFile, "dbxAPI_connectToDBX. Establish connection to dropbox storage server.", True)
        DROPBOX_BATCH_UPLOAD_STRUCT.dbx = dropbox.Dropbox(DROPBOX_BATCH_UPLOAD_STRUCT.dbxAccessToken)
    except:
        fileIO.fileIO_writeToLog(appConfig.logFile, "ERROR: dbxAPI_connectToDBX. Unable to establish connection to dropbox storage server.", True)
###############################################################################

###############################################################################
def dbxAPI_filesToUpload(DROPBOX_BATCH_UPLOAD_STRUCT):
    try:
        [row, col] = DROPBOX_BATCH_UPLOAD_STRUCT.df.shape
        
        DROPBOX_BATCH_UPLOAD_STRUCT.fileNameList          = []
        DROPBOX_BATCH_UPLOAD_STRUCT.fileSizeList          = []
        DROPBOX_BATCH_UPLOAD_STRUCT.localDbxFileNameList  = []
        DROPBOX_BATCH_UPLOAD_STRUCT.localDbxDirNameList   = []
        DROPBOX_BATCH_UPLOAD_STRUCT.remoteDbxFileNameList = []
        
        for i in range(row):
            
            try:
                inputDir    = DROPBOX_BATCH_UPLOAD_STRUCT.df["inputDir"][i]
                localDbxDir = DROPBOX_BATCH_UPLOAD_STRUCT.df["localDropboxDir"][i]
            except:
                inputDir    = DROPBOX_BATCH_UPLOAD_STRUCT.df["inputDir"]
                localDbxDir = DROPBOX_BATCH_UPLOAD_STRUCT.df["localDropboxDir"]
                
            if (fileIO.fileIO_isFile(inputDir) == True):
                fileName          = fileIO.fileIO_getFileName(inputDir)
                fileSize          = fileIO.fileIO_getFileSize(inputDir, "bytes")
                localDbxFileName  = localDbxDir + "/" + fileName
                localDbxDirName   = localDbxDir
                remoteDbxFileName = dbxAPI_getRemoteDbxFileName(localDbxFileName, DROPBOX_BATCH_UPLOAD_STRUCT.dbxDir)
                
                DROPBOX_BATCH_UPLOAD_STRUCT.fileNameList.append(inputDir)
                DROPBOX_BATCH_UPLOAD_STRUCT.fileSizeList.append(fileSize)
                DROPBOX_BATCH_UPLOAD_STRUCT.localDbxFileNameList.append(localDbxFileName)
                DROPBOX_BATCH_UPLOAD_STRUCT.localDbxDirNameList.append(localDbxDirName)
                DROPBOX_BATCH_UPLOAD_STRUCT.remoteDbxFileNameList.append(remoteDbxFileName)
                
            else:
                pathFileNameList      = fileIO.fileIO_getFilesInDir(inputDir)
                
                for pathFileName in pathFileNameList:
                    fileName          = fileIO.fileIO_getFileName(pathFileName)
                    fileCompatibleFlag = dbxAPI_fileCompatible(DROPBOX_BATCH_UPLOAD_STRUCT, fileName)
                    
                    if (fileCompatibleFlag == True):
                        fileSize          = fileIO.fileIO_getFileSize(pathFileName, "bytes")
                        localDbxFileName  = dbxAPI_getLocalDbxFileName(pathFileName, inputDir, localDbxDir)
                        localDbxDirName   = dbxAPI_getLocalDbxDirName(fileName, localDbxFileName)
                        remoteDbxFileName = dbxAPI_getRemoteDbxFileName(localDbxFileName, DROPBOX_BATCH_UPLOAD_STRUCT.dbxDir)
                        
                        DROPBOX_BATCH_UPLOAD_STRUCT.fileNameList.append(pathFileName)
                        DROPBOX_BATCH_UPLOAD_STRUCT.fileSizeList.append(fileSize)
                        DROPBOX_BATCH_UPLOAD_STRUCT.localDbxFileNameList.append(localDbxFileName)
                        DROPBOX_BATCH_UPLOAD_STRUCT.localDbxDirNameList.append(localDbxDirName)
                        DROPBOX_BATCH_UPLOAD_STRUCT.remoteDbxFileNameList.append(remoteDbxFileName)
                    else:
                        fileIO.fileIO_writeToLog(appConfig.logFile, "BEWARE: %s cannot be uploaded to Dropbox." %(pathFileName))
                    
        fileIO.fileIO_writeToLog(appConfig.logFile, "File\tSize (bytes)\tDropbox file\tDropbox dir\tRemote dropbox file")
        for fileName, fileSize, dbxFileName, dbxDirName, remoteDbxFileName in zip(DROPBOX_BATCH_UPLOAD_STRUCT.fileNameList, DROPBOX_BATCH_UPLOAD_STRUCT.fileSizeList, DROPBOX_BATCH_UPLOAD_STRUCT.localDbxFileNameList, DROPBOX_BATCH_UPLOAD_STRUCT.localDbxDirNameList, DROPBOX_BATCH_UPLOAD_STRUCT.remoteDbxFileNameList):
            fileIO.fileIO_writeToLog(appConfig.logFile, "%s\t%d\t%s\t%s\t%s" %(fileName, fileSize, dbxFileName, dbxDirName, remoteDbxFileName))
            
        fileIO.fileIO_writeToLog(appConfig.logFile, "dbxAPI_filesToUpload. Listing of files to upload successful.", True)
        
    except:
        fileIO.fileIO_writeToLog(appConfig.logFile, "ERROR: dbxAPI_filesToUpload. Listing of files to upload failed.", True)
###############################################################################

###############################################################################
def dbxAPI_fileCompatible(DROPBOX_BATCH_UPLOAD_STRUCT, fileName):
    try:
        compatibleFlag = True
        
        for excludeName in DROPBOX_BATCH_UPLOAD_STRUCT.dbxExcludeFileNameList:
            if (fileName == excludeName):
                compatibleFlag = False
                
        for token in DROPBOX_BATCH_UPLOAD_STRUCT.dbxExcludeFileContainList:
            if (token in fileName):
                compatibleFlag = False
                
        return compatibleFlag
    except:
        fileIO.fileIO_writeToLog(appConfig.logFile, "ERROR: dbxAPI_fileCompatible. Unable to check compatibility of %s." %(fileName), True)
        return True
###############################################################################

###############################################################################
def dbxAPI_getLocalDbxFileName(fileName, inputDir, localDbxDir):
    try:
        localDbxFileName = fileName.replace(inputDir, localDbxDir)
        
        return localDbxFileName
    except:
        fileIO.fileIO_writeToLog(appConfig.logFile, "ERROR: dbxAPI_getLocalDbxFileName. Unable to get local Dropbox file name for %s." %(fileName), True)
###############################################################################

###############################################################################
def dbxAPI_getLocalDbxDirName(fileName, localDbxFile):
    try:
        if (platform.system() == "Windows"):
            localDbxDir = localDbxFile.split("\\" + fileName)[0]
        elif (platform.system() == "Linux"):
            localDbxDir = localDbxFile.split("/" + fileName)[0]
            
        return localDbxDir
    except:
        fileIO.fileIO_writeToLog(appConfig.logFile, "ERROR: dbxAPI_getLocalDbxDirName. Unable to get local Dropbox directory for %s." %(localDbxFile), True)
###############################################################################

###############################################################################
def dbxAPI_getRemoteDbxFileName(localDbxFileName, localDbxBaseDir):
    try:
        remoteDbxFileName = localDbxFileName.replace(localDbxBaseDir, "")
        
        if (platform.system() == "Windows"):
            remoteDbxFileName = remoteDbxFileName.replace("\\", "/")
            
        return remoteDbxFileName
    except:
        fileIO.fileIO_writeToLog(appConfig.logFile, "ERROR: dbxAPI_getRemoteDbxFileName. Unable to get remote file name for %s." %(localDbxFileName), True)
###############################################################################

###############################################################################
def dbxAPI_mkdirs(DROPBOX_BATCH_UPLOAD_STRUCT):
    try:
        ##### CREATE LOCAL DROPBOX BASE DIRECTORY
        [row, col] = DROPBOX_BATCH_UPLOAD_STRUCT.df.shape
        
        for i in range(row):
            
            try:
                localDbxDir = DROPBOX_BATCH_UPLOAD_STRUCT.df["localDropboxDir"][i]
            except:
                localDbxDir = DROPBOX_BATCH_UPLOAD_STRUCT.df["localDropboxDir"]
                
            try:
                fileIO.fileIO_mkdir(localDbxDir)
            except:
                fileIO.fileIO_writeToLog(appConfig.logFile, "WARNING: dbxAPI_mkdir. Unable to create local Dropbox directory %s." %(localDbxDir))
                
        ##### CREATE LOCAL DROPBOX RECURSIVE DIRECTORIES
        localDbxDirList = numpy.unique(DROPBOX_BATCH_UPLOAD_STRUCT.localDbxDirNameList)
        
        for localDbxDir in localDbxDirList:
            try:
                fileIO.fileIO_mkdir(localDbxDir)
            except:
                fileIO.fileIO_writeToLog(appConfig.logFile, "WARNING: dbxAPI_mkdir. Unable to create local Dropbox directorie %s." %(localDbxDir))
    except:
        fileIO.fileIO_writeToLog(appConfig.logFile, "ERROR: dbxAPI_mkdir. Unable to create local Dropbox directories.", True)
###############################################################################

###############################################################################
def dbxAPI_filesToUpload_Batch(DROPBOX_BATCH_UPLOAD_STRUCT):
    try:
        DROPBOX_BATCH_UPLOAD_STRUCT.fileNameListBatch          = []
        DROPBOX_BATCH_UPLOAD_STRUCT.localDbxFileNameListBatch  = []
        DROPBOX_BATCH_UPLOAD_STRUCT.remoteDbxFileNameListBatch = []
        
        fileListBatch = []
        dbxFileListBatch = []
        remoteDbxFileListBatch = []
        batchSize = 0
        
        for i in range(len(DROPBOX_BATCH_UPLOAD_STRUCT.fileNameList)):
            fileName          = DROPBOX_BATCH_UPLOAD_STRUCT.fileNameList[i]
            fileSize          = DROPBOX_BATCH_UPLOAD_STRUCT.fileSizeList[i]
            dbxFileName       = DROPBOX_BATCH_UPLOAD_STRUCT.localDbxFileNameList[i]
            remoteDbxFileName = DROPBOX_BATCH_UPLOAD_STRUCT.remoteDbxFileNameList[i]
            
            batchSize = batchSize + fileSize
            
            if (batchSize <= convertUnit.convert_gigabyte2byte(DROPBOX_BATCH_UPLOAD_STRUCT.dbxBatchSizeGB)):
                fileListBatch.append(fileName)
                dbxFileListBatch.append(dbxFileName)
                remoteDbxFileListBatch.append(remoteDbxFileName)
                
            elif (batchSize > convertUnit.convert_gigabyte2byte(DROPBOX_BATCH_UPLOAD_STRUCT.dbxBatchSizeGB)):
                DROPBOX_BATCH_UPLOAD_STRUCT.fileNameListBatch.append(fileListBatch)
                DROPBOX_BATCH_UPLOAD_STRUCT.localDbxFileNameListBatch.append(dbxFileListBatch)
                DROPBOX_BATCH_UPLOAD_STRUCT.remoteDbxFileNameListBatch.append(remoteDbxFileListBatch)
                
                fileListBatch = [fileName]
                dbxFileListBatch = [dbxFileName]
                remoteDbxFileListBatch = [remoteDbxFileName]
                batchSize = fileSize
                
            if (i == len(DROPBOX_BATCH_UPLOAD_STRUCT.fileNameList) - 1):
                DROPBOX_BATCH_UPLOAD_STRUCT.fileNameListBatch.append(fileListBatch)
                DROPBOX_BATCH_UPLOAD_STRUCT.localDbxFileNameListBatch.append(dbxFileListBatch)
                DROPBOX_BATCH_UPLOAD_STRUCT.remoteDbxFileNameListBatch.append(remoteDbxFileListBatch)
                
        DROPBOX_BATCH_UPLOAD_STRUCT.numBatches = len(DROPBOX_BATCH_UPLOAD_STRUCT.fileNameListBatch)
        
        fileIO.fileIO_writeToLog(appConfig.logFile, "Batch number\tFile\tDropbox file\tRemote dropbox file")
        for i in range(DROPBOX_BATCH_UPLOAD_STRUCT.numBatches):
            for fileName, dbxFileName, remoteDbxFileName in zip(DROPBOX_BATCH_UPLOAD_STRUCT.fileNameListBatch[i], DROPBOX_BATCH_UPLOAD_STRUCT.localDbxFileNameListBatch[i], DROPBOX_BATCH_UPLOAD_STRUCT.remoteDbxFileNameListBatch[i]):
                fileIO.fileIO_writeToLog(appConfig.logFile, "%d\t%s\t%s\t%s" %(i, fileName, dbxFileName, remoteDbxFileName))
                
        fileIO.fileIO_writeToLog(appConfig.logFile, "dbxAPI_filesToUploadBatch. Successfully divided the files into %d batches." %(DROPBOX_BATCH_UPLOAD_STRUCT.numBatches), True)
    except:
        fileIO.fileIO_writeToLog(appConfig.logFile, "ERROR: dbxAPI_filesToUploadBatch. Unable to divide files into batches.", True)
###############################################################################

###############################################################################
def dbxAPI_uploadToDropbox_Batches(DROPBOX_BATCH_UPLOAD_STRUCT):
    for i in range(DROPBOX_BATCH_UPLOAD_STRUCT.numBatches):
        try:
            fileIO.fileIO_writeToLog(appConfig.logFile, "dbxAPI_uploadToDropbox_Batches. Start copying files from batch number %d to Dropbox." %(i), True)
            dbxAPI_copyToDropbox_Batch(DROPBOX_BATCH_UPLOAD_STRUCT, i)
            fileIO.fileIO_writeToLog(appConfig.logFile, "dbxAPI_uploadToDropbox_Batches. Finish copying files from batch number %d to Dropbox." %(i), True)
            while (True):
                time.sleep(convertUnit.convert_min2s(DROPBOX_BATCH_UPLOAD_STRUCT.dbxStatusCheckTimeMin))
                fileIO.fileIO_writeToLog(appConfig.logFile, "dbxAPI_uploadToDropbox_Batches. Checking the upload status of batch number %d to Dropbox." %(i))
                if (dbxAPI_uploadComplete_Batch(DROPBOX_BATCH_UPLOAD_STRUCT) == True):
                    fileIO.fileIO_writeToLog(appConfig.logFile, "dbxAPI_uploadToDropbox_Batches. Successfully uploaded batch number %d to Dropbox." %(i), True)
                    dbxAPI_deleteFiles_Batch(DROPBOX_BATCH_UPLOAD_STRUCT, i)
                    break
        except:
            fileIO.fileIO_writeToLog(appConfig.logFile, "ERROR: dbxAPI_uploadToDropbox_Batches. Unable to upload batch number %d to Dropbox." %(i), True)
###############################################################################

###############################################################################
def dbxAPI_copyToDropbox_Batch(DROPBOX_BATCH_UPLOAD_STRUCT, batchNum):
    try:
        fileNameList          = DROPBOX_BATCH_UPLOAD_STRUCT.fileNameListBatch[batchNum]
        dbxFileNameList       = DROPBOX_BATCH_UPLOAD_STRUCT.localDbxFileNameListBatch[batchNum]
        remoteDbxFileNameList = DROPBOX_BATCH_UPLOAD_STRUCT.remoteDbxFileNameListBatch[batchNum]
        
        DROPBOX_BATCH_UPLOAD_STRUCT.remainingFilesInBatch_local  = fileNameList.copy()
        DROPBOX_BATCH_UPLOAD_STRUCT.remainingFilesInBatch_remote = remoteDbxFileNameList.copy()
        
        for fileName, dbxFileName, remoteDbxFileName in zip(fileNameList, dbxFileNameList, remoteDbxFileNameList):
            fileIO.fileIO_writeToLog(appConfig.logFile, "dbxAPI_uploadToDropbox_Batch. Copying %s to %s." %(fileName, dbxFileName), True)
            dbxAPI_copyToDropbox_File(DROPBOX_BATCH_UPLOAD_STRUCT, fileName, dbxFileName, remoteDbxFileName)
    except:
        fileIO.fileIO_writeToLog(appConfig.logFile, "ERROR: dbxAPI_uploadToDropbox_Batch. Unable to upload batch number %d to Dropbox." %(batchNum), True)
###############################################################################

###############################################################################
def dbxAPI_copyToDropbox_File(DROPBOX_BATCH_UPLOAD_STRUCT, fileName, dbxFileName, remoteDbxFileName):
    try:
        if (dbxAPI_remoteFileExists(DROPBOX_BATCH_UPLOAD_STRUCT, fileName, remoteDbxFileName) == True):
            fileIO.fileIO_writeToLog(appConfig.logFile, "WARNING: dbxAPI_uploadToDropbox_File. File %s already exists on Dropbox." %(fileName))
        else:
            fileIO.fileIO_cpFile(fileName, dbxFileName)
    except:
        fileIO.fileIO_writeToLog(appConfig.logFile, "WARNING: dbxAPI_uploadToDropbox_File. Unable to check if file %s exists on Dropbox. Copying the file by default." %(fileName))
        fileIO.fileIO_cpFile(fileName, dbxFileName)
###############################################################################

###############################################################################
def dbxAPI_remoteFileExists(DROPBOX_BATCH_UPLOAD_STRUCT, localFile, remoteFile):
    try:
        localFileSize  = fileIO.fileIO_getFileSize(localFile, "bytes")
        remoteFileSize = DROPBOX_BATCH_UPLOAD_STRUCT.dbx.files_get_metadata(remoteFile).size
        
        if (localFileSize == remoteFileSize):
            fileIO.fileIO_writeToLog(appConfig.logFile, "dbxAPI_remoteFileExists. %s successfully synced to Dropbox." %(localFile), True)
            return True
        else:
            fileIO.fileIO_writeToLog(appConfig.logFile, "dbxAPI_remoteFileExists. %s is syncing to Dropbox." %(localFile), True)
            return False
    except:
        fileIO.fileIO_writeToLog(appConfig.logFile, "WARNING: dbxAPI_remoteFileExists. Unable to check if %s is synced to Dropbox." %(localFile))
        return False
###############################################################################

###############################################################################
def dbxAPI_deleteFiles_Batch(DROPBOX_BATCH_UPLOAD_STRUCT, batchNum):
    fileNameList = DROPBOX_BATCH_UPLOAD_STRUCT.fileNameListBatch[batchNum]
    for fileName in fileNameList:
        try:
            fileIO.fileIO_writeToLog(appConfig.logFile, "dbxAPI_deleteFiles_Batch. Deleting %s from batch %d." %(fileName, batchNum))
            fileIO.fileIO_rmFile(fileName)
        except:
            fileIO.fileIO_writeToLog(appConfig.logFile, "ERROR: dbxAPI_deleteFiles_Batch. Unable to delete file %s in batch %d." %(fileName, batchNum), True)
###############################################################################

###############################################################################
def dbxAPI_uploadComplete_Batch(DROPBOX_BATCH_UPLOAD_STRUCT):
    try:
        for localFile, remoteFile in zip(DROPBOX_BATCH_UPLOAD_STRUCT.remainingFilesInBatch_local, DROPBOX_BATCH_UPLOAD_STRUCT.remainingFilesInBatch_remote):
            if (dbxAPI_remoteFileExists(DROPBOX_BATCH_UPLOAD_STRUCT, localFile, remoteFile) == True):
                DROPBOX_BATCH_UPLOAD_STRUCT.remainingFilesInBatch_local.remove(localFile)
                DROPBOX_BATCH_UPLOAD_STRUCT.remainingFilesInBatch_remote.remove(remoteFile)
            else:
                fileIO.fileIO_writeToLog(appConfig.logFile, "dbxAPI_uploadComplete_Batch. Uploading %s to Dropbox." %(localFile))
                return False
                
        if (len(DROPBOX_BATCH_UPLOAD_STRUCT.remainingFilesInBatch_local) == 0):
            return True
        else:
            return False
    except:
        fileIO.fileIO_writeToLog(appConfig.logFile, "ERROR: dbxAPI_uploadComplete_Batch. Unable to check if batch upload is complete.", True)
        return False
###############################################################################