import os
import sys
import numpy
import pandas
import platform
import re
import subprocess
import shutil
import threading

import fileIO
import convertUnit


sys.path.append(os.path.abspath("../configuration"))

import appConfig

###############################################################################
def dProc_7zip(DATA_COMPRESS_STRUCT):
    df = pandas.read_csv(os.path.join(appConfig.logDir, "directoryToCompress.txt"), delimiter = "\t", 
                        names = ["dirName", "numSubDir", "numFiles", "size"], header = 0)
    [row, col] = df.shape
    
    f = open(os.path.join(appConfig.logDir, "directoryToCompressStatus.txt"), "w")
    f.write("Directory\tStatus\n")
    
    fileIO.fileIO_writeToLog(appConfig.logFile, message = "COMPRESSING LARGE DIRECTORIES USING 7-ZIP")
    fileIO.fileIO_writeToLog(appConfig.logFile, message = "Directory to zip\tNumber of subdirectories\tNumber of files\tDirectory size (GB)")
    
    if (platform.system() == "Linux"):
        executable = DATA_COMPRESS_STRUCT.linux_7zip
    elif (platform.system() == "Windows"):
        executable = DATA_COMPRESS_STRUCT.windows_7zip
        
    for i in range(row):
        try:
            inputDir = df["dirName"][i]
        except Exception as e:
            print('-'*50)
            print("IMPORTANT NOTICE \nThis is unexpected! inform Praveen")
            print('-'*50)
            print(i, df.shape)
            print(df)
            print('-'*50)
            print(e)
            print('-'*50)
            inputDir = df["dirName"]
            
        zipFileName = inputDir + ".zip"
        ## check permissions
        parentDir = os.path.dirname(zipFileName)
        if not os.access(parentDir, os.W_OK):
            statusMessage = f"ERROR: Can not create {zipFileName}. No write permisssion in {parentDir}"
            f.write("%s\tFalse\n" %(inputDir))
            fileIO.fileIO_writeToLog(appConfig.logFile, statusMessage)
            continue
        
        numSubDir   = df[df["dirName"] == inputDir]["numSubDir"].iloc[0]
        numFiles    = df[df["dirName"] == inputDir]["numFiles"].iloc[0]
        size        = df[df["dirName"] == inputDir]["size"].iloc[0]
        
        fileIO.fileIO_writeToLog(appConfig.logFile, message = "%s\t%d\t%d\t%.6f" %(inputDir, numSubDir, numFiles, size))
        try:
            subprocess.run([executable, "a", zipFileName, inputDir, "-sdel"], check=True)
            statusMessage = "dProc_7zip successful. Compressed %s." %(inputDir)
            fileIO.fileIO_writeToLog(appConfig.logFile, statusMessage)
            # threading.Thread(target=shutil.rmtree, args=(inputDir,)).start()
            f.write("%s\tTrue\n" %(inputDir))
        except Exception as e:
            statusMessage = "ERROR: dProc_7zip failed. Unable to compress %s." %(inputDir)
            f.write("%s\tFalse\n" %(inputDir))
            fileIO.fileIO_writeToLog(appConfig.logFile, statusMessage)

  
    f.close()
###############################################################################

###############################################################################
def dProc_compressGatanData(DATA_COMPRESS_STRUCT):
    try:
        df = pandas.read_csv(os.path.join(appConfig.logDir, "directoryToCompress.txt"), delimiter = "\t", 
                                names = ["dirName", "numSubDir", "numFiles", "size"], header = 0)
        [row, col] = df.shape

        f = open(os.path.join(appConfig.logDir, "directoryToCompress_Update.txt"), "w")
        f.write("Directory\tNumber of subdirectories\tNumber of files\tSize (GB)\n")
        
        fileIO.fileIO_writeToLog(appConfig.logFile, message = "GATAN MOVIE DATA WILL BE COMPRESSED IRRESPECTIVE OF THE SIZE")
        fileIO.fileIO_writeToLog(appConfig.logFile, message = "Directory\tNumber of subdirectories\tNumber of files\tSize (GB)")
        
        i = 0
        while (i < row):
            try:
                zipDir        = df["dirName"][i]
                zipNumSubDir  = df["numSubDir"][i]
                zipNumFiles   = df["numFiles"][i]
                zipFolderSize = df["size"][i]
                parentDir     = None
            except Exception as e:
                print('-'*50)
                print("IMPORTANT NOTICE \nThis is unexpected! inform Praveen")
                print('-'*50)
                print(i, df.shape)
                print(df)
                print('-'*50)
                print(e)
                print('-'*50)
                zipDir        = df["dirName"]
                zipNumSubDir  = df["numSubDir"]
                zipNumFiles   = df["numFiles"]
                zipFolderSize = df["size"]
                parentDir     = None
            
            if (platform.system() == "Linux"):
                match1 = re.search(r"/Hour_\d\d/Minute_\d\d/Second_\d\d", zipDir)
                match2 = re.search(r"/Hour_\d\d/Minute_\d\d",             zipDir)
                match3 = re.search(r"/Hour_\d\d",                         zipDir)
                
                if (match1):
                    parentDir = zipDir.split(match1.group())[0]
                elif (match2):
                    parentDir = zipDir.split(match2.group())[0]
                elif (match3):
                    parentDir = zipDir.split(match3.group())[0]
                    
            elif (platform.system() == "Windows"):
                match1 = re.search(r"\\Hour_\d\d\\Minute_\d\d\\Second_\d\d", zipDir)
                match2 = re.search(r"\\Hour_\d\d\\Minute_\d\d",              zipDir)
                match3 = re.search(r"\\Hour_\d\d",                           zipDir)
                
                if (match1):
                    parentDir = zipDir.split(match1.group())[0]
                elif (match2):
                    parentDir = zipDir.split(match2.group())[0]
                elif (match3):
                    parentDir = zipDir.split(match3.group())[0]
                    
            parentNumSubDir  = zipNumSubDir + 1
            parentNumFiles   = zipNumFiles
            parentFolderSize = zipFolderSize
            
            if (i < row - 1):
                if (parentDir):
                    for j in range(i + 1, row):
                        try:
                            daughterDir        = df["dirName"][j]
                            daughterNumSubDir  = df["numSubDir"][j] + 1
                            daughterNumFiles   = df["numFiles"][j]
                            daughterFolderSize = df["size"][j]
                        except:
                            daughterDir        = df["dirName"]
                            daughterNumSubDir  = df["numSubDir"] + 1
                            daughterNumFiles   = df["numFiles"]
                            daughterFolderSize = df["size"]
                        
                        if (platform.system()=="Linux"):
                            parentDirTemp = parentDir + "/"
                        elif (platform.system()=="Windows"):
                            parentDirTemp = parentDir + "\\"
                            
                        if (parentDirTemp in daughterDir):
                            parentNumSubDir  = parentNumSubDir + daughterNumSubDir
                            parentNumFiles   = parentNumFiles + daughterNumFiles
                            parentFolderSize = parentFolderSize + daughterFolderSize
                        else:
                            f.write("%s\t%d\t%d\t%.6f\n" %(parentDir, parentNumSubDir, parentNumFiles, parentFolderSize))
                            fileIO.fileIO_writeToLog(appConfig.logFile, message = "%s\t%d\t%d\t%.6f" %(parentDir, parentNumSubDir, parentNumFiles, parentFolderSize))
                            i = j
                            break
                else:
                    f.write("%s\t%d\t%d\t%.6f\n" %(zipDir, zipNumSubDir, zipNumFiles, zipFolderSize))
                    fileIO.fileIO_writeToLog(appConfig.logFile, message = "%s\t%d\t%d\t%.6f" %(zipDir, zipNumSubDir, zipNumFiles, zipFolderSize))
                    i = i + 1
            else:
                f.write("%s\t%d\t%d\t%.6f\n" %(zipDir, zipNumSubDir, zipNumFiles, zipFolderSize))
                fileIO.fileIO_writeToLog(appConfig.logFile, message = "%s\t%d\t%d\t%.6f" %(zipDir, zipNumSubDir, zipNumFiles, zipFolderSize))
                i = i + 1
                
        f.close()
        
        fileIO.fileIO_mvFile(os.path.join(appConfig.logDir, "directoryToCompress.txt"), 
                             os.path.join(appConfig.logDir, "directoryToCompress_bk.txt"))
        fileIO.fileIO_mvFile(os.path.join(appConfig.logDir, "directoryToCompress_Update.txt"),
                             os.path.join( appConfig.logDir + "/directoryToCompress.txt"))
        
        statusMessage = "dProc_compressGatanData successful. Gatan movies will be forcefully compressed."
    except:
        statusMessage = "ERROR: dProc_compressGatanData failed."
        
    fileIO.fileIO_writeToLog(appConfig.logFile, statusMessage)
    print (statusMessage)
###############################################################################

###############################################################################
def dProc_deleteCompressedDirectory(DATA_COMPRESS_STRUCT):
    try:
        fileIO.fileIO_writeToLog(appConfig.logFile, message = "DELETE THE FOLDERS THAT HAVE BEEN SUCESSFULLY COMPRESSED.")
        
        df = pandas.read_csv(os.path.join(appConfig.logDir, "directoryToCompressStatus.txt"), delimiter = "\t", 
                             names = ["dirName", "status"], header = 0)
        [row, col] = df.shape
        
        for i in range(row):
            try:
                dirName = df["dirName"][i]
                status  = df["status"][i]
            except:
                dirName = df["dirName"]
                status  = df["status"]
                
            if (status == True):
                fileIO.fileIO_writeToLog(appConfig.logFile, message = "Deleting directory %s" %(dirName))
                fileIO.fileIO_rmDir(dirName)
                
        statusMessage = "dProc_deleteDirectory. Process completed."
    except:
        statusMessage = "ERROR: dProc_deleteDirectory. Process failed."
        
    fileIO.fileIO_writeToLog(appConfig.logFile, statusMessage)
    print (statusMessage)
###############################################################################
###############################################################################
def dProc_standardizeDirNames(DATA_COMPRESS_STRUCT):
    try: 
        fileIO.fileIO_writeToLog(appConfig.logFile, message = r"SCAN THROUGH THE DIRECTORY STRUCTURE AND RENAME FOLDERS WITH \n IN THEIR NAME!")    
        scanDir = DATA_COMPRESS_STRUCT.inputDir    
        fileIO.fileIO_dirExists(scanDir)
        
        for root, dirs, files in os.walk(scanDir):
            print ("Checking %s" %(root))
            for f in files:
                if ('\n' in f) or ('\t' in f):
                    new_fname = f.replace('\n', '-n').replace('\t', '-t')
                    os.rename(os.path.join(root, f), os.path.join(root, new_fname))
                    log_message = f'renamed {f} to {new_fname} inside {root}'
                    fileIO.fileIO_writeToLog(appConfig.logFile, message = log_message)
            for d in dirs:
                if ('\n' in d) or ('\t' in d):
                    new_dirname = d.replace('\n', '-n').replace('\t', '-t')
                    os.rename(os.path.join(root, d), os.path.join(root, new_dirname))
                    log_message = f'renamed {d} to {new_dirname} inside {root}'
                    fileIO.fileIO_writeToLog(appConfig.logFile, message = log_message)

        statusMessage = "dProc_standardizeDirNames successful"
    except:
        statusMessage = "ERROR: dProc_standardizeDirNames failed"
        
    fileIO.fileIO_writeToLog(appConfig.logFile, statusMessage)
    print(statusMessage)

###############################################################################
###############################################################################
def dProc_directoryFilesAndSize(DATA_COMPRESS_STRUCT):
    try:
        f = open(os.path.join(appConfig.logDir, "directoryList.txt"), "w")
        f.write("Directory\tNumber of files\tFile size (GB)\tFile types\n")
        
        fileIO.fileIO_writeToLog(appConfig.logFile, message = "SCAN THROUGH THE DIRECTORY STRUCTURE AND CALCULATE NUMBER OF FILES WITH SIZE IN EACH FOLDER.")
        fileIO.fileIO_writeToLog(appConfig.logFile, message = "Directory\tNumber of files\tFile size (GB)\tNumber of file types")
        
        scanDir = DATA_COMPRESS_STRUCT.inputDir
        fileIO.fileIO_dirExists(scanDir)
        
        for root, dirs, files in os.walk(scanDir):
            print ("Scanning %s" %(root))
            
            fileExtList = []
            fileSize    = 0
            numFiles    = 0
            
            for name in files:
                fileName  = os.path.join(root, name)
                extension = fileIO.fileIO_getFileExtension(name)
                
                if (extension not in DATA_COMPRESS_STRUCT.exclusionList):
                    fileExtList.append(extension)
                    
                numFiles += 1
                fileSize += os.path.getsize(fileName)
            numFileTypes = numpy.size(numpy.unique(fileExtList))
            fileSize     = convertUnit.convert_byte2gigabyte(fileSize)
            
            f.write("%s\t%d\t%.10f\t%d\n" %(root, numFiles, fileSize, numFileTypes))
            fileIO.fileIO_writeToLog(appConfig.logFile, message = "%s\t%d\t%.10f\t%d" %(root, numFiles, fileSize, numFileTypes))
                
        f.close()
        
        statusMessage = "dProc_directoryFilesAndSize successful. Completed scanning through all the directories."
    except:
        statusMessage = "ERROR: dProc_directoryFilesAndSize failed."
        
    fileIO.fileIO_writeToLog(appConfig.logFile, statusMessage)
    print (statusMessage)
###############################################################################

###############################################################################
def dProc_directoryFilesAndSizeConsolidate(DATA_COMPRESS_STRUCT):
    try:
        df = pandas.read_csv(os.path.join(appConfig.logDir, "directoryList.txt"), delimiter = "\t", 
                             names = ["dirName", "numFiles", "size", "fileTypes"], header = 0)
        [row,col] = df.shape
        
        f = open(os.path.join(appConfig.logDir, "directorySize.txt"), "w")
        f.write("Directory\tNumber of subdirectories\tNumber of files\tSize (GB)\tFile types\n")
        
        fileIO.fileIO_writeToLog(appConfig.logFile, message = "CALCULATING THE TOTAL NUMBER OF FILES, DIRECTORIES, AND FILETYPES IN EACH FOLDER RECURSIVELY.")
        fileIO.fileIO_writeToLog(appConfig.logFile, message = "Directory\tNumber of subdirectories\tNumber of files\tSize (GB)\tNumber of file types")
        
        for i in range(row):
            try:
                parentDir        = df["dirName"][i]
                parentNumFiles   = df["numFiles"][i]
                parentFolderSize = df["size"][i]
                parentFileTypes  = df["fileTypes"][i]
            except:
                parentDir        = df["dirName"]
                parentNumFiles   = df["numFiles"]
                parentFolderSize = df["size"]
                parentFileTypes  = df["fileTypes"]
                
            if (platform.system() == "Linux"):
                parentDirTemp = parentDir + "/"
            elif (platform.system() == "Windows"):
                parentDirTemp = parentDir + "\\"
                
            numSubDir = 0
            for j in range(i + 1, row):
                try:
                    daughterDir        = df["dirName"][j]
                    daughterNumFiles   = df["numFiles"][j]
                    daughterFolderSize = df["size"][j]
                    daughterFileTypes  = df["fileTypes"][j]
                except:
                    daughterDir        = df["dirName"]
                    daughterNumFiles   = df["numFiles"]
                    daughterFolderSize = df["size"]
                    daughterFileTypes  = df["fileTypes"]
                
                if (parentDirTemp in daughterDir):
                    parentNumFiles   += daughterNumFiles
                    parentFolderSize += daughterFolderSize
                    numSubDir        += 1
                    parentFileTypes  += daughterFileTypes
                else:
                    break
                    
            f.write("%s\t%d\t%d\t%.10f\t%d\n" %(parentDir, numSubDir, parentNumFiles, parentFolderSize, parentFileTypes))
            fileIO.fileIO_writeToLog(appConfig.logFile, message = "%s\t%d\t%d\t%.10f\t%d" %(parentDir, numSubDir, parentNumFiles, parentFolderSize, parentFileTypes))
        f.close()
        
        statusMessage = "dProc_directoryFilesAndSizeRecursive successful. Completed estimating cumulative files and size of each directory."
    except:
        statusMessage = "ERROR: dProc_directoryFilesAndSizeRecursive failed."
        
    fileIO.fileIO_writeToLog(appConfig.logFile, statusMessage)
    print (statusMessage)
###############################################################################

###############################################################################
def dProc_findDirectoryToCompress(DATA_COMPRESS_STRUCT):
    try:
        firstCompressDirFound = False
        df = pandas.read_csv(os.path.join(appConfig.logDir, "directorySize.txt"), delimiter = "\t", 
                             names = ["dirName", "numDir", "numFiles", "size", "fileTypes"], header = 0)
        [row, col] = df.shape
        
        f = open(os.path.join(appConfig.logDir, "directoryToCompress.txt"), "w")
        f.write("Directory\tNumber of subdirectories\tNumber of files\tSize (GB)\n")
        
        fileIO.fileIO_writeToLog(appConfig.logFile, message = "LIST OF ALL POTENTIAL DIRECTORIES THAT CAN BE COMPRESSED.")
        fileIO.fileIO_writeToLog(appConfig.logFile, message = "Directory\tNumber of subdirectories\tNumber of files\tSize (GB)")
        
        for i in range(row):
            try:
                parentDir        = df["dirName"][i]
                parentNumDir     = df["numDir"][i]
                parentNumFiles   = df["numFiles"][i]
                parentFolderSize = df["size"][i]
                parentFileTypes  = df["fileTypes"][i]
            except:
                parentDir        = df["dirName"]
                parentNumDir     = df["numDir"]
                parentNumFiles   = df["numFiles"]
                parentFolderSize = df["size"]
                parentFileTypes  = df["fileTypes"]
            
            if (firstCompressDirFound == False):
                ###### A DIRECTORY SMALLER THAN fileSizeLimitGB IS COMPRESSED
                if (parentFolderSize <= DATA_COMPRESS_STRUCT.fileSizeLimitGB):
                    print ("Selecting for compression %s" %(parentDir))
                    firstCompressDirFound = True
                    
                    f.write("%s\t%d\t%d\t%.6f\n" %(parentDir, parentNumDir, parentNumFiles, parentFolderSize))
                    fileIO.fileIO_writeToLog(appConfig.logFile, message = "%s\t%d\t%d\t%.6f" %(parentDir, parentNumDir, parentNumFiles, parentFolderSize))
                    
                    if (platform.system() == "Linux"):
                        latestCompressParentDir = parentDir + "/"
                    elif (platform.system() == "Windows"):
                        latestCompressParentDir = parentDir + "\\"
                    
                ###### A DIRECTORY LARGER THAN fileSizeLimitGB WITH NOT MORE
                ###### THAN TWO FILE TYPES, NO SUBDIRECTORIES, AND MORE
                ###### THAN maxSameFileTypeInDir FILES WILL BE COMPRESSED
                elif (parentFolderSize > DATA_COMPRESS_STRUCT.fileSizeLimitGB and \
                      parentFileTypes <= 2 and \
                      parentNumDir == 0 and \
                      parentNumFiles >= DATA_COMPRESS_STRUCT.maxSameFileTypeInDir):
                    print ("Selecting for compression %s" %(parentDir))
                    firstCompressDirFound = True
                    
                    f.write("%s\t%d\t%d\t%.6f\n" %(parentDir, parentNumDir, parentNumFiles, parentFolderSize))
                    fileIO.fileIO_writeToLog(appConfig.logFile, message = "%s\t%d\t%d\t%.6f" %(parentDir, parentNumDir, parentNumFiles, parentFolderSize))
                    
                    if (platform.system() == "Linux"):
                        latestCompressParentDir = parentDir + "/"
                    elif (platform.system() == "Windows"):
                        latestCompressParentDir = parentDir + "\\"
                        
            elif (latestCompressParentDir not in parentDir):
                if (parentFolderSize <= DATA_COMPRESS_STRUCT.fileSizeLimitGB):
                    print ("Selecting for compression %s" %(parentDir))
                    firstCompressDirFound = True
                    
                    f.write("%s\t%d\t%d\t%.6f\n" %(parentDir, parentNumDir, parentNumFiles, parentFolderSize))
                    fileIO.fileIO_writeToLog(appConfig.logFile, message = "%s\t%d\t%d\t%.6f" %(parentDir, parentNumDir, parentNumFiles, parentFolderSize))
                    
                    if (platform.system() == "Linux"):
                        latestCompressParentDir = parentDir + "/"
                    elif (platform.system() == "Windows"):
                        latestCompressParentDir = parentDir + "\\"
                    
                ###### A DIRECTORY LARGER THAN fileSizeLimitGB WITH NOT MORE
                ###### THAN TWO FILE TYPES, NO SUBDIRECTORIES, AND MORE
                ###### THAN maxSameFileTypeInDir FILES WILL BE COMPRESSED
                elif (parentFolderSize > DATA_COMPRESS_STRUCT.fileSizeLimitGB and \
                      parentFileTypes <= 2 and \
                      parentNumDir == 0 and \
                      parentNumFiles >= DATA_COMPRESS_STRUCT.maxSameFileTypeInDir):
                    print ("Selecting for compression %s" %(parentDir))
                    firstCompressDirFound = True
                    
                    f.write("%s\t%d\t%d\t%.6f\n" %(parentDir, parentNumDir, parentNumFiles, parentFolderSize))
                    fileIO.fileIO_writeToLog(appConfig.logFile, message = "%s\t%d\t%d\t%.6f" %(parentDir, parentNumDir, parentNumFiles, parentFolderSize))
                    
                    if (platform.system() == "Linux"):
                        latestCompressParentDir = parentDir + "/"
                    elif (platform.system() == "Windows"):
                        latestCompressParentDir = parentDir + "\\"
                        
        f.close()
        
        statusMessage = "dProc_findDirectoryToCompress successful. Completed making list of directories to compress."
    except:
        statusMessage = "ERROR: dProc_findDirectoryToCompress failed."
        
    fileIO.fileIO_writeToLog(appConfig.logFile, statusMessage)
    print (statusMessage)
###############################################################################

###############################################################################
def dProc_rmTemporaryFiles(DATA_COMPRESS_STRUCT):
    try:
        fileIO.fileIO_rmFile(os.path.join(appConfig.logDir, "directoryList.txt"))
        fileIO.fileIO_rmFile(os.path.join(appConfig.logDir, "directorySize.txt"))
        fileIO.fileIO_rmFile(os.path.join(appConfig.logDir, "directoryToCompress.txt"))
        fileIO.fileIO_rmFile(os.path.join(appConfig.logDir, "directoryToCompress_bk.txt"))
        fileIO.fileIO_rmFile(os.path.join(appConfig.logDir, "directoryToCompressStatus.txt"))
        
        statusMessage = "dProc_rmTemporaryFiles successful. Temporary log files removed."
    except:
        statusMessage = "ERROR: dProc_rmTemporaryFiles failed."
        
    fileIO.fileIO_writeToLog(appConfig.logFile, statusMessage)
    print (statusMessage)
###############################################################################

###############################################################################
def dProc_splitLargeFiles(DATA_COMPRESS_STRUCT):
    try:
        fileIO.fileIO_writeToLog(appConfig.logFile, message = "SPLIT LARGE FILES (> %.2f GB) INTO SMALLER FILES." %(DATA_COMPRESS_STRUCT.fileSizeLimitGB))
        
        scanDir = DATA_COMPRESS_STRUCT.inputDir    
        if (fileIO.fileIO_dirExists(scanDir) == True):
            fileNameList = fileIO.fileIO_getFilesInDir(scanDir)
        else:
            fileNameList = [scanDir + ".zip"]
            
        for fileName in fileNameList:
            fileSize = fileIO.fileIO_getFileSize(fileName, "GB")
            if (fileSize > DATA_COMPRESS_STRUCT.fileSizeLimitGB):
                fileSizeLimit       = convertUnit.convert_gigabyte2byte(DATA_COMPRESS_STRUCT.fileSizeLimitGB)
                fileSplitChunckSize = convertUnit.convert_megabyte2byte(DATA_COMPRESS_STRUCT.fileSplitChunckSizeMB)
                
                fileIO.fileIO_splitFile(fileName, fileSizeLimit, fileSplitChunckSize)
                    
        statusMessage = "dProc_splitLargeFiles. Successfully split large files."
    except:
        statusMessage = "ERROR: dProc_splitLargeFiles. Unable to split large files."
        
    fileIO.fileIO_writeToLog(appConfig.logFile, statusMessage)
    print (statusMessage)
###############################################################################
