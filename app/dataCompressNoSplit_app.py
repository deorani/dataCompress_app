import os
import sys
import argparse
from threading import Thread

sys.path.append(os.path.abspath("../configuration"))
sys.path.append(os.path.abspath("../resources"))
sys.path.append(os.path.abspath("../structures"))

import appConfig
import structDef
import fileIO
import dataProcess
import resourceMonitor


###############################################################################
def main(src, size):
    ######## READING THE DATA COMPRESSION CONFIGURATION
    data = fileIO.fileIO_readJson("../configuration/dataCompressConfig.json")
    DATA_COMPRESS_STRUCT = structDef.structDef_dataCompressInit(data, src, size)
    
    
    ######## READING THE RESOURCE MONITOR CONFIGURATION
    data = fileIO.fileIO_readJson("../configuration/resourceMonitorConfig.json")
    RESOURCE_MONITOR_STRUCT = structDef.struct_def_resourceMonitorInit(data, "generic")
    
    
    ######## STARTING A DAEMON PROCESS FOR RESOURCE MONITORING
    resourceMonitorThread = Thread(target = resourceMonitor.monitor_resourceUsageGeneric, args = [RESOURCE_MONITOR_STRUCT], daemon = True)
    resourceMonitorThread.start()
    
    
    ######## REMOVE OLD LOGS
    # Commented out the following, Praveen 10 March 2025
    # dataProcess.dProc_rmTemporaryFiles(DATA_COMPRESS_STRUCT)
    
    
    ######## START DATA COMPRESSION TASK
    # RECURSIVELY SCAN THROUGH THE DIRECTORY AND CALCULATE THE
    # SIZE, NUMBER OF FILES, AND DIRECTORY IN EACH
    dataProcess.dProc_standardizeDirNames(DATA_COMPRESS_STRUCT)
    dataProcess.dProc_directoryFilesAndSize(DATA_COMPRESS_STRUCT)
    dataProcess.dProc_directoryFilesAndSizeConsolidate(DATA_COMPRESS_STRUCT)
    
    # BASED ON THE CONDITIONS IN DATA_COMPRESS_STRUCT, CERTAIN FOLDERS
    # WILL BE SELECTED FOR COMPRESION
    dataProcess.dProc_findDirectoryToCompress(DATA_COMPRESS_STRUCT)
    
    # GATAN MOVIES WILL BE COMPRESSED IRRESPECTIVE OF THEIR SIZE
    dataProcess.dProc_compressGatanData(DATA_COMPRESS_STRUCT)

    # COMPRESS THE SELECTED DIRECTORIES USING 7-ZIP
    dataProcess.dProc_7zip(DATA_COMPRESS_STRUCT)
    
    ######## DELETE THE DIRECTORIES THAT HAVE BEEN COMPRESSED
    dataProcess.dProc_deleteCompressedDirectory(DATA_COMPRESS_STRUCT)
    
    
    ######## SPLIT LARGE FILES
    #dataProcess.dProc_splitLargeFiles(DATA_COMPRESS_STRUCT)

###############################################################################


###############################################################################
######## USER ARGUMENT PARSER
parser = argparse.ArgumentParser(prog = "dataCompress_app.py",
                                 description = "Python script to compress folders and split large files.")
parser.add_argument("-src",  type = str,               help = "Input directory that needs to be compressed")
parser.add_argument("-size", type = int, default = 50, help = "Maximum allowed folder/file size after compression (in GB)")

args = parser.parse_args()
args.src = os.path.normpath(args.src)


if (args.src == None):
    fileIO.fileIO_writeToLog(appConfig.logFile, "FATAL: Exit program. Please enter input directory to compress.", True)
    sys.exit()
elif (args.size == None):
    fileIO.fileIO_writeToLog(appConfig.logFile, "FATAL: Exit program. Please enter maximum allowed folder/file size after compression (in GB).", True)
    sys.exit()
else:
    ######## MAIN FUNCTION CALL
    fileIO.fileIO_writeToLog(appConfig.logFile, "dataCompress_app. Compressing %s with maximum allowed folder/file size after compression = %d GB." %(args.src, args.size))
    main(args.src, args.size)
###############################################################################