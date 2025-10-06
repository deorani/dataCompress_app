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
import dropboxAPI
import resourceMonitor


###############################################################################
def main(src, dst, profile):
    ######## READING THE DROPBOX CONFIGURATION
    data = fileIO.fileIO_readJson("../configuration/dropboxConfig.json")
    DROPBOX_BATCH_UPLOAD_STRUCT = structDef.structDef_dropboxBatchUploadInit(data, src, dst, profile)
    
    
    ######## READING THE RESOURCE MONITOR CONFIGURATION FOR DROPBOX
    data = fileIO.fileIO_readJson("../configuration/resourceMonitorConfig.json")
    RESOURCE_MONITOR_STRUCT = structDef.struct_def_resourceMonitorInit(data, "dropbox")
    
    
    ######## STARTING A DAEMON PROCESS FOR RESOURCE MONITORING
    resourceMonitorThread = Thread(target = resourceMonitor.monitor_resourceUsageProcess, args = [RESOURCE_MONITOR_STRUCT], daemon = True)
    resourceMonitorThread.start()
    
    
    ######## UPLOADING THE FILES TO DROPBOX IN BATCHES
    dropboxAPI.dbxAPI_connectToDBX(DROPBOX_BATCH_UPLOAD_STRUCT)
    dropboxAPI.dbxAPI_filesToUpload(DROPBOX_BATCH_UPLOAD_STRUCT)
    dropboxAPI.dbxAPI_mkdirs(DROPBOX_BATCH_UPLOAD_STRUCT)
    dropboxAPI.dbxAPI_filesToUpload_Batch(DROPBOX_BATCH_UPLOAD_STRUCT)
    dropboxAPI.dbxAPI_uploadToDropbox_Batches(DROPBOX_BATCH_UPLOAD_STRUCT)
###############################################################################


###############################################################################
######## USER ARGUMENT PARSER
parser = argparse.ArgumentParser(prog = "dropboxAppUpload_app.py",
                                 description = "Python script to upload large volume of data to Dropbox using desktop application.")
parser.add_argument("-src",     type = str, help = "Input directory that needs to be moved to Dropbox")
parser.add_argument("-dst",     type = str, help = "Destination Dropbox directory")
parser.add_argument("-profile", type = str, help = "Profile name which will be used to check upload status.")

args = parser.parse_args()

if (args.src == None):
    fileIO.fileIO_writeToLog(appConfig.logFile, "FATAL: Exit program. Please enter source directory to upload.", True)
    sys.exit()
elif (args.dst == None):
    fileIO.fileIO_writeToLog(appConfig.logFile, "FATAL: Exit program. Please enter destination Dropbox directory.", True)
    sys.exit()
elif (args.profile == None):
    fileIO.fileIO_writeToLog(appConfig.logFile, "FATAL: Exit program. Please enter Dropbox profile to monitor upload progress.", True)
    sys.exit()
else:
    ######## MAIN FUNCTION CALL
    fileIO.fileIO_writeToLog(appConfig.logFile, "dropboxAppUpload_app. Uploading %s to %s using profile %s." %(args.src, args.dst, args.profile))
    main(args.src, args.dst, args.profile)
###############################################################################
