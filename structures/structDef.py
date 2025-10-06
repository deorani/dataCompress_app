import os
import sys
import pandas

sys.path.append(os.path.abspath("../resources"))

import fileIO


###############################################################################
class structDef_dataCompressInit:
    def __init__(self, configType, src, size):
        self.exclusionList         = configType["EXCLUSION_LIST"]
        self.fileSplitChunckSizeMB = configType["FILE_SPLIT_CHUNCK_SIZE_MB"]
        self.maxSameFileTypeInDir  = configType["MAX_SAME_FILETYPE_IN_DIR"]
        self.windows_7zip          = configType["7ZIP_LOCATION_WINDOWS"]
        self.linux_7zip            = configType["7ZIP_LOCATION_LINUX"] 
        self.inputDir = src
        self.fileSizeLimitGB = size
###############################################################################


###############################################################################
class structDef_dropboxBatchUploadInit:
    def __init__(self, configType, inputDir, localDropboxDir, profile):
        self.dbxDir                    = configType["DROPBOX_APP"]["DROPBOX_DIR"]
        self.dbxExcludeFileNameList    = configType["DROPBOX_APP"]["DROPBOX_EXCLUDE_FILE_NAME_LIST"]
        self.dbxExcludeFileContainList = configType["DROPBOX_APP"]["DROPBOX_EXCLUDE_FILE_CONTAIN_LIST"]
        self.dbxBatchSizeGB            = configType["DROPBOX_APP"]["DROPBOX_BATCH_SIZE_GB"]
        self.dbxStatusCheckTimeMin     = configType["DROPBOX_APP"]["DROPBOX_STATUSCHECK_TIME_MIN"]
        
        self.dbxBatchTimeLimitHR       = configType["DROPBOX_APP"]["DROPBOX_BATCH_TIME_LIMIT_HR"]
        self.dbxChunckSizeMB           = configType["DROPBOX_APP"]["DROPBOX_CHUNK_SIZE_MB"]
        
        data = fileIO.fileIO_readJson("../configuration/credential.json")
        self.dbxAccessToken = data["DROPBOX"][profile]["ACCESS_TOKEN"]
        
        data = {
                    "inputDir"       : [inputDir],\
                    "localDropboxDir": [localDropboxDir]\
               }
        self.df = pandas.DataFrame(data)
###############################################################################


###############################################################################
class struct_def_resourceMonitorInit:
    def __init__(self, configType, application = "generic"):
        if (application == "generic"):
            self.resourceCheckIntervalS = configType["GENERIC"]["RESOURCE_CHECK_INTERVAL_S"]
            self.diskName               = configType["GENERIC"]["DISK_NAME"]
        elif (application == "dropbox"):
            self.exe                    = configType["DROPBOX_APP"]["EXE"]
            self.restartTimeMin         = configType["DROPBOX_APP"]["RESTART_TIME_MIN"]
            self.maximumInactiveTimeMin = configType["DROPBOX_APP"]["MAXIMUM_INACTIVE_TIME_MIN"]
            self.minimumResourceMBps    = configType["DROPBOX_APP"]["MINIMUM_RESOURCE_MBPS"]
            self.resourceCheckIntervalS = configType["GENERIC"]["RESOURCE_CHECK_INTERVAL_S"]
            self.diskName               = configType["GENERIC"]["DISK_NAME"]
###############################################################################