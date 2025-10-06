import os
import sys

sys.path.append(os.path.abspath("../configuration"))

import appConfig
import fileIO

def convert_byte2gigabyte(x):
    try:
        x_convert = x / (1024.0 * 1024.0 * 1024.0)
        return x_convert
    except:
        fileIO.fileIO_writeToLog(appConfig.logFile, "ERROR: convert_byte2gigabyte. Unable to convert bytes to gigabytes.", True)
        
        
def convert_byte2megabyte(x):
    try:
        x_convert = x / (1024.0 * 1024.0)
        return x_convert
    except:
        fileIO.fileIO_writeToLog(appConfig.logFile, "ERROR: convert_byte2megabyte. Unable to convert megabytes to bytes.", True)
        
        
def convert_gigabyte2byte(x):
    try:
        x_convert = x * (1024.0 * 1024.0 * 1024.0)
        return x_convert
    except:
        fileIO.fileIO_writeToLog(appConfig.logFile, "ERROR: convert_gigabyte2byte. Unable to convert gigabytes to bytes.", True)
        
        
def convert_megabyte2byte(x):
    try:
        x_convert = x * (1024.0 * 1024.0)
        return x_convert
    except:
        fileIO.fileIO_writeToLog(appConfig.logFile, "ERROR: convert_megabyte2byte. Unable to convert megabytes to bytes.", True)
        
        
def convert_min2s(x):
    try:
        x_convert = x * (60.0)
        return x_convert
    except:
        fileIO.fileIO_writeToLog(appConfig.logFile, "ERROR: convert_min2s. Unable to convert minutes to seconds.", True)