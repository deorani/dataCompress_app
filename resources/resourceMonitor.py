import os
import sys
import time
import platform
import psutil

sys.path.append(os.path.abspath("../configuration"))

import appConfig
import fileIO
import convertUnit


###############################################################################
def monitor_resourceUsageGeneric(RESOURCE_MONITOR_STRUCT):
    try:
        resourceUsageFlag = True
        
        while(True):
            # MEASURE DISK AND NET I/O AT TIME POINT 1
            tic  = time.time()
            
            disk = psutil.disk_io_counters()
            disk_read_MB_tic  = convertUnit.convert_byte2megabyte(disk.read_bytes)
            disk_write_MB_tic = convertUnit.convert_byte2megabyte(disk.write_bytes)
            
            net  = psutil.net_io_counters()
            net_sent_MB_tic = convertUnit.convert_byte2megabyte(net.bytes_sent)
            net_recv_MB_tic = convertUnit.convert_byte2megabyte(net.bytes_recv)
            
            # SLEEP FOR SOME TIME
            time.sleep(RESOURCE_MONITOR_STRUCT.resourceCheckIntervalS)
            
            # MEASURE DISK AND NET I/O AT TIME POINT 2
            toc = time.time()
            
            disk = psutil.disk_io_counters()
            disk_read_MB_toc  = convertUnit.convert_byte2megabyte(disk.read_bytes)
            disk_write_MB_toc = convertUnit.convert_byte2megabyte(disk.write_bytes)
            
            net = psutil.net_io_counters()
            net_sent_MB_toc = convertUnit.convert_byte2megabyte(net.bytes_sent)
            net_recv_MB_toc = convertUnit.convert_byte2megabyte(net.bytes_recv)
            
            # CALCULATE DISK AND NET I/O SPEED
            disk_IO_speed = (disk_read_MB_toc - disk_read_MB_tic) / (toc-tic) + (disk_write_MB_toc - disk_write_MB_tic) / (toc-tic)
            net_IO_speed  = (net_sent_MB_toc  - net_sent_MB_tic)  / (toc-tic) + (net_recv_MB_toc   - net_recv_MB_tic)   / (toc-tic)
            data_IO_speed = disk_IO_speed + net_IO_speed
            
            # WRITE THE RESOURCE USAGE TO FILE
            fileIO.fileIO_writeToResourceLog("%.2f\t%.2f\t%s" %(data_IO_speed, 0, resourceUsageFlag))
    except:
        fileIO.fileIO_writeToLog(appConfig.logFile, "ERROR: monitor_resourceUsageGeneric. Unable to calculate resource usage.")
###############################################################################

###############################################################################
def monitor_resourceUsageProcess(RESOURCE_MONITOR_STRUCT):
    try:
        # BY DEFAULT IT IS ASSUMED THAT THE PROCESS IS RUNNING
        resourceUsageFlag = True
        
        while(True):
            # MEASURE DISK AND NET I/O AT TIME POINT 1
            tic  = time.time()
            
            disk = psutil.disk_io_counters(perdisk = True)
            disk_read_MB_tic  = convertUnit.convert_byte2megabyte(disk[RESOURCE_MONITOR_STRUCT.diskName].read_bytes)
            disk_write_MB_tic = convertUnit.convert_byte2megabyte(disk[RESOURCE_MONITOR_STRUCT.diskName].write_bytes)
            
            net  = psutil.net_io_counters()
            net_sent_MB_tic = convertUnit.convert_byte2megabyte(net.bytes_sent)
            net_recv_MB_tic = convertUnit.convert_byte2megabyte(net.bytes_recv)
            
            # SLEEP FOR SOME TIME
            time.sleep(RESOURCE_MONITOR_STRUCT.resourceCheckIntervalS)
            
            # MEASURE DISK AND NET I/O AT TIME POINT 2
            toc = time.time()
            
            disk = psutil.disk_io_counters(perdisk = True)
            disk_read_MB_toc  = convertUnit.convert_byte2megabyte(disk[RESOURCE_MONITOR_STRUCT.diskName].read_bytes)
            disk_write_MB_toc = convertUnit.convert_byte2megabyte(disk[RESOURCE_MONITOR_STRUCT.diskName].write_bytes)
            
            net = psutil.net_io_counters()
            net_sent_MB_toc = convertUnit.convert_byte2megabyte(net.bytes_sent)
            net_recv_MB_toc = convertUnit.convert_byte2megabyte(net.bytes_recv)
            
            # CALCULATE DISK AND NET I/O SPEED
            disk_IO_speed = (disk_read_MB_toc - disk_read_MB_tic) / (toc-tic) + (disk_write_MB_toc - disk_write_MB_tic) / (toc-tic)
            net_IO_speed  = (net_sent_MB_toc  - net_sent_MB_tic)  / (toc-tic) + (net_recv_MB_toc   - net_recv_MB_tic)   / (toc-tic)
            data_IO_speed = disk_IO_speed + net_IO_speed
            
            # CHECKING IF PROCESS RUNNING AND UPDATING RESOURCE LOG
            if (data_IO_speed <= RESOURCE_MONITOR_STRUCT.minimumResourceMBps):
                if (resourceUsageFlag == True):
                    resourceUsageFlag     = False
                    processStop_startTime = tic
                    processStop_duration  = toc - tic
                    
                else:
                    processStop_duration = toc - processStop_startTime
                    if (processStop_duration > convertUnit.convert_min2s(RESOURCE_MONITOR_STRUCT.maximumInactiveTimeMin)):
                        fileIO.fileIO_writeToResourceLog("%.2f\t%.2f\t%s" %(data_IO_speed, processStop_duration, resourceUsageFlag))
                        
                        fileIO.fileIO_writeToResourceLog("Restarting application")
                        monitor_restartDropboxApp(RESOURCE_MONITOR_STRUCT)
                        fileIO.fileIO_writeToResourceLog("Restarted application")
                        
                        resourceUsageFlag = True
                        processStop_duration = 0
            else:
                resourceUsageFlag = True
                processStop_duration = 0
                
            fileIO.fileIO_writeToResourceLog("%.2f\t%.2f\t%s" %(data_IO_speed, processStop_duration, resourceUsageFlag))
    except:
        fileIO.fileIO_writeToLog(appConfig.logFile, "ERROR: monitor_resourceUsageProcess. Unable to calculate resource usage.")
###############################################################################

###############################################################################
def monitor_restartDropboxApp(RESOURCE_MONITOR_STRUCT):
    try:
        if (platform.system() == "Windows"):
            fileIO.fileIO_writeToLog(appConfig.logFile, "monitor_restartDropboxApp. Restarting Dropbox application.", True)
            
            f = open("restartProcess.bat", "w")
            
            f.write("@echo off\n")
            f.write("taskkill /im dropbox.exe /t /f\n")
            f.write("timeout %d\n" %(convertUnit.convert_min2s(RESOURCE_MONITOR_STRUCT.restartTimeMin)))
            f.write("start \"\" \"%s\"\n" %(RESOURCE_MONITOR_STRUCT.exe))
            f.write("exit\n")
            
            f.close()
            
            os.system("restartProcess.bat")
            time.sleep(5)
            
            fileIO.fileIO_rmFile("restartProcess.bat")
            
            fileIO.fileIO_writeToLog(appConfig.logFile, "monitor_restartDropboxApp. Successfully restarted Dropbox application.", True)
    except:
        fileIO.fileIO_writeToLog(appConfig.logFile, "ERROR: monitor_restartDropboxApp. Unable to restart Dropbox application.", True)
###############################################################################