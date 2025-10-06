import os
import sys
import socket

sys.path.append(os.path.abspath("../resources"))

import timeUtils

homeDir = ".."

hostName = socket.gethostname()
timeStamp = timeUtils.timeUtils_timestamp("_")

logDir  = os.path.join(homeDir, "logs", timeStamp)
logFile   = os.path.join(logDir, f"{hostName}_{timeStamp}.log")
resourceMonitorFile = os.path.join(logDir, "resourceMonitor_" + hostName + ".log")

if (os.path.exists(logDir) == False):
    os.makedirs(logDir)
    
if (os.path.isfile(logFile) == False):
    f = open(logFile, "w")
    f.close()
    
f = open(resourceMonitorFile, "w")
f.write("Date\tTime\tData IO speed (MBps)\tDuration of inactivity (s)\tProcess active flag\n")
f.close()