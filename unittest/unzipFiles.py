import os
import sys
import platform
import subprocess

sys.path.append(os.path.abspath("../configuration"))

import appConfig
import fileIO

if (platform.system() == "Linux"):
    executable = "7za"
elif (platform.system() == "Windows"):
    executable = "C:\\Program Files\\7-Zip\\7z.exe"
    
f = open("input.txt", "r")
lines = f.read().splitlines()

for zipFile in lines:
    outputDir = fileIO.fileIO_getFileDirectory(zipFile)
    fileIO.fileIO_writeToLog(appConfig.logFile, "Uncompressing %s." %(zipFile))
    subprocess.run([executable, "x", zipFile, "-o%s" %(outputDir), "-aoa"])
    fileIO.fileIO_rmFile(zipFile)