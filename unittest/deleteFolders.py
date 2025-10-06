import os
import sys

sys.path.append(os.path.abspath("../configuration"))
sys.path.append(os.path.abspath("../resources"))

import appConfig
import fileIO

f = open("input.txt", "r")
lines = f.read().splitlines()

for line in lines:
    fileIO.fileIO_rmDir(line)