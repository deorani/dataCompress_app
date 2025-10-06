import os
import sys
import datetime

sys.path.append(os.path.abspath("../configuration"))

import appConfig
import fileIO

############################################################
def timeUtils_timestamp(delimiter = "\t"):
    """ Return the current date time as a string in format
    'YYYYMMDD HHMMSS'.
    
    Parameters:
    ----------
    NULL
        
    Returns:
    -------
    Current date and time separated by delimiter
    
    Usage:
    -----
    dt = timeUtils_timestamp(delimiter = " ")
    """
    try:
        if (delimiter == "\t"):
            string = datetime.datetime.now().strftime("%Y%m%d\t%H%M%S")
        elif (delimiter == "_"):
            string = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        elif (delimiter == " "):
            string = datetime.datetime.now().strftime("%Y%m%d %H%M%S")
        elif (delimiter == ":"):
            string = datetime.datetime.now().strftime("%Y%m%d:%H%M%S")
        elif (delimiter == ","):
            string = datetime.datetime.now().strftime("%Y%m%d,%H%M%S")
        elif (delimiter == "."):
            string = datetime.datetime.now().strftime("%Y%m%d.%H%M%S")
        else:
            string = datetime.datetime.now().strftime("%Y%m%d\t%H%M%S")
        return string
    except:
        fileIO.fileIO_writeToLog(appConfig.logFile, message = "ERROR: Unable to get time stamp. Return 00000000\t000000.")
        string = "00000000\t000000"
        return string
############################################################