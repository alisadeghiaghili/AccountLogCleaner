# -*- coding: utf-8 -*-
"""
Created on Tue Jan 10 10:26:00 2023

@author: sadeghi.a
"""

import pandas as pd
import os
import warnings

warnings.filterwarnings("ignore")
workingDir = r'D:\AccountCleaner'
os.chdir(workingDir)

from funcs import *
logsPath = r'D:\Accounts'

files = extractWantedFiles(logsPath)

engine = createEngine()

for file in files:    
    fileType = detectFileType(file)
    data = loadTextFiles(file, logsPath, fileType)
    
    if data.shape[0] == 0: 
        continue
    
    data = makeDataClean(data, fileType)
    data = enrichData(data, file, fileType)
    createPickle(data, fileType)

    dbtypes = setDBTypes(fileType)
    if fileType == "ValidationOk":
        fixColumnSize(data, fileType).to_sql(name = 'ValidationOk', con = engine, schema = 'Account', if_exists='append', index = False, dtype = dbtypes)
    elif fileType == "ValidationError":
        fixColumnSize(data, fileType).to_sql(name = 'ValidationError', con = engine, schema = 'Account', if_exists='append', index = False, dtype = dbtypes)
        
moveLogs(logsPath, files)