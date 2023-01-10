# -*- coding: utf-8 -*-
"""
Created on Tue Jan 10 10:27:22 2023

@author: sadeghi.a
"""
import os
import re
import pandas as pd
from datetime import datetime
import shutil
import sqlalchemy as sa
import codecs

def extractWantedFiles(path):
    files = os.listdir(path)
    result = [re.findall(string = file, pattern = 'ValidationError\d*.txt|ValidationOk\d*.txt')[0] for file in files ]  
    return result

def detectFileType(file):
    pattern = '(ValidationError|ValidationOk)\d*\.txt'
    return re.findall(pattern, file)[0]

def readErroneusLines(filePath):
    with codecs.open(filePath, 'r', 'UTF-8') as file:
        lines = file.readlines()
    lines = [re.findall(string = line, pattern = ".*{.*")[0].replace('\t\r', '') for line in lines if re.findall(string = line, pattern = "{")]
    linesSplitted = [line.split('\t') for line in lines]
    linesCorrected = []
    for line in linesSplitted:
        if(len(line) == 10):
            linesCorrected.append(line[0:6] + [line[6] + line[7] + line[8] + line[9]])
        elif(len(line) == 7):
            linesCorrected.append(line)
    return linesCorrected 

def renameCols(df, fileType):
    if fileType == "ValidationOk":
        df.columns = ["BankName", "AccountNumber", "ShebaNumber", "NationalCode", "TransactionTime", "Status"]
    elif fileType == "ValidationError":
        df.columns = ["BankName", "AccountNumber", "ShebaNumber", "NationalCode", "TransactionTime", "ErrorCode", "Status"]
    return df

def loadTextFiles(fileName, path, fileType):
    filePath = '/'.join([path, fileName])
    df = pd.read_csv(filepath_or_buffer=filePath, delimiter='\t', header=None, on_bad_lines='skip',  engine='python').dropna(axis = 1, how = "all")
    df = renameCols(df, fileType)
    if fileType == "ValidationError":
        linesCorrected = readErroneusLines(filePath)
        dfCorrctedLines = pd.DataFrame(linesCorrected)
        dfCorrctedLines = renameCols(dfCorrctedLines, fileType)
        completeDF = pd.concat([df, dfCorrctedLines])
        return completeDF
    else: 
        return df
    
        
def makeDataClean(data, fileType):
    for col in range(data.shape[1]):
        data.iloc[:, col] = data.iloc[:, col].str.replace('^.*?:','', regex = True).str.strip()
    return data
        
def enrichData(data, file, fileType):
    data['Date'] = data.TransactionTime.str[:10]
    data['FileName'] = file
    data['Type'] = fileType
    if fileType == "ValidationOk":
        columnsOrder = ["BankName", "AccountNumber", "ShebaNumber", "NationalCode", "Date", "TransactionTime", "Status", 'FileName', 'Type']
    elif fileType == "ValidationError":
        columnsOrder = ["BankName", "AccountNumber", "ShebaNumber", "NationalCode", "Date", "TransactionTime", "ErrorCode", "Status", 'FileName', 'Type']
    return data[columnsOrder]
    
def createPickle(data, fileType):
    if fileType == "ValidationOk":
        data.to_pickle('Pickles/ValidationOk.pickle')
    elif fileType == "ValidationError":
        data.to_pickle('Pickles/ValidationError.pickle')

def moveLogs(path, files):
    folderName = datetime.strftime(datetime.now(), '%Y-%m-%d %H%M')
    os.makedirs('/'.join([path, folderName]))
    for file in files:
        shutil.move('/'.join([path, file]), '/'.join([path, folderName, file]))
    
def createEngine():
    config = 'mssql+pyodbc://172.16.1.121/SadeghiTest?driver=SQL+Server+Native+Client+11.0'
    return sa.create_engine(config)

def setDBTypes(fileType):
    if fileType == "ValidationOk":
        dtype = {"BankName":  sa.types.NVARCHAR(length=50), 
                 "AccountNumber": sa.types.VARCHAR(length=50), 
                 "ShebaNumber": sa.types.VARCHAR(length=50),
                 "NationalCode": sa.types.VARCHAR(length=30), 
                 "Date": sa.types.VARCHAR(length=10), 
                 "TransactionTime": sa.types.VARCHAR(length=21),  
                 "Status": sa.types.NVARCHAR(length=1000), 
                 "FileName": sa.types.NVARCHAR(length=50), 
                 "Type": sa.types.VARCHAR(length=20)}
    elif fileType == "ValidationError":
        dtype = {"BankName":  sa.types.NVARCHAR(length=100), 
                 "AccountNumber": sa.types.VARCHAR(length=50), 
                 "ShebaNumber": sa.types.VARCHAR(length=50),
                 "NationalCode": sa.types.VARCHAR(length=30), 
                 "Date": sa.types.VARCHAR(length=10), 
                 "TransactionTime": sa.types.VARCHAR(length=21), 
                 "ErrorCode": sa.types.VARCHAR(length=10), 
                 "Status": sa.types.NVARCHAR(length=1000), 
                 "FileName": sa.types.NVARCHAR(length=50), 
                 "Type": sa.types.VARCHAR(length=20)}
    return dtype

def fixColumnSize(data, fileType):
    if fileType == "ValidationOk":
        data.BankName = data.BankName.str[:50]
        data.AccountNumber = data.AccountNumber.str[:50]
        data.ShebaNumber = data.ShebaNumber.str[:50]
        data.NationalCode = data.NationalCode.str[:50]
        data.Date = data.Date.str[:50]
        data.TransactionTime = data.TransactionTime.str[:50]
        data.Status = data.Status.str[:1000]
        data.FileName = data.FileName.str[:50]
        data.Type = data.Type.str[:50]
    elif fileType == "ValidationError":
        data.BankName = data.BankName.str[:100]
        data.AccountNumber = data.AccountNumber.str[:50]
        data.ShebaNumber = data.ShebaNumber.str[:50]
        data.NationalCode = data.NationalCode.str[:50]
        data.Date = data.Date.str[:50]
        data.TransactionTime = data.TransactionTime.str[:50]
        data.ErrorCode = data.ErrorCode.str[:50]
        data.Status = data.Status.str[:1000]
        data.FileName = data.FileName.str[:50]
        data.Type = data.Type.str[:50]
    return data