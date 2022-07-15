import boto3
import botocore
import os
import h5py as h5
import re


#s3 = boto3.client('s3', aws_access_key_id='AKIA2UYKQHP63X5PQCMP', aws_secret_access_key='Exae5tQaotHCuUj3s+cPZi3qE/0CGIK4GPoU+RWS')
#bucket = 'gpr-scan-data'
#file = '0-0.hdf5' #replace with desired file
#container = '2022-07-13T19:21:48.201056/raw/' #leave empty is file is loose in bucket
#dirPath = os.path.join(os.getcwd(), file)


#data_set = h5.File(file ,'r')

#if(os.path.exists(dirPath) != True):
 #   s3.download_file(bucket, container+file, dirPath)

class GPRDataHelper:
    awsAccessKey = ''
    awsSecretKey = ''
    bucket = 'gpr-scan-data'

    def __init__(self, accessKey, secretKey): #constructor
        self.awsAccessKey = accessKey
        self.awsSecretKey = secretKey

    def download(self, scanID, folder, file): #download for internal use

        s3 = boto3.client('s3', aws_access_key_id = self.awsAccessKey, aws_secret_access_key = self.awsSecretKey)
        s3Dir = os.path.join(scanID, folder, file)
        localDir = os.path.join(os.getcwd(), file)

        if(os.path.exists(localDir) != True):
            s3.download_file(self.bucket, s3Dir, localDir)

    def getRawData(self, scanID, lineNumber, sweepNumber): #download a raw data file given the scan folder, line number, and sweep number

        fileName = str(lineNumber) + '-' + str(sweepNumber) + '.hdf5' #create the file name using provided parameters 
        self.download(scanID, 'raw', fileName) #call download method
        return(h5.File(os.path.join(os.getcwd(), fileName), 'r')) #returns the downloaded file in hdf5 format

    def getProcessedData(self, scanID, lineNumber):

        fileName = str(lineNumber) + '-bg.hdf5'
        self.download(scanID, 'raw', fileName)
        return(h5.File(os.path.join(os.getcwd(), fileName), 'r'))

    def listRaw(self, scanID): #lists all files in the raw folder within given scan folder
        s3 = boto3.client('s3', aws_access_key_id = self.awsAccessKey, aws_secret_access_key = self.awsSecretKey)
        response = s3.list_objects_v2(Bucket = self.bucket, Prefix=(scanID +'/raw/'))

        rawList = []
        tempStr = ''
        for thing in response.get('Contents', []):
            tempStr += thing['Key']

            if(tempStr[-3:] == 'df5'): #checks if file name has finished reading so we can start copying a new one
                formatStr = tempStr.replace((scanID +'/raw/'), '')
                rawList.append(formatStr)
                tempStr, formatStr = '',  ''

        return rawList
    
    def listProcessed(self, scanID): #lists all files in the img folder within given scan folder
        s3 = boto3.client('s3', aws_access_key_id = self.awsAccessKey, aws_secret_access_key = self.awsSecretKey)
        response = s3.list_objects_v2(Bucket = self.bucket, Prefix=(scanID +'/img/'))

        imgList = []
        tempStr = ''
        for thing in response.get('Contents', []):
 
            tempStr += thing['Key']

            if(tempStr[-3:] == 'df5' or tempStr[-3:] == 'png'):
                formatStr = tempStr.replace((scanID +'/img/'), '')
                imgList.append(formatStr)
                tempStr, formatStr = '',  ''

        return imgList

    def findRaw(self, scanID, lineNumber): #helper function to find find all matching raw files in given scan folder
        regex = str(lineNumber)+"-(\d+).hdf5"

        rawList = []

        listR = self.listRaw(scanID)
        test_str = ""

        for item in listR: #reformats list from listRaw() to something regex can use
            test_str += item + '\n'

        matches = re.finditer(regex, test_str, re.MULTILINE)

        for matchNum, match in enumerate(matches, start=1):
            rawList.append(match.group())
        
        return rawList #returns list of matching file names


    def findProcessed(self, scanID): #helper function to find find all matching processed files in given scan folder
        regex = r"(\d)+-bg.hdf5"

        procList = []

        listP = self.listProcessed(scanID)
        test_str = ""

        for item in listP: #reformats list from listRaw() to something regex can use
            test_str += item + '\n'

        matches = re.finditer(regex, test_str, re.MULTILINE)

        for matchNum, match in enumerate(matches, start=1):
            procList.append(match.group())
        
        return procList #returns list of matching file names

    def getRawDataByLine(self, scanID, lineNumber): #get all raw files that match the line number in the given scan folder
                                                    #and return them as a list of htf5 datasets
        rawList = self.findRaw(scanID, lineNumber)
        data_set = []
        
        for item in rawList:
            self.download(scanID, 'raw', item) #call download method
            data_set.append(h5.File(os.path.join(os.getcwd(), item), 'r')) #addes downloaded file to data_set list in htf5 format
        return data_set

    def getProcessedDataAll(self, scanID): #get all processed files in the given scan folder
                                                    #and return them as a list of htf5 datasets
        procList = self.findProcessed(scanID)
        data_set = []
        
        for item in procList:
            self.download(scanID, 'img', item) #call download method
            data_set.append(h5.File(os.path.join(os.getcwd(), item), 'r')) #addes downloaded file to data_set list in htf5 format
        return data_set

