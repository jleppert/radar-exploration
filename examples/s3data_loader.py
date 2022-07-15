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

    def __init__(self, accessKey, secretKey):
        self.awsAccessKey = accessKey
        self.awsSecretKey = secretKey

    def download(self, scanID, folder, file):

        s3 = boto3.client('s3', aws_access_key_id = self.awsAccessKey, aws_secret_access_key = self.awsSecretKey)
        s3Dir = os.path.join(scanID, folder, file)
        localDir = os.path.join(os.getcwd(), file)

        if(os.path.exists(localDir) != True):
            self.s3.download_file(self.bucket, s3Dir, localDir)

    def getRawData(self, scanID, lineNumber, sweepNumber):

        fileName = str(lineNumber) + '-' + str(sweepNumber) + '.hdf5'
        self.download(scanID, 'raw', fileName)
        return(h5.File(os.path.join(os.getcwd(), fileName), 'r'))

    def getProcessedData(self, scanID, lineNumber):

        fileName = str(lineNumber) + '-bg.hdf5'
        self.download(scanID, 'raw', fileName)
        return(h5.File(os.path.join(os.getcwd(), fileName), 'r'))

    def listRaw(self, folder):
        s3 = boto3.client('s3', aws_access_key_id = self.awsAccessKey, aws_secret_access_key = self.awsSecretKey)
        response = s3.list_objects_v2(Bucket = self.bucket, Prefix=(folder +'/raw/'))

        rawList = []
        tempStr = ''
        for thing in response.get('Contents', []):
            tempStr += thing['Key']

            if(tempStr[-3:] == 'df5'):
                formatStr = tempStr.replace((folder +'/raw/'), '')
                rawList.append(formatStr)
                tempStr, formatStr = '',  ''

        return rawList
    
    def listProcessed(self, folder):
        s3 = boto3.client('s3', aws_access_key_id = self.awsAccessKey, aws_secret_access_key = self.awsSecretKey)
        response = s3.list_objects_v2(Bucket = self.bucket, Prefix=(folder +'/img/'))

        imgList = []
        tempStr = ''
        for thing in response.get('Contents', []):
 
            tempStr += thing['Key']

            if(tempStr[-3:] == 'df5' or tempStr[-3:] == 'png'):
                formatStr = tempStr.replace((folder +'/img/'), '')
                imgList.append(formatStr)
                tempStr, formatStr = '',  ''

        return imgList

    def findRaw(self, folder):
        regex = r"(\d+)-(\d+).hdf5"

        listR = self.listRaw(folder)
        test_str = ""

        for item in listR:
            test_str += item + '\n'

        matches = re.finditer(regex, test_str, re.MULTILINE)

        for matchNum, match in enumerate(matches, start=1):
            print ("Match {matchNum} was found at {start}-{end}: {match}".format(matchNum = matchNum, start = match.start(), end = match.end(), match = match.group()))
    
        for groupNum in range(0, len(match.groups())):
            groupNum = groupNum + 1
            print ("Group {groupNum} found at {start}-{end}: {group}".format(groupNum = groupNum, start = match.start(groupNum), end = match.end(groupNum), group = match.group(groupNum)))
        


    #def getRawDataByLine(self, scanId, lineNumber):
    
