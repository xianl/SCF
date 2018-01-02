#!/usr/bin/python
# -*- coding: utf-8 -*-

import os
from ftplib import FTP
from qcloud_cos import CosClient
from qcloud_cos import UploadFileRequest
from qcloud_cos import CreateFolderRequest

def ftpconnect():
    ftp = FTP()
    ftp.connect('ftp.ncdc.noaa.gov', 21)
    ftp.login('','')
    return ftp

def GetCosClient():
    appid = 1253142785
    secret_id = u'xxxx'    #change to your own secret_id
    secret_key = u'xxxx'   #change to your own secret_key
    region_info = "sh"     #change to your own region
    cos_client = CosClient(appid, secret_id, secret_key, region=region_info)
    return cos_client

def uploadfile():

    ftp = ftpconnect()
    myCosClient = GetCosClient()

    datapath = "/pub/data/noaa/isd-lite/"
    beginyear = 2017
    endyear=2017
    year = beginyear

    while year <= endyear:
        ftppath = datapath + str(year)
        filelist = ftp.nlst(ftppath)

        cospath = u'/' + str(year)+ u'/'
        ret = myCosClient.create_folder(CreateFolderRequest(u'fredcos',cospath))
        print ret

        for file in filelist:
            localfilepaths = file.split("/")
            localfilepath = localfilepaths[len(localfilepaths) - 1]
            bufsize = 1024

            localfile = open(localfilepath, 'wb')
            ftp.retrbinary('RETR ' + file, localfile.write, bufsize)
            cosfilepath = cospath + localfilepath
            request = UploadFileRequest(u'fredtest', cosfilepath, localfilepath.decode("utf-8"))
            localfilepath.decode()
            upload_file_ret = myCosClient.upload_file(request)
            print upload_file_ret
            localfile.close()

            if os.path.exists(localfilepath):
                os.remove(localfilepath)

        year = year + 1
    ftp.close()

if __name__ == "__main__":
    uploadfile()




