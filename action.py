#!/usr/bin/python
# -*- coding: utf-8 -*-

import datetime
import gzip
from qcloud_cos import CosClient
from qcloud_cos import UploadFileRequest
from qcloud_cos import DownloadFileRequest
import logging

logger = logging.getLogger()

# Download files
def download_file(cos_client, bucket, key, local_file_path):
    request = DownloadFileRequest(bucket, key, local_file_path)
    download_file_ret = cos_client.download_file(request)
    if download_file_ret['code'] == 0:
        logger.info("Download file [%s] Success" % key)
        return 0
    else:
        logger.error("Download file [%s] Failed, err: %s" % (key, download_file_ret['message']))
        return -1

#  Upload file to bucket
def upload_file(cos_client, bucket, key, local_file_path):
    request = UploadFileRequest(bucket.decode('utf-8'), key.decode('utf-8'), local_file_path.decode('utf-8'))
    upload_file_ret = cos_client.upload_file(request)
    if upload_file_ret['code'] == 0:
        logger.info("Upload data map file [%s] Success" % key)
        return 0
    else:
        logger.error("Upload data map file [%s] Failed, err: %s" % (key, upload_file_ret['message']))
        return -1

def action_handler(event, context):

    #Create CosClient to upload/download COS file
    appid = 1253142785      # change to user's appid
    secret_id = u'xxx'   # change to user's secret_id
    secret_key = u'xxx'  # change to user's secret_key
    region = u'sh'          # change to user's region
    cos_client = CosClient(appid, secret_id, secret_key, region)

    #specify the source and destination bucket location
    source_bucket = event['Records'][0]['cos']['cosBucket']['name']
    source_bucket_file_key = '/' + event['Records'][0]['cos']['cosObject']['key'].split('/')[-1]
    source_file_name = source_bucket_file_key.split('/')[-1].split('.')[0]
    dest_bucket = u'output'
    dest_bucket_file_key = u'/max_temperature_'+ source_file_name

    #specify the temp file location
    source_file_tmp_path = u'/tmp/' + source_file_name
    dest_file_temp_path = u'/tmp/max_temperature_' + source_file_name

    #download the source file from cos bucket and take actions
    download_ret = download_file(cos_client,source_bucket,source_bucket_file_key,source_file_tmp_path)
    if download_ret == 0:
        dest_file_temp = open(dest_file_temp_path, 'w')
        max_temp = -999.9

        #find the maximum temperature
        with gzip.open(source_file_tmp_path) as inputfile:
            for line in inputfile:
                temp = int(line[14:19]) / 10.0
                if temp > max_temp:
                    max_temp = temp

        #write the result to the temp file and upload to the cos bucket
        dest_file_temp.write(source_file_name + ' ' + str(max_temp))
        dest_file_temp.close()
        upload_ret = upload_file(cos_client, dest_bucket, dest_bucket_file_key, dest_file_temp_path)
        return upload_ret
    else:
        return -1


def main_handler(event, context):
    start_time = datetime.datetime.now()
    print("Action Started")
    res = action_handler(event, context)
    end_time = datetime.datetime.now()
    print("Action duration: " + str((end_time-start_time).microseconds/1000) + "ms")
    if res == 0:
        return "Action Done"
    else:
        return "Action Failed"
