#!/usr/bin/env python
#_*_ codig: utf8 _*_
json_path="./json/vars.json"
log_Path="./Logs" # Test
Downloads_Path="./S3Download"

data_base_connect="host=10.10.130.38 dbname=cdntest user=vodtransfers3 password=vod-2022" #use (main) (functions:extract_xml_data, Duration_Transform)
profile={   #Use (main)
    'index.mpd': 4,
    'index.m3u8' : 6,
    'Manifest' : 3
    }
Bucket_logs='testaurora' #Use (functions:Dowload_Logs)
Bucket_logs_old='dbeventlogsold' #Use (functions:Dowload_Logs)
aws_profile='pythonapps' #Use (functions:Dowload_Logs, extract_xml_data)
Mail_To='mgarcia@vcmedios.com.co' #Use (functions:SendMail)

dict_summary={}
count_newmanifest=0
count_VmxSegments=0
count_newsegments=0
quantity=0

    