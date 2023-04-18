#!/usr/bin/env python
#_*_ codig: utf8 _*_
json_path="./json/vars.json"
log_Path="./Logs" # Test
Downloads_Path="./S3Download"

data_base_connect="host=10.10.130.38 dbname=aroundtest user=vodtransfers3 password=vod-2022" #use (main) (functions:extract_xml_data, Duration_Transform)
profile={   #Use (main)
    'index.mpd': 4,
    'index.m3u8' : 6,
    'Manifest' : 3
    }
Bucket_logs='montecablelogs' #Use (functions:Dowload_Logs)
Bucket_logs_old='montecablelogs-old' #Use (functions:Dowload_Logs)
aws_profile='pythonapps' #Use (functions:Dowload_Logs, extract_xml_data)
Mail_To='mgarcia@vcmedios.com.co' #Use (functions:SendMail)
mso='Montecable_Uy'

dict_summary={}
count_manifest=0
count_segments=0
quantity=0

    