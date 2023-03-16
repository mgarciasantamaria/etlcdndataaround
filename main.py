#!/usr/bin/env python
#_*_ codig: utf8 _*_
import sys, traceback, gzip, time, datetime, psycopg2, json, os, re
from Modules.functions import *
from Modules.constants import *

if __name__ == '__main__':
    date_log=str(datetime.datetime.strftime(datetime.datetime.now(), "%Y-%m-%d"))
    if Flag_Status('r'):    
        try:
            logs_List=Download_Logs(date_log)
            #logs_Path="C:/Users/mgarcia/Downloads/logs/"
            #logs_List=os.listdir(logs_Path)
            print(f"Log list: {logs_List}")
            postgresql=psycopg2.connect(data_base_connect)
            curpsql=postgresql.cursor()
            for file_path in logs_List:
                beginning=time.time()
                with gzip.open(f'{file_path}', 'rt') as file:
                    for line in file:
                        quantity+=1
                        if '#' in line:
                            pass
                        else:
                            columns=line.split('\t')
                            Uri=columns[7].split('/')
                            Status_Validate='200'==columns[8] or '206'==columns[8]
                            Query_Validate=not(columns[11]=='-') and len(columns[11].split('&'))==3
                            Manifest_Validate='index.m3u8'== Uri[-1] or 'index.mpd'==Uri[-1] or 'Manifest'== Uri[-1]
                            if Manifest_Validate and Status_Validate  and Query_Validate:
                                SQL="INSERT INTO manifests VALUES(%s,%s,%s);"
                                DATA=(
                                    columns[0]+' '+columns[1],      # datetime
                                    re.search(r"&Signature=(.{20}).*", columns[11]).groups()[0], # signature
                                    profile[Uri[-1]],               # Segment Duration
                                    )
                                curpsql.execute(SQL,DATA)
                                count_newmanifest+=1
                            else: 
                                Segment_Validate= re.search(r"index_video_\d\d?_\d_(\d+).mp4", Uri[-1]) or re.search(r"Fragments\(v=(\d+)\)", Uri[-1]) or re.search(r"index_\d_(\d+).ts", Uri[-1])
                                if Status_Validate and Query_Validate and Segment_Validate:
                                    SQL="INSERT INTO segments VALUES(%s, %s, %s);"
                                    DATA=(
                                        columns[0]+' '+columns[1],      # datetime
                                        re.search(r"&Signature=(.{20}).*",columns[11]).groups()[0], # signature
                                        Segment_Validate.groups()[0],                # segmento
                                        )    
                                    curpsql.execute(SQL, DATA)
                                    count_newsegments+=1
                                    #postgresql.commit()
                postgresql.commit()
                finish=time.time()
                dict_summary['Log_summary']=file_path.split('/')[-1]
                dict_summary['Lines_processed']=str(quantity)
                dict_summary['New_manifests_Registered']=str(count_newmanifest)
                dict_summary['New_segments_Registered']=str(count_newsegments)
                dict_summary['Process_duration']=str(round((finish-beginning),3))
                dict_summary_srt=json.dumps(dict_summary, sort_keys=False, indent=8)
                print_log("a", dict_summary_srt, date_log)
                os.remove(file_path)
                dict_summary={}
                count_newmanifest=0
                count_VmxSegments=0
                count_newsegments=0
                quantity=0
            curpsql.close()
            postgresql.close()
        except:
            finish=time.time()
            Flag_Status("w")
            curpsql.close()
            postgresql.close() #Postgresql
            error=sys.exc_info()[2]
            errorinfo=traceback.format_tb(error)[0]
            error2=str(sys.exc_info()[1])
            dict_summary['Log_summary']=file_path.split('/')[-1]
            dict_summary['Lines_processed']=str(quantity)
            dict_summary['Process_duration']=str(round((finish-beginning),3))
            dict_summary['Log_Error']={
                'Error': str(sys.exc_info()[1]),
                'error_info': errorinfo
            }
            dict_summary_srt=json.dumps(dict_summary, sort_keys=False, indent=8)
            print_log("a", dict_summary_srt, date_log)
            mail_subject='Error DataAnaylitics'
            SendMail(dict_summary_srt, mail_subject)
    else:
        text_print="awsupload application failure not recognized\n"
        print_log("a", text_print, date_log)
        quit()
    