#!/usr/bin/env python
#_*_ codig: utf8 _*_
import sys, traceback, gzip, time, datetime, psycopg2, json, os
from Modules.functions import *
from Modules.constants import *

if __name__ == '__main__':
    date_log=str(datetime.datetime.strftime(datetime.datetime.now(), "%Y-%m-%d"))
    if Flag_Status('r'):    
        try:
            #logs_List=Download_Logs(date_log)
            logs_Path="C:/Users/mgarcia/Downloads/logs/"
            logs_List=os.listdir(log_Path)
            print(f"Log list: {logs_List}")
            #postgresql=psycopg2.connect(data_base_connect)
            #curpsql=postgresql.cursor()
            for file_path in logs_List:
                beginning=time.time()
                with gzip.open(f'{logs_Path+file_path}', 'rt') as file:
                    for line in file:
                        quantity+=1
                        if '#' in line:
                            pass
                        else:
                            columns=line.split('\t')
                            Uri=columns[7].split('/')
                            Status_Validate='200'==columns[8] or '206'==columns[8]
                            Query_Validate=not('CMCD=' in columns[11]) and not(columns[11]=='-') and len(columns[11].split('&'))==5
                            if len(Uri)==5 and Uri[1]=='prod':
                                Manifest_Validate='master.m3u8'== Uri[4] or 'stream.mpd'==Uri[4] or 'Manifest'==Uri[4]
                                if Manifest_Validate and Status_Validate  and Query_Validate:
                                    Type=columns[7].split('/')[3].split('.')[1] # NEW
                                    SQL="INSERT INTO new_manifests VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s);"
                                    DATA=(
                                        columns[0]+' '+columns[1],      # datetime
                                        columns[11].split('&')[0],      # clientid
                                        columns[11].split('&')[1],      # contentid
                                        columns[4],                     # ip
                                        columns[14][:15],               # manifestsid
                                        columns[11].split('&')[2][3:],  # country
                                        columns[11].split('&')[3][2:],  # mso
                                        columns[11].split('&')[4][2:],  # device
                                        profile[Type][0],               # Segment Duration
                                        )
                                    #****************************************************************************
                                    curpsql.execute(SQL,DATA)
                                    postgresql.commit()
                                    count_newmanifest+=1
                            elif len(Uri)>5 and Uri[1]=='prod':
                                Video_Validate=Uri[4]=='video' or 'HD' in Uri[4] or 'SD' in Uri[4] or 'video' in Uri[5]
                                Segment_Validate=not('init.mp4' in Uri[len(Uri)-1] or 'iframes.m3u8' in Uri[len(Uri)-1] or 'stream.m3u8' in Uri[len(Uri)-1] or 'video=0' in Uri[len(Uri)-1])
                                Query_Validate_seg=not('CMCD=' in columns[11]) and not(columns[11]=='-') and len(columns[11].split('&'))>=6
                                if Status_Validate and Video_Validate and Segment_Validate and Query_Validate_seg:
                                    if len(columns[11].split('?'))==1:
                                        Type=Uri[3].split('.')[1]
                                        SQL="INSERT INTO new_segmentos VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s);"
                                        DATA=(
                                            columns[0]+' '+columns[1],      # datetime
                                            columns[11].split('&')[0][2:],  # clientid
                                            columns[11].split('&')[1][2:],  # contentid
                                            columns[4],                     # ip
                                            columns[11].split('&')[5][3:],  # manifestid
                                            Uri[len(Uri)-1],                # segmento
                                            profile[Type][0],               # Duration
                                            profile[Type][1],               # Type
                                            Uri[3],                         # Mediaid
                                        )    
                                        curpsql.execute(SQL, DATA)
                                        count_newsegments+=1
                                        postgresql.commit()
                                    elif len(columns[11].split('?'))==2:
                                        Type=Uri[3].split('.')[1]
                                        Query=columns[11].split('?')[0]
                                        SQL="INSERT INTO new_segmentos VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s);"
                                        DATA=(
                                            columns[0]+' '+columns[1],      # datetime
                                            Query.split('&')[0][2:],        # clientid
                                            Query.split('&')[1][2:],        # contentid
                                            columns[4],                     # ip
                                            Query.split('&')[5][3:],        # manifestid
                                            Uri[len(Uri)-1],                # segmento
                                            profile[Type][0],               # Duration
                                            profile[Type][1],               # Type
                                            Uri[3],                         # Mediaid
                                        )    
                                        curpsql.execute(SQL, DATA)
                                        count_newsegments+=1
                                        postgresql.commit()
                                elif Status_Validate and Video_Validate and Segment_Validate:
                                    Type=Uri[3].split('.')[1]
                                    if Type=='vmxmpd' or Type=='vmxm3u8':
                                        count_VmxSegments+=1
                #text_print=f"Log {file_path}.\n\t\t\t{quantity} lines proceseed.\n\t\t\t{count_newmanifest} manifest finded and registered in Manifest table.\n\t\t\t{count_VmxSegments} Verimatrix Segments.\n\t\t\t{count_newsegments} Segments finded and registered in newSegmentos table."
                finish=time.time()
                dict_summary['Log_summary']=file_path.split('/')[8]
                dict_summary['Lines_processed']=str(quantity)
                dict_summary['New_manifests_Registered']=str(count_newmanifest)
                dict_summary['New_segments_Registered']=str(count_newsegments)
                dict_summary['Vmx_segments']=str(count_VmxSegments)
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
            postgresql.close() #Postgresqlv
            
            
        except:
            finish=time.time()
            Flag_Status("w")
            curpsql.close()
            postgresql.close() #Postgresql
            error=sys.exc_info()[2]
            errorinfo=traceback.format_tb(error)[0]
            dict_summary['Log_summary']=file_path.split('/')[8]
            dict_summary['Lines_processed']=str(quantity)
            dict_summary['New_manifests_Registered']=str(count_newmanifest)
            dict_summary['New_segments_Registered']=str(count_newsegments)
            dict_summary['Vmx_segments']=str(count_VmxSegments)
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
    