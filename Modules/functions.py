#!/usr/bin/env python
#_*_ codig: utf8 _*_
import datetime, json, os, boto3, smtplib, datetime
from email.message import EmailMessage
from Modules.constants import *

def SendMail(text, mail_subject):
    msg = EmailMessage()
    msg.set_content(text)
    msg['Subject'] = mail_subject
    msg['From'] = 'alarmas-aws@vcmedios.com.co'
    msg['To'] = [Mail_To]
    conexion = smtplib.SMTP(host='10.10.122.17', port=25)
    conexion.ehlo()
    conexion.send_message(msg)
    conexion.quit()
    return

def Download_Logs(DATE_LOG):
    objects={'Objects':[]}
    list_objects=[]
    aws_session=boto3.Session(profile_name=aws_profile)
    s3_client=aws_session.client('s3')
    logs=s3_client.list_objects_v2(Bucket=Bucket_logs)
    if 'Contents' in logs:    
        for i in range(len(logs['Contents'])):
            log_Key=logs['Contents'][i]['Key']
            with open(f'{Downloads_Path}/{log_Key}', 'wb') as data:
                s3_client.download_fileobj(Bucket_logs, log_Key, data)
            objects['Objects'].append({'Key': log_Key,})
            list_objects.append(f"{Downloads_Path}/{log_Key}")
            s3_client.copy_object(
                Bucket=Bucket_logs_old,
                CopySource=f'{Bucket_logs}/{log_Key}',
                Key=f'{log_Key}'
                )
            
        s3_client.delete_objects(
                Bucket=Bucket_logs,
                Delete=objects
            )
        return list_objects
    else:
        text_print=f"Logs not found"
        print_log("a",text_print,DATE_LOG)
        return []

def print_log(OPTION, TEXT, DATE_LOG):
    log_file=open(f"{log_Path}/{DATE_LOG}_log.txt", OPTION)
    log_file.write(f"{str(datetime.datetime.strftime(datetime.datetime.now(), '%H:%M:%S'))}\t{TEXT}\n")
    log_file.close()

def Flag_Status(OPTION):
    with open(json_path, "r") as json_file:
            json_data=json.load(json_file)
    if OPTION=="r":
        return json_data["FLAG"]
    elif OPTION=="w":
        json_data["FLAG"]=False
        with open(json_path, "w") as json_file:
            json.dump(json_data, json_file)
    else:
        pass