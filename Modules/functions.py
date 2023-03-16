#!/usr/bin/env python
#_*_ codig: utf8 _*_
import datetime, json, os, boto3, smtplib, datetime
from email.message import EmailMessage
from Modules.constants import * #Se importan las constantes definidas en el archivo constants.py

#*****************************************************************************************************************************************************************************************************************************
#Se define la función llamada SendMail que toma dos argumentos: text y mail_subject.
#La función tiene como objetivo enviar un correo electrónico con el contenido
# y el asunto proporcionados a una dirección de correo electrónico especificada.
def SendMail(text, mail_subject):
    msg = EmailMessage() #crea una nueva instancia de la clase EmailMessage() y la asigna a la variable msg. Esta clase se utiliza para crear y enviar mensajes de correo electrónico.
    msg.set_content(text) #se establece el contenido del correo electrónico como el valor del argumento text que se pasó a la función.
    msg['Subject'] = mail_subject #establece el asunto del correo electrónico como el valor del argumento mail_subject que se pasó a la función.
    msg['From'] = 'alarmas-aws@vcmedios.com.co' #Establece la dirección de correo electrónico del remitente como alarmas-aws@vcmedios.com.co.
    msg['To'] = [Mail_To] #Establece la dirección de correo electrónico del destinatario como el valor de una constante llamada Mail_To.
    conexion = smtplib.SMTP(host='10.10.122.17', port=25) #Se establece una conexión con un servidor SMTP que se encuentra en la dirección IP 10.10.122.17 y el puerto 25. Esta conexión se asigna a la variable conexion.
    conexion.ehlo() #Se utiliza el comando EHLO (abreviatura de "Hola extendido") para iniciar una conexión con el servidor SMTP.
    conexion.send_message(msg) #se envía el mensaje de correo electrónico utilizando la conexión establecida anteriormente.
    conexion.quit() #se cierra la conexión SMTP.
    return #devuelve none.
#*****************************************************************************************************************************************************************************************************************************

#****************************************************************************************************************************************************************
#Se define la función llamada Download_Logs, que toma un argumento llamado DATE_LOG. 
#La función tiene como objetivo descargar todos los archivos de registro de un Bucket 
# de Amazon S3 y guardarlos en una ruta local.
def Download_Logs(DATE_LOG):
    objects={'Objects':[]} #Se inicializa la variable como un diccionario vacio.
    list_objects=[] #Se inicializa la variable como una lista vacia.
    aws_session=boto3.Session(profile_name=aws_profile) #Se crea una sesion de AWS bajo el perfil definido en la constante aws_profile.
    s3_client=aws_session.client('s3') #Se establece conexion con el servicio S3.
    logs=s3_client.list_objects_v2(Bucket=Bucket_logs) #se listan todos los objetos (archivos) en el Bucket definido en la constante Bucket_logs de S3.
    if 'Contents' in logs: #Se comprueba si hay objetos en el bucket.   
        for i in range(len(logs['Contents'])): #Si hay objetos se itera sobre cada objeto.
            log_Key=logs['Contents'][i]['Key'] #Captura de la ruta del objeto.
            with open(f'{Downloads_Path}/{log_Key}', 'wb') as data: #Se crea un archivo en el folder local.
                s3_client.download_fileobj(Bucket_logs, log_Key, data) #Se descarga el objeto y se copia el contenido al archivo anteriormente creado.
            objects['Objects'].append({'Key': log_Key,}) #Se agrega al dicionario el nombre del objeto descargado.
            list_objects.append(f"{Downloads_Path}/{log_Key}") #Se agrega la ruta completa del objeto descargado en el folder local.
            #Se realiza un a copia del objeto en el bucket definido en la constante Bucket_logs_old.
            #s3_client.copy_object( 
                # Bucket=Bucket_logs_old, #Se establece el bucket de destino.
                # CopySource=f'{Bucket_logs}/{log_Key}', #Se establece la ruta del objeto en el bucket de origen.
                # Key=f'{log_Key}' #Se establece el nombre del objeto en el destino.
                # )
        #Se eliminan los objetos en el bucket de origen.    
        # s3_client.delete_objects(
        #         Bucket=Bucket_logs, #Se establce el bucket.
        #         Delete=objects #Se establce la lista de objetos a eliminar.
        #     )
        return list_objects #Retorna la lista de objetos cada uno con su ruta en el folder local.
    else:
        text_print=f"Logs not found" #Se define el texto a registrar en el log de eventos.
        print_log(text_print,DATE_LOG) #Se registra el texto en el log de eventos.
        return [] #Retorna lista vacia.
#*****************************************************************************************************************************************************************

#*********************************************************************************************************************************************************************************
#Se define la funcion llamada print_log, que toma dos argumentos llamados TEXT, DATE_LOG.
#La funcion tiene como objetivo registrar en un archivo txt un texto definido en la variable TEXT.
def print_log(TEXT, DATE_LOG):
    log_file=open(f"{log_Path}/{DATE_LOG}_log.txt", "a") #Se abre el archivo de la ruta especificada. Si el archivo no existe este se crea. 
    log_file.write(f"{str(datetime.datetime.strftime(datetime.datetime.now(), '%H:%M:%S'))}\t{TEXT}\n") #Se escribe en el archivo log el texo especificado en el argumento TEXT.
    log_file.close() #Se cierra el archivo.
#**********************************************************************************************************************************************************************************

#****************************************************************************************************************
#Se define la funcion llamada Flag_status, que toma un argumento llamado OPTION.
#La funcion tiene como objetivo leer y escribir un archivo json.
def Flag_Status(OPTION):
    with open(json_path, "r") as json_file: #Se abre el archivo json especificado en la constante json_path.
            json_data=json.load(json_file) #Se lee el contenido del archivo.
    if OPTION=="r": #Comprueba si el argumento OPTION es igual al caracter 'r'.
        return json_data["FLAG"] #Retorna el value de la llave "FLAG".
    elif OPTION=="w": #Comprueba si el argumento OPTION es igual al caracter 'w'.
        json_data["FLAG"]=False #Se establece el value de la llave "FLAG" en false.
        with open(json_path, "w") as json_file: #Se abre el archivo json.
            json.dump(json_data, json_file) #Se escribe en el archivo json.
    else:
        pass
#***************************************************************************************************************