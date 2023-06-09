#!/usr/bin/env python
#_*_ codig: utf8 _*_
import sys, traceback, gzip, time, datetime, psycopg2, json, os, re
from Modules.functions import *
from Modules.constants import *

if __name__ == '__main__':
    date_log=str(datetime.datetime.strftime(datetime.datetime.now(), "%Y-%m-%d")) #Se recoge el dato de fecha y hora en el instante en que se ejecuta el codigo.
    if Flag_Status('r'): #Se consulta el estado de la bandera "FLAG" en el archivo json.    
    #Si el estado es true se ejecuta el proceso.
        try:
            logs_List=Download_Logs(date_log) #Se recoge la lista de los archivos logs descargados que entrega la funcion Download_Logs.
            print(f"Log list: {logs_List}") #Se imprime en pantalla la lista de los logs.
            postgresql=psycopg2.connect(data_base_connect) #Se establece conexion con la base de datos.
            curpsql=postgresql.cursor() #Se activa el cursor en la base de datos.
            for file_path in logs_List: #Se itera dentro de la lista de logs selecionando uno por uno.
                beginning=time.time() #Captura del dato de hora y fecha
                with gzip.open(f'{file_path}', 'rt') as file: #Se abre el archivo para acceder a los datos.
                    for line in file: #Se itera sobre el archivo recogiendo los datos linea a linea.
                        quantity+=1 #Contador de lineas procesadas.
                        if '#' in line: #Si en la linea selecionada se encuentra el caracter '#' se ignora.
                            pass
                        else:
                            columns=line.split('\t') #Se crea una lista con cada dato en la linea separado por tabulacion.
                            Uri=columns[7].split('/') #Los datos de la lista columns en la posicion 8 se separan por el caracter '/' y se crea una lista con los datos separados.
                            Status_Validate='200'==columns[8] or '206'==columns[8] #Se valida que el dato de la lista columns en la posicion 9 corresponda con el esperado. 
                            Query_Validate=columns[11]!='-' #Se valida que el dato de la lista columns en la posicion 12 no este vacio.
                            Manifest_Validate='index.m3u8'== Uri[-1] or 'index.mpd'==Uri[-1] or 'Manifest'== Uri[-1] #Se valida con el dato de la lista Uri en la utima posicion corresponda con lo esperado. Esto identifica si los datos corresponden a un manifest.
                            if Manifest_Validate and Status_Validate  and Query_Validate: #Condicion que evalua si todas lar variables son true.
                                user_agent=columns[9] #Se guarda en la variable user_agent el dato contenido en la lista column en la posicion 9
                                curpsql.execute(f"SELECT id_user_agent FROM device_catalog WHERE user_agent LIKE '{user_agent}';") #Consulta la base de datos el dato de la variable user_agent
                                id_user_agent=curpsql.fetchall() #Captura del dato que devuelve la consulta
                                if id_user_agent == []: #Si el dato devuelto por la consulta es una lista vacia.
                                    SQL="INSERT INTO device_catalog (user_agent,name_device) VALUES(%s,%s);" #Secuencia SQL para insertan datos en la tabla device_catalog 
                                    DATA=( #Datos para insertar con la secuencia anterior
                                        user_agent, #Dato user_agent tomado del log
                                        "STB" #Nombre del dispositivo
                                    )
                                    curpsql.execute(SQL,DATA) #Comando para ejecutar la secuencia SQL anterior
                                    curpsql.execute(f"SELECT id_user_agent FROM device_catalog WHERE user_agent LIKE '{user_agent}';") #Consulta la base de datos el dato de la variable user_agent
                                    id_user_agent=curpsql.fetchall() #Captura del dato que devuelve la consulta anterior
                                curpsql.execute(f"SELECT id_mso FROM mso WHERE mso_name LIKE '{mso}';") #Consulta a la base de datos el dato de la variable mso 
                                mso_id=curpsql.fetchall() #Captura del dato que devuelve la consulta anterior
                                if mso_id == []: #Si el dato que devuelve la consulta es una lista vacia
                                    SQL="INSERT INTO mso (mso_name,mso_country) VALUES(%s,%s);" #Secuencia SQL que permite insertar datos en la tabla mso
                                    DATA=( #Datos insertar en la tabla mso
                                        mso, #Nombre del mso
                                        "UY" #Country del mso
                                    )
                                    curpsql.execute(SQL,DATA) #Se ejecuta la secuencia SQL anterior
                                    curpsql.execute(f"SELECT id_mso FROM mso WHERE mso_name LIKE '{mso}';") #Consulta a la base de datos el dato de la variable mso 
                                    mso_id=curpsql.fetchall() #Captura del dato que devuelve la consulta anterior
                                SQL="INSERT INTO manifest VALUES(%s,%s,%s,%s,%s);" #Sentencia SQL para insertar datos en la tabla mnifests de la base de datos
                                DATA=(#Datos a registrar con la sentencia anterior.
                                    re.search(r"(.{15}).*", columns[14]).groups()[0],   # manifestid
                                    id_user_agent[0][0],                                # id_user_agent
                                    mso_id[0][0],                                       # id_mso_fk
                                    re.search(r"/out/v1/(.*)", columns[7]).groups()[0], # uri cs-uri-steam
                                    columns[0]+' '+columns[1],                          # datetime
                                    )
                                print(DATA)
                                curpsql.execute(SQL,DATA) #Se envia a la base de datos la sentencia a ejecutar.
                                count_manifest+=1 #Contador de manifest registrados
                            else: 
                                Segment_Validate= re.search(r"index_video_\d\d?_\d_(\d+).mp4", Uri[-1]) or re.search(r"Fragments\(v=(\d+)\)", Uri[-1]) or re.search(r"index_\d_(\d+).ts", Uri[-1]) #Se recoge el valor del dato en la lista Uri en la utima posicion segun la coinsidencia.
                                if Status_Validate and Query_Validate and Segment_Validate: #Condicion que evalua si todas las varibles son true.
                                    SQL="INSERT INTO segmentos (manifestid_fk,create_date) VALUES(%s,%s);" #Sentencia SQL para insertar datos en la tabla segements de la base de datos.
                                    DATA=(#Datos a registrar con la sentencia anterior
                                        re.search(r"&id=(.{15})",columns[11]).groups()[0], #manifestid
                                        columns[0]+' '+columns[1],                      # datetime
                                        )
                                    print(DATA)   
                                    curpsql.execute(SQL, DATA) #Se envia a la base de datos la sentencia a ejecutar.
                                    count_segments+=1 #Contador de segmentos registrados.
                postgresql.commit() #Se guarda los cambios realizados en la base de datos.
                finish=time.time() #Captura del tiempo en el instante que termina de procesar un log.
                dict_summary['Log_summary']=file_path.split('/')[-1] #Se agrega al resumen el nombre del log procesado.
                dict_summary['Lines_processed']=str(quantity) #Se agrega al resumen la cantidad total de lineas procesadas.
                dict_summary['New_manifests_Registered']=str(count_manifest) #Se agrega al resumen la cantidad total de manifests registrados.
                dict_summary['New_segments_Registered']=str(count_segments) #Se agrega al resumen la cantidad total de segmentos registrados.
                dict_summary['Process_duration']=str(round((finish-beginning),3)) #Se agrega al resumen el calculo de la duracion total de la ejecucion del codeigo.
                dict_summary_srt=json.dumps(dict_summary, sort_keys=False, indent=8) #Se transforma el diccionario a formato texto.
                print_log(dict_summary_srt, date_log) #Se registra en el log de eventos el resumen.
                print(dict_summary_srt)
                #os.remove(file_path) #Se elimnina el log procesado del folde local.
                dict_summary={} #se limpia el diccionario.
                #Se establecen los contadores a cero.
                count_manifest=0 
                count_segments=0
                quantity=0
            curpsql.close() #Se cierra la conexion con el cursor de la base de datos.
            postgresql.close() #Se cierra la conexion con la base de datos.
        except:
            finish=time.time() #Captura del tiempo en el instante que termina de procesar un log.
            Flag_Status("w") #Se cambia el estado de la bandera "FLAG" a false.
            curpsql.close() #Se cierra la conexion con el cursor de la base de datos.
            postgresql.close() #Se cierra la conexion con la base de datos.
            error=sys.exc_info()[2] #Captura del error generado por el sistema.
            errorinfo=traceback.format_tb(error)[0] #Cartura del detalle del error.
            dict_summary['Log_summary']=file_path.split('/')[-1] #Se agrega al resumen el nombre del log procesado.
            dict_summary['Lines_processed']=str(quantity) #Se agrega al resumen la cantidad total de lineas procesadas.
            dict_summary['Process_duration']=str(round((finish-beginning),3)) #Se agrega al resumen el calculo de la duracion total de la ejecucion del codigo.
            #Se agrega al resumen detalle del error ocurrido.
            dict_summary['Log_Error']={
                'Error': str(sys.exc_info()[1]),
                'error_info': errorinfo
            }
            dict_summary_srt=json.dumps(dict_summary, sort_keys=False, indent=8) #Se transforma el diccionario a formato texto.
            print(dict_summary_srt)
            print_log(dict_summary_srt, date_log) #Se registra en el log de eventos el resumen.
            mail_subject='Error DataAnaylitics' #Se establece el asunto del correo.
            #SendMail(dict_summary_srt, mail_subject) #Se envia correo electronico.
    else:
        text_print="awsupload application failure not recognized\n" #Texto a imprimir o registrar.
        print_log(text_print, date_log) #Se registra el texto anterior en el log de eventos.
        quit() #Termina la ejecucion del codigo.
    