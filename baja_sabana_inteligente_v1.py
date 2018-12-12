# -*- coding: utf-8 -*-
"""
Created on Tue Dec 11 15:02:30 2018

@author: Alvaro Gutierrez Vargas
"""
'''================='''
import pandas as pd
#import tia.bbg.datamgr as dm
import time
import csv
from time import gmtime, strftime
#from SendSabana import SendSabana
import re
from datetime import datetime, timedelta
import os



'''Eleccion de la fecha de inicio '''
idate='1/1/2006'

'''=======----ARCHIVO TICKERS----=========='''
#Archivo con los tickers debe ser un xlsx y estar en el mismo directorio
path_inicial = os.getcwd()
tickers = pd.read_excel('tickers.xlsx')
#Tomamos los tickers de la data a bajar y lo convertimos en una lista
tickers_por_bajar = tickers.iloc[:,0].values.tolist()
'''======================================='''


'''=========================================='''
'''===---IDENTIFICANDO DATA DISPONIBLE ---==='''
'''=========================================='''
'''#(1)'''
#Creando una lista con los archivos en el directorio.
files_in_directory = [f for f in os.listdir('.') if os.path.isfile(f)]        
'''Recuperando todas las posibles SABANAS en el directorio'''
lista_sabanas=[]
for ss in files_in_directory:
    if ss[:6]=='SABANA':
        lista_sabanas.append(ss)

'''#(2)'''
lista_sabanas_dia = []
SABANA_FUENTE = []
for ff in set(lista_sabanas):
    if ff[:10] =='SABANA_DIA':
        lista_sabanas_dia.append(ff)
    elif ff[:13] =='SABANA_FUENTE':
        SABANA_FUENTE.append(ff)    

'''#(3)'''            
#El not not : se leeria: "es no nula"
parsed_lista_sabanas_dia=[]    
if  not not lista_sabanas_dia:
    for gg in lista_sabanas_dia:
        parse_text = gg.split("_")
        parsed_lista_sabanas_dia.append(parse_text[4])
        
    #Recuperando los ID
    ID_list=[int(x[0:-4]) for x in parsed_lista_sabanas_dia]
    ID_maximo=max(ID_list)
    
    #Eligiendo la base mas actual en la lista (la con mayor ID)
    #Contando la cantidad de caracteres del ID maximo
    largo_ID_maximo= len(str(ID_maximo))
    #recuperando sabana asociada a dicho ID maximo
    SABANA_DIA_ID_MAX = []
    for qq in lista_sabanas_dia:
        #los +4 que se le suman son por los q caracteres de ".txt"
        if qq[-(largo_ID_maximo+4):]==str(ID_maximo)+'.txt':
            SABANA_DIA_ID_MAX.append(qq)
    #recuperando sabana asociada a dicho ID maximo
    SABANA_DIA_ID_MAX =SABANA_DIA_ID_MAX[0]        
    #Declaramos sabana_anterior igual a la sabana con el ID_MAXIMO



'''========================================================='''
'''===---IDENTIFICANDO NUMERO DE EJECICION---==='''
'''========================================================='''

if  not not lista_sabanas_dia:
    '''=============N-ESIMA EJECUCION============='''
    sabana_anterior=SABANA_DIA_ID_MAX[0]
elif not not SABANA_FUENTE:
    '''=============2DA EJECUCION============='''
    sabana_anterior= SABANA_FUENTE[0]
    
    '''==================================================='''
    '''=======----   ACTUALIZANDO LA DATA   ----=========='''
    '''==================================================='''
    #Ahora nos preguntamos: ¿Existe alguno adicional a los que ya teniamos?
    with open(sabana_anterior) as f:
      reader = csv.reader(f)
      row1 = next(reader)  # gets the first line
    #separamos las columnas de la lista creada por tab
    tickers_antiguos=re.split(r'\t+', row1[0])
    #eliminamos la palabra date de la lista de activos ya creados
    tickers_antiguos.remove('date')
    #Analizamos cuales son los nuevos activos que se quieren descargar
    union=list(set(tickers_antiguos).union(tickers_por_bajar))
    intersection=list(set(tickers_antiguos).intersection(tickers_por_bajar))
    #generamos una lista con los activos que no se encuentran en la base que 
    #tenemos cargada
    tickers_nuevos=set(union) - set(intersection)
    
    '''===================================='''
    '''=========Tickers Antiguos==========='''
    '''===================================='''
    #Para la data que ya tenemos en la base de datos
    TDate=time.strftime("%m/%d/%Y")
    #Cargo toda la sabana que tenemos, para recuperar la Fecha de inicio
    # y para consolidar todo en una solo dataframe a imprimir en disco.-    
    try:
        dateparse = lambda x: pd.datetime.strptime(x, '%Y-%m-%d')
        base=pd.read_csv(sabana_anterior, sep='\t',parse_dates=['date'], date_parser=dateparse)
    except ValueError:
        dateparse = lambda x: pd.datetime.strptime(x, '%Y-%m-%d')
        base=pd.read_csv(sabana_anterior, sep=',',parse_dates=['date'], date_parser=dateparse)    
    """ toma la columna date y colocala como indice del df """
    base = base.set_index('date')        
    avanza_un_dia =1
    f_aux = base.index[-1] + timedelta(days=avanza_un_dia)
    star_old= f_aux.strftime("%m/%d/%Y") 
    #Actualizo la informacion para los Tickers Antiguos 
    for i in range(len(tickers_antiguos)): #Descargando todos los tickers nuevos
        mgr = dm.BbgDataManager()
        sids= mgr[tickers_antiguos[i]]
        if i==0:
            tickers_old = sids.get_historical(['PX_LAST'],start= star_old, end= TDate)
            tickers_old.columns = [tickers_antiguos[i]]
        else:
            tickers_old_i = sids.get_historical(['PX_LAST'],start= star_old, end= TDate)
            tickers_old_i.columns = [tickers_antiguos[i]]
            tickers_old= pd.concat([tickers_old,tickers_old_i],axis=1)        
    
    #Append de la data actualizada        
    tickers_update=base.append(tickers_old, ignore_index = True)
    
    '''===================================='''
    '''=======  Tickers Nuevos ============'''
    '''===================================='''
    #Si y solo si existen tickers entonces baja esos datos y hace el concat con lo que ya bajo
    if bool(tickers_nuevos) :
        #ocupamos como fecha de inicio donde parte la base que cargamos recien.
        start_new_aux= base.index[0]
        
        start_new= start_new_aux.strftime("%m/%d/%Y") 
        for i in range(len(tickers_nuevos)): #Descargando todos los tickers nuevos
            mgr = dm.BbgDataManager()
            sids= mgr[tickers_nuevos[i]]
            if i==0:
                tickers_new= sids.get_historical(['PX_LAST'],start= start_new, end= TDate)
                tickers_new.columns = [tickers_nuevos[i]]
            else:
                tickers_new_i = sids.get_historical(['PX_LAST'],start= start_new, end= TDate)
                tickers_new_i.columns = [tickers_nuevos[i]]
                tickers_new = pd.concat([tickers_new,tickers_new_i],axis=1)
                
        #Concat base con antiguos acutalizados mas los nuevos tickers
        tickers_update = pd.concat([tickers_update, tickers_new], axis=1, join_axes=[tickers_update.index])
    '''===Escribiendo la sabana actualizada en el disco====='''    
    hora=strftime("%Y-%m-%d %H:%M:%S" )
    hora=hora.replace(" ","_")
    hora=hora.replace(":","_")
    tickers_update.to_csv('SABANA_DIA'+hora+'.txt', sep='\t')   

    #SendSabana(directorio+'sabana_TIA_'+hora+'.txt')

else :
    '''=============1RA EJECUCION============='''
    TDate=time.strftime("%m/%d/%Y")
    
    for i in range(len(tickers_por_bajar)): #Descargando todos los tickers nuevos
        mgr = dm.BbgDataManager()
        sids= mgr[tickers_por_bajar[i]]
        if i==0:
            sabana_1st = sids.get_historical(['PX_LAST'],start= idate, end= TDate)
            sabana_1st.columns = [tickers_por_bajar[i]]
        else:
            sabana_1st_i = sids.get_historical(['PX_LAST'],start= idate, end= TDate)
            sabana_1st_i.columns = [tickers_por_bajar[i]]
            sabana_1st = pd.concat([sabana_1st,sabana_1st_i],axis=1)  
    
    
    hora=strftime("%Y-%m-%d %H:%M:%S" )
    hora=hora.replace(" ","_")
    hora=hora.replace(":","_")
    sabana_1st.to_csv('SABANA_FUENTE'+hora+'.txt', sep='\t')   
     
    
    