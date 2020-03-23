#!/usr/bin/env python3
# Copyleft Gter srl 2020
# Lorenzo Benvenuto

from datetime import datetime, timedelta
import urllib.request
import wget
import sys,os
import logging
import time
import logging #levels are DEBUG, INFO, WARNING, ERROR, CRITICAL
import psycopg2
from credenziali import *
import getopt
import urllib.request
import ftplib
#passi da fare

#1. controllare che il dato ci sia e nel caso scaricarlo. Se il dato non ci fosse scrivere su un file di log

#1.1 per scaricare il dato bisogna comporre in automatico il suo nome (quindi nomenclatura RINEX 3.02)


def rinex302filename(st_code,ST,session_interval,obs_freq,data_type,data_type_flag,bin_flag,data_format,compression):
    '''function to dynamically define the filename in rinex 3.02 format)

    Needed parameters

    1: STATION/PROJECT NAME (SPN); format XXXXMRCCC
        XXXX: station code
        M: monument or marker number (0-9)
        R: receiver number (0-9)
        CCC:  ISO Country CODE see ISO 3166-1 alpha-3 
    
    2: DATA SOURCE (DS)
        R – From Receiver data using vendor or other software
        S – From data Stream (RTCM or other)
        U – Unknown (1 character)
    
    3: START TIME (ST); fomrat YYYYDDDHHMM (UTC)
        YYYY - Gregorian year 4 digits,
        DDD  – day of year,
        HHMM - Hour and minutes of day

    4: FILE PERIOD (FP): format DDU
        DD – file period 
        U – units of file period.
        Examples:
        15M – 15 Minutes 
        01H – 1 Hour
        01D – 1 Day
        01Y – 1 Year 
        00U - Unspecified
    
    5: DATA FREQ (DF); format DDU
        DD – data frequency 
        U – units of data rate 
        Examples:
        XXS – Seconds,
        XXM – Minutes,
        XXH – Hours,
        XXD – Days
        XXU – Unspecified

    6: DATA TYPE (DT); format DD (default value are MO for obs and MN for nav)
        GO - GPS Obs.,
        RO - GLONASS Obs., 
        EO - Galileo Obs. 
        JO - QZSS Obs., 
        CO - BDS Obs., 
        IO – IRNSS Obs., 
        SO - SBAS Obs., 
        MO Mixed Obs., 
        GN - Nav. GPS, 
        RN- Glonass Nav., 
        EN- Galileo Nav., 
        JN- QZSS Nav., 
        CN- BDS Nav., 
        IN – IRNSS Nav., 
        SN- SBAS Nav., 
        MN- Nav. All GNSS Constellations 
        MM-Meteorological Observation 
        Etc

    7: FORMAT
        Three character indicating the data format:
        RINEX - rnx, 
        Hatanaka Compressed RINEX – crx, 
        ETC
    
    8: COMPRESSION
        .zip
        .gz
        .tar.gz
        etc

    '''
    filename=''

    # STATION/PROJECT NAME

    M=0 #da capire
    R=0 #da capire

    if st_code=='SAOR':
        CCC='FRA'
    else:
        CCC='ITA'
    
    SPN='{}{}{}{}_'.format(st_code,M,R,CCC)

    filename+=SPN

    # DATA SOURCE 
    DS='R'
    filename+='{}_'.format(DS)

    # START TIME
    filename+='{}_'.format(ST)
    

    # FILE PERIOD
    interval=timedelta(seconds=session_interval*60)

    if interval.days != 0 and interval.seconds//3600 ==0:
        FP='%02dD'%(interval.days)
    elif interval.days == 0 and interval.seconds//3600 !=0:
        FP='%02dH'%(interval.seconds//3600)
    else:
        FP='00U'

    filename+='{}_'.format(FP)

    # DATA FREQ
    
    freq=timedelta(seconds=obs_freq)
    #print(freq)
    #print((freq.seconds//60)%60,freq.seconds)
    if freq.seconds!=0 and (freq.seconds//60)%60==0:
        DF='%02dS'%(freq.seconds)
    elif freq.seconds==0 and (freq.seconds//60)%60!=0:
        DF='%02dM'%((freq.seconds//60)%60)
    else:
        DF='00U'

    filename+='{}'.format(DF)

    # DATA TYPE
    if data_type_flag:
        filename+='{}_'.format(data_type)
        #parte per compressione
    else:
        if bin_flag:
            filename+='.dat'
        else:
            filename+='_{}.{}'.format(data_format,compression)
    
    return filename





#### MAIN ####

try:
    interval=sys.argv[1] 
except:
    print('specify an interval, the correct syntax is:\nscarica_dati.py <interval> <format>')
    sys.exit()
try:
    data_format=sys.argv[2]
except:
    print('specify the data format, the correct syntax is:\nscarica_dati.py <interval> <format>')
    sys.exit()


'''
#leggo intervallo di registrazione (da utende mi aspetto day o hour)
interval = ''
data_format=''
try:
    opts, args = getopt.getopt(sys.argv[1:], "hi:n", "hf:n" ["help", "interval=", "format="])
except getopt.GetoptError:
    print('scarica_dati.py -i <interval> -f <format>')
    sys.exit(2)
for opt, arg in opts:
    if opt == '-h':
        print('noaa2postgresql_ftp.py -i <interval (day or hour)> -f <format (binary or rinex)')
        sys.exit()
    elif opt in ("-i", "--interval"):
        interval = arg
        
if interval=='':
    print('ERROR: specify an interval')
    sys.exit()
elif data_format=='':
    print('ERROR: specify the data format')
    sys.exit()
print(interval)
print(data_format)

sys.exit()
    #quit()
'''
Stazioni=['XXMG','CAMA','AIGI','BEAN','SAOR']
#operazione da fare per ogni stazione (thread?)
Data_installazione='20200720000' #(format YYYYDDDHHMM, where DDD= day of the year)
#while True:
    #DEFINISCO IL FILE ATTUALE DA SCARICARE
    
    #logging.basicConfig(filename='./downloaded_raw_data/{0}/{0}_log.txt'.format(Stazioni[1]), level=logging.INFO,
    #        format='%(asctime)s:%(levelname)s:%(message)s')

day_of_year = datetime.utcnow().utctimetuple().tm_yday
year=datetime.utcnow().utctimetuple().tm_year
hour=datetime.utcnow().utctimetuple().tm_hour
hour_start=hour-1
months=datetime.utcnow().utctimetuple().tm_mon
days=datetime.utcnow().utctimetuple().tm_mday
minutes=datetime.utcnow().utctimetuple().tm_min

if interval=='day':
    
    start_time='%04d%03d0000'%(year,day_of_year)
    session_interval=1440
    ftp_interv_folder='dati_giornalieri'
    
elif interval=='hour':
    start_time='%04d%03d%02d00'%(year,day_of_year,hour_start) #i minuti li definisco io a mano tanto saranno sempre 00
    session_interval=60
    ftp_interv_folder='dati_orari'
else:
    print('ERROR: wrong interval')

if data_format=='rinex':
    end_fname='.gz'
    bin_flag=False
elif data_format=='binary':
    end_fname='.dat'
    bin_flag=True
else:
    print('ERROR: wrong data_format')

#LEGGO ULTIMO FILE SCARICATO DA DB
conn = psycopg2.connect(host=ip, dbname=db, user=user, password=pwd, port=port)
#autocommit
conn.set_session(autocommit=True)
cur = conn.cursor()
#prova con file rinex orari (per file dat e rinex giornalieri cambiare tabella)
query= "SELECT rinex_data from meteognss_ztd.log_dw_{}data_{} where staz='{}' order by rinex_data desc limit 1".format(data_format,interval, Stazioni[1])
try:
    cur.execute(query)
except:
    print('errore.... scrivo nel log?')

a=cur.fetchall()

#CONTROLLO ESISTENZA ULTIMO FILE SCARICATO
if len(a)==0: #empty table (first time running script)
    last_dwnl_file=Data_installazione
else: 
    last_dwnl_file=a[0][0]

print('\n'+last_dwnl_file)
print(start_time)
#ELENCO LISTA FILE ATTESI DA SCARICARE
list_tbd=[]
now=datetime.now()
yd=now.strftime("%j")
inizio=datetime.strptime(last_dwnl_file,"%Y%j%H%M")
fine=datetime.strptime(start_time,"%Y%j%H%M")
#print(inizio,fine)
#print(fine-inizio)

#print('prima while',inizio==fine)
if inizio != fine:
    inizio+=timedelta(hours=1)#per non includere nella lista da scaricare l'ultimo file scaricato
    while inizio!=fine:
        #print('dentro while')
        list_tbd.append(inizio.strftime('%Y%j%H%M'))
        inizio+=timedelta(hours=1)
    list_tbd.append(fine.strftime('%Y%j%H%M')) #per includere nella lista file da scaricare l'ultimo

print(list_tbd)


#CONTROLLO SE LISTA DA SCARICARE VUOTA

if len(list_tbd)==0:
    #CERCO DI SCARICARE FILE CHE NON SONO STATI SCARICATI IN PRECEDENZA
    query="SELECT rinex_data FROM meteognss_ztd.log_dw_{}data_{} where cod_dw != 0 order by rinex_data asc;".format(data_format,interval)
    try:
        cur.execute(query)
    except:
        print('errore.... scrivo nel log?')

    arretrati_tbd=cur.fetchall()
            
    #print(arretrati_tbd)
    
    if len(arretrati_tbd)!=0: 
        # CI SONO FILE ARRETRATI
        for i in arretrati_tbd:

            #print(i[0])
            file_tbd=rinex302filename(Stazioni[1],i[0],session_interval,30,'MO',False,bin_flag,'Hatanaka-RINEX302','tar.gz') #intervallo di registrazione va espresso in minuti (60 o 1440), frequenza va espressa in secondi
            url='https://www.gter.it/concerteaux_gnss/rawdata/{}/{}/{}'.format(Stazioni[1],ftp_interv_folder,file_tbd)
            #print(url)
            output_directory ='./downloaded_raw_data/{}/{}/'.format(Stazioni[1],data_format)
            #print(output_directory)
            try:
                wget.download(url, out=output_directory)
                query="UPDATE meteognss_ztd.log_dw_{}data_{} SET cod_dw=0 WHERE rinex_data='{}' and staz='{}';".format(data_format,interval,i[0],Stazioni[1])
                
                try:
                    cur.execute(query)
                except:
                    print('violazione chiave primaria.... scrivo nel log?')

            except Exception as e:
                #print("Could not download for reason ",str(e))
                query="UPDATE meteognss_ztd.log_dw_{}data_{} SET dw_failure_reason='{}' WHERE rinex_data='{}' and staz='{}';".format(data_format,interval, str(e),i[0],Stazioni[1])
                #print(query)
                try:
                    cur.execute(query)
                except:
                    print('violazione chiave primaria.... scrivo nel log?')

else:
    #SCARICO I FILE
    #CONFRONTO LISTA FILE ATTESI CON LISTA FILE SU SERVER
    ftp = ftplib.FTP('ftp.gter.it')
    ftp.login(user_ftp,pwd_ftp)
    ftp.cwd('/www.gter.it/concerteaux_gnss/rawdata/{}/{}'.format(Stazioni[1],ftp_interv_folder))
    data_tot = []
    data_rinex = []

    ftp.dir(data_tot.append)
    for i in data_tot:
        if i.endswith('{}'.format(end_fname)):
            data_rinex.append(i[74:85])
        else:
            continue
    ftp.quit()

    for i in list_tbd:
        
        if i not in data_rinex:
            print(i,'non presente')
            query="INSERT INTO meteognss_ztd.log_dw_%sdata_%s (rinex_data,staz,cod_dw,dw_failure_reason) VALUES ('%s', '%s',%d,'file not sent by the receiver');" %(data_format,interval, i,Stazioni[1],1)
            try:
                cur.execute(query)
            except:
                print('violazione chiave primaria.... scrivo nel log?')
        
        elif i in data_rinex:
            print(i,'presente')
            file_tbd=rinex302filename(Stazioni[1],i,session_interval,30,'MO',False,bin_flag,'Hatanaka-RINEX302','tar.gz') #intervallo di registrazione va espresso in minuti (60 o 1440), frequenza va espressa in secondi
            #print(file_tbd)
            
            url='https://www.gter.it/concerteaux_gnss/rawdata/{}/{}/{}'.format(Stazioni[1],ftp_interv_folder,file_tbd)
            output_directory ='./downloaded_raw_data/{}/{}/'.format(Stazioni[1],data_format)
            try:
                wget.download(url, out=output_directory)
                query="INSERT INTO meteognss_ztd.log_dw_%sdata_%s (rinex_data,staz,cod_dw) VALUES ('%s', '%s',%d);" %(data_format,interval, i,Stazioni[1],0)
                try:
                    cur.execute(query)
                except:
                    print('violazione chiave primaria.... scrivo nel log?')

            except Exception as e:
                #print("Could not download for reason ",str(e))
                query="INSERT INTO meteognss_ztd.log_dw_%sdata_%s (rinex_data,staz,cod_dw,dw_failure_reason) VALUES ('%s', '%s',%d,'%s');" %(data_format,interval, i,Stazioni[1],1,str(e))
                '''
                da decommentare quando lo script sarà su gishosting?
                try:
                    cur.execute(query)
                except:
                    print('violazione chiave primaria.... scrivo nel log?')
                '''    
cur.close()
conn.close()
    #time.sleep(600) #10 minuti


    
    

    


