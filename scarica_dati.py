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
    print(freq)
    print((freq.seconds//60)%60,freq.seconds)
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


# definisco il nome del file da scaricare


print(ip,db,user,password,port)
sys.exit()
while True:
    Stazioni=['XXMG','CAMA','AIGI','BEAN','SAOR']

    #operazione da fare per ogni stazione

    logging.basicConfig(filename='./downloaded_raw_data/{0}/{0}_log.txt'.format(Stazioni[1]), level=logging.INFO,
            format='%(asctime)s:%(levelname)s:%(message)s')

    day_of_year = datetime.utcnow().utctimetuple().tm_yday
    year=datetime.utcnow().utctimetuple().tm_year
    hour=datetime.utcnow().utctimetuple().tm_hour
    months=datetime.utcnow().utctimetuple().tm_mon
    days=datetime.utcnow().utctimetuple().tm_mday
    minutes=datetime.utcnow().utctimetuple().tm_min



    start_time='%04d%03d%02d00'%(year,day_of_year,hour-1) #i minuti li definisco io a mano tanto saranno sempre 00
    #print(start_time)

    #devo fare un flag per il datatype:

    # mettere il secondo boolean True per scaricare file binario o False per scaricare file RINEX
    obs=rinex302filename(Stazioni[1],start_time,60,30,'MO',False,False,'Hatanaka-RINEX302','tar.gz') #intervallo di registrazione va espresso in minuti (60 o 1440), frequenza va espressa in secondi
    print(obs)

    # scarico il file

    # devo vedere utlimo file processato (faccio un file con ultima ora processata, oppure 5 file di log di cui leggo ultima riga)
    # fare differenza rispetto a ora, e capire quanti e quali file cercare (potrebbe essere sia che il mio script non abbia girato sia che il ricevitore non abbia mandato il dato)
    # cerco i file

    with open ('./downloaded_raw_data/{0}/{0}_log.txt'.format(Stazioni[1]),'r') as logfile:
        righe=logfile.readlines()
    print(righe[-1])

    try:
        bs_test='CAMA00ITA_R_20200711300_01H_31S.dat'
        
        url='https://www.gter.it/concerteaux_gnss/rawdata/{}/dati_orari/{}'.format(Stazioni[1],obs)
        output_directory ='./downloaded_raw_data/CAMA/'
        filename = wget.download(url, out=output_directory)
        logging.info('Downloaded file {}:0'.format(obs)) #0 ok

    except:
        logging.warning('File {} NOT Downloaded:1'.format(obs)) #1 errore

    time.sleep(3600)


#to do :
# - implementare lettura e scrittura tabella 
# - leggere ultimo file scaricato e scaricare tutti i file da li in giu'
# - capire come mai ogni tanto non mandi i file binari (lanciare script sul 99 per un po di tempo)
# - capire come fare a leggere tutti i file di una pagina web (e/o fare un ls del server ftp)