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


def rinex302filename(st_code,ST,session_interval,obs_freq,data_type,data_type_flag,bin_flag,data_format='RINEX',compression=None):
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
        if None the filename will ends with .YYO
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
            if compression != None:
                filename+='_{}.{}'.format(data_format,compression)      
            else:
                filename+='.{}O'.format(ST[2:4])
            
    
    return filename


def dat2rinex(path,binfile,rinex_version):
    '''
    Function to convert binary file into Rinex file
    the function deletes all navigations file an only keeps the obs

    The function returns 0 if the conversion succeed; if the conversion 
    doesn't succeed the function returns a string with the error raised
    '''
    os.system('/opt/NovAtel\ Inc/NovAtel\ Convert/NovAtelConvert -r{} {}{}'.format(rinex_version,path,binfile))
        
    nav_files=[i for i in os.listdir(path) if not i.endswith('O') and not i.endswith('dat') and not i.endswith('gz') and not i.endswith('zip') and not i.endswith('rnx')]
    obs_file=[i[:-3] for i in os.listdir(path) if i.endswith('O')]
    
    if binfile[:-3] in obs_file:
        conv_status=0
    else:
        conv_status=1
    # rimuovo i file navigazionali (non servono per ztd)
    for i in nav_files:
        os.system('rm {}{}'.format(path,i))
    #rimuovo il file binario
    #os.system('rm {}{}'.format(path,binfile))
    return conv_status


def uncompressRinex(dir_path,path,rinexfile,compression,hatanaka=True):
    '''
    function to uncompress an archive containing a rinex file
    the function uncompress the rinex if it's in hatanaka format
    the function also changes the extension of the rinex file:
    rnx --> yyO
    Lastly the function removes the input rinex file
    The function returns the uncompressed rinex file name or None if the conversion 
    was not successful
    '''
    if not os.path.exists('{}temp'.format(path)):
        os.makedirs('{}temp'.format(path))

    if compression=='tar.gz':
        os.system('tar xfz {0}{1} -C {0}temp/'.format(path,rinexfile)) #tar xvfz (x=extract, v=verbole, f=file, z=extract gzip file)
    elif compression=='zip':
        os.system('unzip {}{}'.format(path,rinexfile))

    #os.system('rm {}{}'.format(path,rinexfile))
    
    if hatanaka:
        try:
            htk_rinex=[i for i in os.listdir('{}temp'.format(path)) if not i.endswith('MN.rnx')]
            nav_file=[i for i in os.listdir('{}temp'.format(path)) if i.endswith('MN.rnx')]
            os.system('rm {}temp/{}'.format(path,nav_file[-1]))
            
            
            os.system('{}/CRX2RNX {}temp/{}'.format(dir_path,path,htk_rinex[-1]))
            os.system('rm {}temp/{}'.format(path,htk_rinex[-1]))

            new_rinex=os.listdir('{}temp/'.format(path))[0]
            
            newrnxname='{}.{}O'.format(new_rinex[:-7],new_rinex[14:16])
            
            os.system('mv {0}temp/{1} {0}{2}'.format(path,new_rinex,newrnxname))
            
            os.system('rm {}{}'.format(path,rinexfile))
            os.system('rmdir {}temp'.format(path))
            return newrnxname

        except Exception as e:
            print('can\'t pass from .rnx to .yyO for reason ',e)
            return
    else:
        try:
            new_rinex=[i for i in os.listdir('{}temp'.format(path)) if not i.endswith('MN.rnx')]
            nav_file=[i for i in os.listdir('{}temp'.format(path)) if i.endswith('MN.rnx')]
            os.system('rm {}temp/{}'.format(path,nav_file[-1]))

            new_rinex=new_rinex[-1]
            
            newrnxname='{}.{}O'.format(new_rinex[:-7],new_rinex[14:16])
            
            os.system('mv {0}temp/{1} {0}{2}'.format(path,new_rinex,newrnxname))
            
            os.system('rm {}{}'.format(path,rinexfile))
            os.system('rmdir {}temp'.format(path))
            return newrnxname
        except Exception as e:
            print('can\'t pass from .rnx to .yyO for reason ',e)
            return 



#### MAIN ####
def main():


    print('\nStart script ',datetime.now())
    try:
        interval=sys.argv[1]
    except:
        print('specify an interval, the correct syntax is:\nscarica_dati.py <interval>')
        sys.exit()

    #input sulla registrazione del dato
    data_format='rinex'
    rinex_format='Hatanaka-RINEX302'
    obs_freq=30
    compression_format='tar.gz'
    
    
    
    Data_installazione='20201220000' #(format YYYYDDDHHMM, where DDD= day of the year)
    
    

    dir_path = os.path.dirname(os.path.realpath(__file__))
    #output_directory ='{2}/downloaded_raw_data/{0}/{1}/'.format(stz,data_format,dir_path)
    #print('la directory di output è',output_directory)


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


    #CONNESSIONE AL DB
    conn = psycopg2.connect(host=ip, dbname=db, user=user, password=pwd, port=port)
    #autocommit
    conn.set_session(autocommit=True)
    cur = conn.cursor()
    #prova con file rinex orari (per file dat e rinex giornalieri cambiare tabella)

    #Stazioni=['XXMG','BEAN','AIGI','CAMA','SAOR']
    

    query= "SELECT cod FROM concerteaux.stazioni_lowcost WHERE operativa=True"
    
    try:
        cur.execute(query)
    except Exception as e:
        print('errore: ',e)

    Stazioni=[i[0] for i in cur.fetchall()] 
    print('\nAcive Stations:',end=' ')
    for s in Stazioni:        
        print(s, end=' ')

    print('\n\n********** Start Loop on Stations **********\n')
    


    for stz in Stazioni: #CICLO FOR su TUTTE LE STAZIONI PRESENTI

        print('Station: ',stz)
        #LEGGO ULTIMO FILE SCARICATO DA DB
        query= "SELECT rinex_data from meteognss_ztd.log_dw_{}data_{} where staz='{}' order by rinex_data desc limit 1".format(data_format,interval, stz)
        
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

        print('last_dwnl_file = '+last_dwnl_file)
        print('start_time = ',start_time)
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

        print('file to be downloaded ',list_tbd)


        #CONTROLLO SE LISTA DA SCARICARE VUOTA

        if len(list_tbd)==0:
            #CERCO DI SCARICARE FILE CHE NON SONO STATI SCARICATI IN PRECEDENZA
            # seleziono solo gli ultimi 24 file non scaricati (da decidere se 24 è un numero che può andare)
            query="SELECT rinex_data FROM meteognss_ztd.log_dw_{}data_{} where staz='{}' and cod_dw != 0 order by rinex_data desc limit 24;".format(data_format,interval,stz)
            try:
                cur.execute(query)
            except:
                print('errore.... scrivo nel log?')

            arretrati_tbd=cur.fetchall()
            print('old file to be downloaded ',arretrati_tbd)

            if len(arretrati_tbd)!=0:
                # CI SONO FILE ARRETRATI
                for i in arretrati_tbd:

                    #controllo se il file e' stato inviato dal ricevitore
                    
                    file_tbd=rinex302filename(stz,i[0],session_interval,obs_freq,'MO',False,False,rinex_format,compression_format) #intervallo di registrazione va espresso in minuti (60 o 1440), frequenza va espressa in secondi
                
                    if os.path.exists('{}/{}/{}/{}'.format(folder_ftp,stz,ftp_interv_folder,file_tbd)):
                        print(i[0],'RINEX file present')
                        query="UPDATE meteognss_ztd.log_dw_{}data_{} SET cod_dw=0, dw_failure_reason='file downloaded in a second time' WHERE rinex_data='{}' and staz='{}';".format(data_format,interval,i[0],stz)
                        
                        try:
                            cur.execute(query)
                        except:
                            print('violazione chiave primaria.... scrivo nel log?')
                        #estraggo converto da Hatanaka a YYO
                        file_rnx=uncompressRinex(dir_path,'{}/{}/{}/'.format(folder_ftp,stz,ftp_interv_folder),file_tbd,compression_format,True)
                        
                        #sposto il file nella cartella di goGPS
                        try:
                            os.system('mv {}/{}/{}/{} {}/{}'.format(folder_ftp,stz,ftp_interv_folder,file_rnx, dir_path, rnx_goGPS))
                        except Exception as e:
                            print('can not move the file to the goGPS folder for reason ',e)

                    else:
                        #non riesco trovo il rinex, quindi provo con il binario
                        file_tbd=rinex302filename(stz,i[0],session_interval,obs_freq,'MO',False,True)
                        
                        if os.path.exists('{}/{}/{}/{}'.format(folder_ftp,stz,ftp_interv_folder,file_tbd)): #se il file esiste
                            print(i[0],'RINEX file not present, but .dat file present')
                            conversione=dat2rinex('{}/{}/{}/'.format(folder_ftp,stz,ftp_interv_folder),file_tbd,3.04)
                            if conversione==0:
                                #file convertito correttamente, aggiorno il db

                                query="UPDATE meteognss_ztd.log_dw_{}data_{} SET cod_dw=0, dw_failure_reason='file downloaded in a second time' WHERE rinex_data='{}' and staz='{}';".format(data_format,interval,i[0],stz)
                                try:
                                    cur.execute(query)
                                except:
                                    print('violazione chiave primaria.... scrivo nel log?')
                                #sposto il file nella cartella di goGPS
                                
                                file_rnx=rinex302filename(stz,i,session_interval,obs_freq,'MO',False,False)
                                try:
                                    os.system('mv {}/{}/{}/{} {}/{}'.format(folder_ftp,stz,ftp_interv_folder,file_rnx, dir_path, rnx_goGPS))
                                except Exception as e:
                                    print('can not move the file to the goGPS folder for reason ',e)
                            else:
                                query="INSERT INTO meteognss_ztd.log_dw_%sdata_%s (rinex_data,staz,cod_dw,dw_failure_reason) VALUES ('%s', '%s',%d,'file downloaded but not converted into RINEX');" %(data_format,interval, i,stz,1)
                                try:
                                    cur.execute(query)
                                except:
                                    print('violazione chiave primaria.... scrivo nel log?')
                        else:
                            #non riesco a trovare nemmeno il binario per la ragione e
                            print(i[0],'RINEX file not present and also .dat file not present')
                            query="UPDATE meteognss_ztd.log_dw_{}data_{} SET dw_failure_reason='{}' WHERE rinex_data='{}' and staz='{}';".format(data_format,interval,'non trovato neanche il file .dat',i[0],stz)
                            #print(query)
                            try:
                                cur.execute(query)
                            except:
                                print('violazione chiave primaria.... scrivo nel log?')

        else:
            #SCARICO I FILE
            #CONFRONTO LISTA FILE ATTESI CON LISTA FILE SU SERVER
            
            #if os.path.exists('{}/{}/{}'.format(folder_ftp,stz,ftp_interv_folder))): check su esistenza cartella

            data_tot = os.listdir('{}/{}/{}'.format(folder_ftp,stz,ftp_interv_folder))
            data_rinex = []

            for i in data_tot:

                if i.endswith(compression_format): #i rinex sono compressi in tar.gz
                    data_rinex.append(i[12:23])
                    #print(i[74:85])
                else:
                    continue

            for i in list_tbd:

                if i not in data_rinex:
                    
                    #non c'è il rinex, ma potrebbe esserci il binario
                    #provo a scaricare il file binario
                    file_tbd=rinex302filename(stz,i,session_interval,obs_freq,'MO',False,True)
                                    
                    if os.path.exists('{}/{}/{}/{}'.format(folder_ftp,stz,ftp_interv_folder,file_tbd)): #se il file esiste
                        print(i,'RINEX file not present, but .dat file present')
                        conversione=dat2rinex('{}/{}/{}/'.format(folder_ftp,stz,ftp_interv_folder),file_tbd,3.04)
                        if conversione==0:
                            #file convertito correttamente, aggiorno il db

                            query="INSERT INTO meteognss_ztd.log_dw_%sdata_%s (rinex_data,staz,cod_dw) VALUES ('%s', '%s',%d);" %(data_format,interval, i,stz,0)
                            try:
                                cur.execute(query)
                            except:
                                print('violazione chiave primaria.... scrivo nel log?')
                            
                            #sposto il file nella cartella di goGPS
                            
                            file_rnx=rinex302filename(stz,i,session_interval,obs_freq,'MO',False,False)
                            try:
                                os.system('mv {}/{}/{}/{} {}/{}'.format(folder_ftp,stz,ftp_interv_folder,file_rnx, dir_path, rnx_goGPS))
                            except Exception as e:
                                print('can not move the file to the goGPS folder for reason ',e)
                    
                        else:
                            query="INSERT INTO meteognss_ztd.log_dw_%sdata_%s (rinex_data,staz,cod_dw,dw_failure_reason) VALUES ('%s', '%s',%d,'file downloaded but not converted into RINEX');" %(data_format,interval, i,stz,1)
                            try:
                                cur.execute(query)
                            except:
                                print('violazione chiave primaria.... scrivo nel log?')
                    
                    else: #se il file non esiste
                        print(i,'RINEX file not present and also .dat file not present')
                        
                        query="INSERT INTO meteognss_ztd.log_dw_%sdata_%s (rinex_data,staz,cod_dw,dw_failure_reason) VALUES ('%s', '%s',%d,'%s');" %(data_format,interval, i,stz,1,'non trovato neanche il file .dat')
                        
                        try:
                            cur.execute(query)
                        except:
                            print('violazione chiave primaria.... scrivo nel log?')
                            
                elif i in data_rinex:
                    print(i,'RINEX file present')
                    file_tbd=rinex302filename(stz,i,session_interval,obs_freq,'MO',False,False,rinex_format,compression_format) #intervallo di registrazione va espresso in minuti (60 o 1440), frequenza va espressa in secondi
                    
                    #url='https://www.gter.it/concerteaux_gnss/rawdata/{}/{}/{}'.format(stz,ftp_interv_folder,file_tbd)
                    
                    if os.path.exists('{}/{}/{}/{}'.format(folder_ftp,stz,ftp_interv_folder,file_tbd)): #se il file esiste
                    
                        query="INSERT INTO meteognss_ztd.log_dw_%sdata_%s (rinex_data,staz,cod_dw) VALUES ('%s', '%s',%d);" %(data_format,interval, i,stz,0)
                        
                        try:
                            cur.execute(query)
                        except:
                            print('violazione chiave primaria.... scrivo nel log?')
                        #estraggo converto da Hatanaka a YYO
                        file_rnx=uncompressRinex(dir_path,'{}/{}/{}/'.format(folder_ftp,stz,ftp_interv_folder),file_tbd,compression_format,True)
                        
                        #sposto il file nella cartella di goGPS
                        try:
                            os.system('mv {}/{}/{}/{} {}/{}'.format(folder_ftp,stz,ftp_interv_folder,file_rnx, dir_path, rnx_goGPS))
                        except Exception as e:
                            print('can not move the file to the goGPS folder for reason ',e)
                    else: #se non esiste o non riesco ad accedere alla cartella
                        query="INSERT INTO meteognss_ztd.log_dw_%sdata_%s (rinex_data,staz,cod_dw) VALUES ('%s', '%s',%d);" %(data_format,interval, i,stz,1)
                    
                        try:
                            cur.execute(query)
                        except:
                            print('violazione chiave primaria.... scrivo nel log?')


                 
    cur.close()
    conn.close()
        #time.sleep(600) #10 minuti
    print('\n********** Downloaded all Data ^_^ **********\n')

    print('End Script ',datetime.now())

if __name__ == "__main__":
    main()
    
    

    


