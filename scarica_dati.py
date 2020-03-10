#!/usr/bin/env python3
# Copyleft Gter srl 2020
# Lorenzo Benvenuto

from datetime import datetime


#passi da fare

#1. controllare che il dato ci sia e nel caso scaricarlo. Se il dato non ci fosse scrivere su un file di log

#1.1 per scaricare il dato bisogna comporre in automatico il suo nome (quindi nomenclatura RINEX 3.02)


def rinex302filename(st_code):
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

    return filename



Stazioni=['XXMG','CAMA','AIGI','BEAN','SAOR']


obs=rinex302filename(Stazioni[0])
print(obs)
# devo vedere utlimo file processato (faccio un file con ultima ora processata, oppure 5 file di log di cui leggo ultima riga)
# fare differenza rispetto a ora, e capire quanti e quali file cercare (potrebbe essere sia che il mio script non abbia girato sia che il ricevitore non abbia mandato il dato)
# cerco i file
day_of_year = datetime.utcnow().utctimetuple().tm_yday
year=datetime.utcnow().utctimetuple().tm_year
hour=datetime.utcnow().utctimetuple().tm_hour
#minutes=datetime.utcnow().utctimetuple().tm_min
minutes=0 #li definisco io a mano tanto saranno sempre 00
print(year,'%03d'%day_of_year,hour,'%02d'%minutes)