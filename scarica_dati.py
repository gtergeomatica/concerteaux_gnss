#!/usr/bin/env python3
# Copyleft Gter srl 2020
# Lorenzo Benvenuto



#passi da fare

#1. controllare che il dato ci sia e nel caso scaricarlo. Se il dato non ci fosse scrivere su un file di log

#1.1 per scaricare il dato bisogna comporre in automatico il suo nome (quindi nomenclatura RINEX 3.02)


def rinex302filename(st_code):
    '''function to dynamically define the filename in rinex 3.02 format)

    Needed parameters

    1: STATION/PROJECT NAME (SPN): format XXXXMRCCC
        XXXX: station code
        M: monument or marker number (0-9)
        R: receiver number (0-9)
        CCC:  ISO Country CODE see ISO 3166-1 alpha-3 
    
    2: 


'''


    filename=''

    M=0 #da capire
    R=0 #da capire

    if st_code=='SAOR':
        CCC='FRA'
    else:
        CCC='ITA'
    
    
    SPN='{}{}{}{}'.format(st_code,M,R,CCC)


    filename+=SPN

    return filename



Stazioni=['XXMG','CAMA','AIGI','BEAN','SAOR']


obs=rinex302filename(Stazioni[0])
print(obs)
