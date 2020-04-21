#!/usr/bin/env python3
# Copyleft Gter srl 2020
# Lorenzo Benvenuto

import wget
import sys,os
import datetime
import psycopg2
from credenziali import *
from datetime import datetime

def getObsIGS(station,year,doy):
    '''
    Function to get RINEX file from IGS

    this function it's needed to simulated data coming from our GNSS network
    When our GNSS network will be operative, this function will be substitue with the 
    script scarica_dati.py
    
    '''
    url='ftp://cddis.nasa.gov/gnss/data/daily/{}/{}/{}o/'.format(year,doy,str(year)[2:])
    print(url)
    filename='{}{}0.{}o.Z'.format(station,doy,str(year)[2:])
    print(filename)
    try:
        obsfile=wget.download('{}{}'.format(url,filename),out='./test_automatiz/RINEX')
        print(type(obsfile))
    except Exception as e:
        print(e)
    
    os.system('uncompress {}'.format(obsfile))
    return obsfile[:-2]
    

def modifyINIfile(inifile,doy,anno):
    '''
    function to modify the configuration file for elaboration with goGPS

    This function reads a sample .ini file and generates a new modified .ini files
    The modified parameters are:
        - doy (day of year)
        - year 
        (those two parameters needs to be checked/modified every elaboratoin) 
        - ..

    The function stores the new .ini files in the goGPS project folder 
    The function returns a string with the path and the name of the new ini file
    
     '''
    oldfilelines=[]
    newINIfilelines=[]
    with open(inifile, 'r') as configfile:
        oldfilelines=configfile.readlines()

    print(oldfilelines[49][0:14])
 
    for i in oldfilelines:
    
        #modify session date
        #start
        if i[0:14]=='sss_date_start':
            start_time= datetime.datetime(anno, 1, 1) + datetime.timedelta(doy - 1)
            newstring ='sss_date_start = "{}-{}-{} 00:00:00"\n'.format(start_time.strftime("%Y"),start_time.strftime("%m"),start_time.strftime("%d"))
            newINIfilelines.append(newstring)

        #modify session date
        #stop       
        elif i[0:14]=='sss_date_stop ':
            stop_time= datetime.datetime(anno, 1, 1) + datetime.timedelta(doy - 1)
            newstring ='sss_date_stop = "{}-{}-{} 23:59:59"\n'.format(start_time.strftime("%Y"),start_time.strftime("%m"),start_time.strftime("%d"))
            newINIfilelines.append(newstring)        
        
        else:
            newINIfilelines.append(i)
    
    prject_path='/home/lorenzo/concerteaux_gnss/test_automatiz/config/' #use absolute path
    newINIfile='elab_{}_{}.ini'.format(anno,doy)
    
    with open ('{}{}'.format(prject_path,newINIfile),'a') as newfile:
        for line in newINIfilelines:
            newfile.write(line)
    
    return('{}{}'.format(prject_path,newINIfile))
   
        
def launchgoGPS(inifile):
    '''
    function to excecute goGPS with a ini file and with no GUI
    The function writes a simple matlab script containg some istructions 
    that will be executed in the matlab shell
    This scripts:
        - goes to the goGPS installation folder
        - starts goGPS with the specified ini file 
        - closes matlab
    After the exection of goGPS the function eleminates the matlab scritp
    '''
    
    with open ('launch_goGPStemp.m','a') as setfile:
        setfile.write('clear all\nclose all\n')
        setfile.write('cd \'/home/lorenzo/source_codes/goGPS_MATLAB/goGPS\'\n')
        setfile.write('goGPS(\'{}\',0);\n'.format(inifile))
        setfile.write('exit')       
        
    os.system('matlab -r launch_goGPStemp')
    os.system('rm launch_goGPStemp.m')
        
        
def readData(path,filename):
    '''
    Function to read csv file with ZTD data
    The function needs the path and the name of the file as inputs
    It returns a list of tuples which looks like: [(data1,ZTD1), (data2,ZTD2).....(datan,ZTDn)]
    data is a datetime object, ZTD is a float
    '''

    dati=[]
    try:
        with open ('{}{}'.format(path,filename),'r') as dfile:
            for i in dfile.readlines()[1:]:
                #print(i.split(',')[0],i.split(',')[1])
                year=i.split(',')[0][6:10]
                month=i.split(',')[0][3:5]
                day=i.split(',')[0][0:2]
                hour=i.split(',')[0][11:13]
                minute=i.split(',')[0][14:16]
                second=i.split(',')[0][17:19]
                dati.append((datetime(int(year),int(month),int(day),int(hour),int(minute),int(second)),float(i.split(',')[1])))
        return dati
       
    except Exception as e:
        print('Error: {}'.format(e))
        return


def uploadZTDtoDB(connection_param,table,year,doy):
    '''
    funtion to upload to DB ZTD data coming from goGPS elaboration
    the function needs:
        - a list with the connection parameters
        - the name of table where upload the data
        - the year
        - the day of year
    (this uploading function regards daily ZTD elaborations) 
   
    '''
    print(connection_param)
    print(table)
    out_path='./test_automatiz/out/{}/{}/'.format(year,doy)
    ztd_file=[f for f in os.listdir(out_path)]
    ztd_station_name=[n[0:4] for n in ztd_file]
    ztd_data=[readData(out_path,z) for z in ztd_file]
    
    #connection to the DB
    conn = psycopg2.connect(host=connection_param[0], dbname=connection_param[1], user=connection_param[2], password=connection_param[3], port=connection_param[4])
    #autocommit
    conn.set_session(autocommit=True)
    cur = conn.cursor()
    for s,d in zip(ztd_station_name,range(len(ztd_data))):
        for z in ztd_data[d]:
            #print(s,z[0],z[1])
            query="INSERT INTO meteognss_ztd.{} VALUES ('{}','{}',{}) ;".format(table,s,z[0],z[1])
            try:
                cur.execute(query)
            except Exception as e:
                print('error: ',e)
    cur.close()
    conn.close()





anno=2019

giorno=319

stazioni=['gras','geno','ieng']

connection=[ip,db,user,pwd,port]  

#rinex_in=[getObsIGS(i,anno,giorno) for i in stazioni]

inifile_path='./test_automatiz/config/elab_partenza.ini'

#new_ini_file=modifyINIfile(inifile_path,giorno,anno)

#launchgoGPS(new_ini_file)
      
uploadZTDtoDB(connection,'dati_test_temp',anno,giorno)