#!/usr/bin/env python3
# Copyleft Gter srl 2020
# Lorenzo Benvenuto

import wget
import sys,os
import datetime
import psycopg2
from credenziali import *
from datetime import datetime, timedelta
import scarica_dati

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
    

def modifyINIfile(goGPSproject,doy,anno):
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
    with open('{}/config/elab_partenza.ini'.format(goGPSproject), 'r') as configfile:
        oldfilelines=configfile.readlines()

    print(oldfilelines[49][0:14])
 
    for i in oldfilelines:
    
        #modify session date
        #start
        if i[0:14]=='sss_date_start':
            start_time= datetime(anno, 1, 1) + timedelta(doy - 1)
            newstring ='sss_date_start = "{}-{}-{} 00:00:00"\n'.format(start_time.strftime("%Y"),start_time.strftime("%m"),start_time.strftime("%d"))
            newINIfilelines.append(newstring)

        #modify session date
        #stop       
        elif i[0:14]=='sss_date_stop ':
            stop_time= datetime(anno, 1, 1) + timedelta(doy - 1)
            newstring ='sss_date_stop = "{}-{}-{} 23:59:59"\n'.format(start_time.strftime("%Y"),start_time.strftime("%m"),start_time.strftime("%d"))
            newINIfilelines.append(newstring)        
        
        else:
            newINIfilelines.append(i)
    
    project_conf='{}/config/'.format(goGPSproject) #use absolute path
    newINIfile='elab_{}_{}.ini'.format(anno,doy)
    
    with open ('{}{}'.format(project_conf,newINIfile),'a') as newfile:
        for line in newINIfilelines:
            newfile.write(line)
    
    return('{}{}'.format(project_conf,newINIfile))
   
        
def launchgoGPS(goGPSpath,inifile):
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
        setfile.write('cd {}\n'.format(goGPSpath))
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


def uploadZTDtoDB(goGPSproject,connection_param,table,year,doy):
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
    out_path='{}/out/{}/{}/'.format(goGPSproject,year,doy)
    try:
        ztd_file=[f for f in os.listdir(out_path)]
    except:
        # in questo caso non e' stata processata nessuna stazione
        conn = psycopg2.connect(host=connection_param[0], dbname=connection_param[1], user=connection_param[2], password=connection_param[3], port=connection_param[4])
        #autocommit
        conn.set_session(autocommit=True)
        cur = conn.cursor()
        query1='SELECT cod FROM concerteaux.stazioni_lowcost;'
        cur.execute(query1)
        stzn=[i[0] for i in cur.fetchall()]
        queries=['INSERT INTO meteognss_ztd.log_ztd_elaborations(id_station, year, doy, processed) VALUES (\'{}\', {}, {}, False)'.format(i,year,doy) for i in stzn]
        for q in queries:
            cur.execute(q)
        return
    
    
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
            query1="INSERT INTO meteognss_ztd.{} VALUES ('{}','{}',{}) ;".format(table,s,z[0],z[1])
            try:
                cur.execute(query1)
                
            except Exception as e:
                print('error: ',e)
        query2='INSERT INTO meteognss_ztd.log_ztd_elaborations(id_station, year, doy, processed) VALUES (\'{}\', {}, {}, True)'.format(s,year,doy)
        try:
            cur.execute(query2)
        except Exception as e:
            print('error: ',e)
    cur.close()
    conn.close()

    #rimuovo cartella con risultati elaborazione?
    os.system('rm -r {}'.format(out_path))





goGPSpath='/home/gter/REPOSITORY/goGPS_MATLAB/goGPS'
goGPSproject='{}/ZTD_elaborations'.format(os.path.dirname(os.path.realpath(__file__)))

conn = psycopg2.connect(host=ip, dbname=db, user=user, password=pwd, port=port)
conn.set_session(autocommit=True)
cur = conn.cursor()
query= "SELECT cod FROM concerteaux.stazioni_lowcost WHERE operativa=True"
try:
    cur.execute(query)
except Exception as e:
    print('errore: ',e)
Stazioni=[i[0] for i in cur.fetchall()] 

cur.close()
conn.close()

print(Stazioni)

#print(Stazioni)
#anno=2020

#giorno=180


connection=[ip,db,user,pwd,port]  

now=datetime.now()
dir_path = os.path.dirname(os.path.realpath(__file__))

day_of_year = datetime.utcnow().utctimetuple().tm_yday
year=datetime.utcnow().utctimetuple().tm_year
hour=datetime.utcnow().utctimetuple().tm_hour
hour_start=hour-1
months=datetime.utcnow().utctimetuple().tm_mon
days=datetime.utcnow().utctimetuple().tm_mday
minutes=datetime.utcnow().utctimetuple().tm_min
#print(now)
#print(year)
#print(day_of_year)
starting_time=str(year)+str(day_of_year)+'0000'
#print(starting_time)
day_to_process=day_of_year-4
#rinex_to_process=[scarica_dati.rinex302filename(s,starting_time,1440,30,'MO',False,False) for s in Stazioni]

print('Processing dei dati del giorno: {}, anno: {}'.format(day_to_process,year))

#rinex_in=[getObsIGS(i,anno,giorno) for i in stazioni]


new_ini_file=modifyINIfile(goGPSproject,day_to_process,year)

launchgoGPS(goGPSpath,new_ini_file)

uploadZTDtoDB(goGPSproject, connection,'ztd_bendola',year,day_to_process)