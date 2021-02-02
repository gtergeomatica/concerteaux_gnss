#!/usr/bin/env python3
# Copyleft Gter srl 2020
# Lorenzo Benvenuto

import wget
import sys,os
import datetime
import psycopg2
from credenziali import *
from datetime import datetime, timedelta, date
import scarica_dati
#from ftplib import FTP  
from ftplib import FTP_TLS


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
    with open('{}/config/elab_partenza_new.ini'.format(goGPSproject), 'r') as configfile:
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
        setfile.write('cd {}/goGPS\n'.format(goGPSpath))
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
    #print(connection_param)
    #print(table)
    out_path='{}/out/{:04d}/{:03d}/'.format(goGPSproject,year,doy)

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
    print ('ZTD values for stations ',ztd_station_name,' ,for year {:04d} day {:03d}, correctly uploaded to database!'.format(year,doy) )
    #rimuovo cartella con risultati elaborazione?
    os.system('rm -r {}'.format(out_path))
    print('ZTD output files (.csv) removed locally!')
    


def __date2weeksday(date, start):
    """Convert date to spent weeks and day of week from a start date."""
    delta = date - start
    if delta.days >= 0:
        weeks = delta.days // 7
        dayofweek = delta.days % 7
        return weeks, dayofweek
    else:
        raise ValueError('Invalid date: %s, too early.' %date)




def date2gpswd(date):
    """Convert date to GPS week and day of week, return int tuple (week, day).
    Example:
    >>> from datetime import date
    >>> date2gpswd(date(2017, 5, 17))
    (1949, 3)
    >>> date2gpswd(date(1917, 5, 17))
    Traceback (most recent call last):
    ...
    ValueError: Invalid date: 1917-05-17, too early.
    """
    return __date2weeksday(date, GPS_START_DATE)

def doy2weeksday(doy, year):
    '''function to convert year and doy into gpsweek and dow'''
    
    datamy=datetime(year, 1, 1) + timedelta(doy - 1)

    #print(type(date(year,datamy.month,datamy.day)))
    pippo=date2gpswd(date(year,datamy.month,datamy.day))
    print(pippo)


def rmEphemerides (goGPSpath, doy,year):
    
    #cerco la settimana e il giorno GPS
    datamy=datetime(year, 1, 1) + timedelta(doy - 1)
    gpsweek=date2gpswd(date(year,datamy.month,datamy.day))[0]
    
    eph_path=goGPSpath+'/data/satellite/EPH/'+str(gpsweek)
    
    if os.path.exists(eph_path):
        os.system('rm -r {}'.format(eph_path))
    else:
        print(eph_path+' does not exist')

def rmCLK (goGPSpath, doy,year):
    
    #cerco la settimana e il giorno GPS
    datamy=datetime(year, 1, 1) + timedelta(doy - 1)
    gpsweek=date2gpswd(date(year,datamy.month,datamy.day))[0]
    
    clk_path=goGPSpath+'/data/satellite/CLK/'+str(gpsweek)
    
    if os.path.exists(clk_path):
        os.system('rm -r {}'.format(clk_path))
    else:
        print(clk_path+' does not exist')    


def upload2ftpserver(local_folder, remote_folder, file):

    
    #print(pwd_ftp)
    ftp = FTP_TLS(url_ftp)  
    ftp.login(user_ftp, pwd_ftp) 



    with open('{}/{}'.format(local_folder,file), 'rb') as f:  
        ftp.storbinary('STOR %s' % '{}/{}'.format(remote_folder,file), f)  
    ftp.quit()




#### MAIN ####

goGPSpath='/home/gter/REPOSITORY/goGPS_MATLAB_git'
goGPSproject='{}/ZTD_elaborations'.format(os.path.dirname(os.path.realpath(__file__)))


# Carico i dati rinex (da spostare dopo)
rinex_folder=os.listdir('{}/RINEX'.format(goGPSproject))
#giorno=datetime.now()
#anno='{}'.format(giorno.year)
year=datetime.utcnow().utctimetuple().tm_year
day_of_year = datetime.utcnow().utctimetuple().tm_yday
ystd=day_of_year-1
start_time='{:04d}{:03d}'.format(year,ystd) #questo perche il rinex completo lo si ha solo del giorno precedente
#print(start_time)
#sys.exit()
for i in rinex_folder:
    #out_subfolder=i[0:4]
    if i[12:19] == start_time:
        print(i)
        upload2ftpserver('{}/RINEX'.format(goGPSproject),'/www.gter.it/concerteaux_gnss/rawdata/{}'.format(i[0:4]),i)
#sys.exit()



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

connection=[ip,db,user,pwd,port]  

now=datetime.now()
dir_path = os.path.dirname(os.path.realpath(__file__))
GPS_START_DATE = date(1980, 1, 6)
day_of_year = datetime.utcnow().utctimetuple().tm_yday
year=datetime.utcnow().utctimetuple().tm_year


starting_time=str(year)+str(day_of_year)+'0000'

day_to_process=day_of_year-5
#day_to_process=274


#uploadZTDtoDB(goGPSproject, connection,'ztd_bendola',year,day_to_process)

print('Processing dei dati del giorno: {}, anno: {}'.format(day_to_process,year))




new_ini_file=modifyINIfile(goGPSproject,day_to_process,year)

launchgoGPS(goGPSpath,new_ini_file)
#controllo stato della soluzione

#print(os.path.exists(goGPSproject+'/out/{:04d}/{:03d}'.format(year,day_to_process)))

if os.path.exists(goGPSproject+'/out/{:04d}/{:03d}'.format(year,day_to_process)):
    #qualcosa è stato prodotto come output
    #rimuovo le effemeridi, i file CLK, ERP
    rmEphemerides(goGPSpath, day_to_process,year)
    print('Epeherides correctly removed!')
    rmCLK(goGPSpath, day_to_process,year)
    print('Clock file correctly removed!')
    #sposto il log su aruba 
    logs=os.listdir('{}/out/log'.format(goGPSproject))
    logs.sort() # list of logs file sorted from the elder to the newer (based on filenames)
    upload2ftpserver('{}/out/log'.format(goGPSproject),'/www.gter.it/concerteaux_gnss/ztd_elaborations/log',logs[-1])
    os.system('rm {}/out/log/{}'.format(goGPSproject, logs[-1]))
    print('Elaboration log file, called {} removed locally and uploaded to server!'.format(logs[-1]))
    #rimuovo i RINEX

    # Carico i dati elaborati in formato CSV sul aruba
    elab=os.listdir('{}/out/{:04d}/{:03d}'.format(goGPSproject, year,day_to_process))
    for i in elab:
        upload2ftpserver('{}/out/{:04d}/{:03d}'.format(goGPSproject, year,day_to_process),'/www.gter.it/concerteaux_gnss/ztd_elaborations/output',i)
    print('ZTD output files (.csv) uploaded to server')

    # Carico i dati elaborati sul DB e li rimuovo dalla cartella locale
    uploadZTDtoDB(goGPSproject, connection,'ztd_bendola',year,day_to_process)

    
else:
    #print('case to be implemented')
    print('Check 1: Esiste il percorso alla cartella di output? ',os.path.exists(goGPSproject+'/out/{:04d}/{:03d}'.format(year,day_to_process)))



