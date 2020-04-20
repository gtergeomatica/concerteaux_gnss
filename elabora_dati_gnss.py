import wget
import sys,os
import datetime

def getObsIGS(station,year,doy):
    url='ftp://cddis.nasa.gov/gnss/data/daily/{}/{}/{}o/'.format(year,doy,str(year)[2:])
    print(url)
    filename='{}{}0.{}o.Z'.format(station,doy,str(year)[2:])
    print(filename)
    try:
        obsfile=wget.download('{}{}'.format(url,filename),out='./test_automatiz/RINEX')
        print(type(obsfile))
    except Exception as e:
        print('ciao')
        print(e)
    
    os.system('uncompress {}'.format(obsfile))
    return obsfile[:-2]
    


def modifyINIfile(inifile,doy,anno):
    '''
    function to modify the configuration file for elaboration with goGPS
    
    
    '''
    oldfilelines=[]
    newINIfilelines=[]
    with open(inifile, 'r') as configfile:
        oldfilelines=configfile.readlines()
    
    #print(newINIfile_list)

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
        
   
    #for line in newINIfilelines:
    #    print(line)     
    #write the new ini file
    
    prject_path='./test_automatiz/config/'
    newINIfile='elab_{}_{}.ini'.format(anno,doy)
    
    with open ('{}{}'.format(prject_path,newINIfile),'a') as newfile:
        for line in newINIfilelines:
            newfile.write(line)
    
    return('{}{}'.format(prject_path,newINIfile))
    
        
        
def launchgoGPS(inifile):
    '''
    function to excecute goGPS with a ini file and no GUI
    '''
    
    with open ('launch_goGPStemp.m','a') as setfile:
        setfile.write('clear all\nclose all\n')
        setfile.write('cd \'/home/lorenzo/source_codes/goGPS_MATLAB/goGPS\'\n')
        setfile.write('goGPS(\'{}\',0);\n'.format(inifile))
        setfile.write('exit')       
        
    os.system('matlab -r launch_goGPStemp')
    os.system('rm launch_goGPStemp.m')
        
        
    #scrivo il nuovo inifile  

        
        
        
   
anno=2019
giorno=319

#filename=wget.download('{}{}'.format(url,rinex_name))

stazioni=['gras','geno','ieng']

#rinex_in=[getObsIGS(i,anno,giorno) for i in stazioni]
#print(rinex_in) 

inifile_path='./test_automatiz/config/elab_partenza.ini'

new_ini_file=modifyINIfile(inifile_path,giorno,anno)

launchgoGPS(new_ini_file)