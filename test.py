import urllib.request
import wget
import sys,os



obs='CAMA00ITA_R_20200780800_01H_30S_Hatanaka-RINEX302.tar.gz'

url='https://www.gter.it/concerteaux_gnss/rawdata/CAMA/dati_orari/{}'.format(obs)
output_directory ='./downloaded_raw_data/CAMA/'
try:
    wget.download(url, out=output_directory)
except Exception as e:
    print("Could not download for reason ",str(e))
    
#print(filename)