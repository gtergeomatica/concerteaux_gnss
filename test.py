import urllib.request
import wget
import sys,os
import scarica_dati

path='./obs_tmp/'
testfile='CAMA00ITA_R_20201001100_01H_01S_Hatanaka-RINEX302.tar.gz'
compression_format='tar.gz'

scarica_dati.uncompressRinex(path,testfile,compression_format)




sys.exit()
path='./obs_tmp/'
obs='CAMA00ITA_R_20200731600_0H_30S_Hatanaka-RINEX302.tar.gz'

conversione=scarica_dati.dat2rinex(path,'CAMA00ITA_R_20201001100_01H_01S.dat', 3.04)
print(conversione)
sys.exit()

os.system('/opt/NovAtel\ Inc/NovAtel\ Convert/NovAtelConvert -r 3.04 ./obs_tmp/CAMA00ITA_R_20201001100_01H_01S.dat'.format(rinex_version,path,binfile))

