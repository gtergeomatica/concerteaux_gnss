# Repository per la raccolta dati dalle stazioni GNSS, e loro elaborazione al fine di ottenere ZTD

## Schema indicativo dello script scarica_dati.py

![flusso_download](https://github.com/gtergeomatica/concerteaux_gnss/blob/master/img/flusso_download.jpg)


Conversione in automatico da .dat a RINEX
-----------------------------------------

Si utilizza il tool di novatel scaricabile a [questp link](https://www.novatel.com/support/info/documents/809) (scegliere la versione Linux 64 bit)

Estrarre la cartella scaricata e aprire nel terminare la cartella, ed eseguire il file di installazione

```
~$ cd /Scaricati/NovAtel-Convert-Linux-64bit/R1/NovAtelConvert

~$ ./NovAtelConvert_Setup

```
Il programma verrà installato nella cartella /opt/NovAtel Inc/NovAtel Convert

NB perchè il programma funzioni è necessario installare la libreria libxcb-xinput0:

```
~$ sudo apt-get install libxcb-xinput0
```
Per convertire il file .dat a linea di comando la sinstassi è:

```
~$ /opt/NovAtel\ Inc/NovAtel\ Convert/NovAtelConvert -r3.04 /path/to/file.dat
```


Far girare lo script sul crontab
---------------------------------

2 metodi:

* crontab dell'utente: il crontab dell'utente si visualizza con il comando ``` crontab -l ``` e si edita  con il comando  ``` crontab -e ``` la sintassi è la seguente

```
1-59/* * * * /usr/bin/python3 /percorso_assoluto_script/scarica_dati.py hour > /tmp/file_log.txt

```



* crontab generale: si edita sul file /etc/crontab
```
sudo nano /etc/crontab


# aggiungere riga
1-59/* * * * lorenzo /usr/bin/python3 /percorso_assoluto_script/scarica_dati.py hour rinex > /tmp/file_log.txt
```



Bot telegram
------------------------------------------------------------------

servono le librerie telepot e emoji che si possono installare con pip3

e.g.
```
sudo pip3 install telepot
```

Il bot telegram è sempre in ascolto. 
Parte all'avvio del server grazie allo script sh avvio_bot.sh che va personalizzato e che va messo in `/etc/init.d/`


Loggandosi come utente sudo 
1) fare un link degli script in /etc/init.d/ 
2) assegnare i permessi
3) impostare come script di avvio

```
ln -s $(pwd)/avvio_bot.sh /etc/init.d/
chmod +x /etc/init.d/avvio_bot.sh
update-rc.d avvio_bot.sh defaults
```

Il bot si serve di un file credenziali.py che per ovvie ragioni non è caricato suò repository e che ha questo formato :

```
ip='server_host or IP'
db='nome_db'
user='user'
pwd='password'
port='5432' # or different port


#server aruba
user_ftp='XXXXXXXX@aruba.it'
pwd_ftp='XXXXXXXXX'
url_ftp='ftp.dominio'

# cartella montata su gishosting
folder_ftp='/mnt/dav/concerteaux/gnss/rawdata'

rnx_goGPS='ZTD_elaborations/RINEX'


bot_token='XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX' #bot per notifiche file scaricati
chatID_lorenzo='XXXXXXXXX'

link='https://www.gishosting.gter.it/concerteaux'
```


sono stati impostati 3 comandi:
\telegram_id
\webgis
\stato_stazioni

così come tre tasti analoghi qualora non si usi un comando riconosciuto dal bot

Il bot è gestito dal comando **bot_multithread.py**
