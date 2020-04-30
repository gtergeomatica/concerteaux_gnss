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
