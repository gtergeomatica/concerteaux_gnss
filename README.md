# Repository per la raccolta dati dalle stazioni GNSS, e loro elaborazione al fine di ottenere ZTD

TODO istruzioni (lorenzo)

Far girare lo script sul crontab
---------------------------------

2 metodi:

* crontab dell'utente: il crontab dell'utente si visualizza con il comando ``` crontab -l ``` e si edita  con il comando  ``` crontab -e ``` la sintassi è la seguente

```
1-59/* * * * /usr/bin/python3 /percorso_assoluto_script/scarica_dati.py hour rinex > /tmp/file_log.txt

```



* crontab generale: si edita sul file /etc/crontab
```
sudo nano /etc/crontab


# aggiungere riga
1-59/* * * * lorenzo /usr/bin/python3 /percorso_assoluto_script/scarica_dati.py hour rinex > /tmp/file_log.txt
```
