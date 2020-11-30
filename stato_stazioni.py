#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# Copyleft Gter srl 2020
# Roberto Marzocchi, Lorenzo Benvenuto


import os,sys
#import subprocess
import logging
spath=os.path.dirname(os.path.realpath(__file__))
#exit()
logging.basicConfig(
    format='%(asctime)s\t%(levelname)s\t%(message)s',
    #filemode ='w',
    #filename='{}/log/conversione_oracle_19.log'.format(spath),
    level=logging.DEBUG)

import emoji
import psycopg2
import requests 
from credenziali import *


import urllib.request
import urllib.error

def station_on(hostname):

    try:
        urllib.request.urlopen(hostname, timeout=5)
        status = "t"
        
    except urllib.error.URLError as err:
        status = "f"
        logging.error('Stazione {} non attiva'.format(hostname))

    return status




def telegram_bot_sendtext(bot_message,chat_id):
    
    send_text = 'https://api.telegram.org/bot' + bot_token + '/sendMessage?chat_id=' + chat_id + '&parse_mode=Markdown&text=' + bot_message

    response = requests.get(send_text)

    return response.json()

def chatIDlist ():
    '''
    funzione che restituisce la lista con i chat ID di telegram salvati sul DB
    '''
    conn = psycopg2.connect(host=ip, dbname=db, user=user, password=pwd, port=port)
    conn.set_session(autocommit=True)
    cur = conn.cursor()
    query='SELECT telegram_id FROM concerteaux.telegram_utenti where valido =\'t\';'
    try:
        cur.execute(query)
        cid=cur.fetchall()
    except Exception as e:
        logging.error(e)
    return [i[0] for i in cid]
    print(cid)

def main():
    
    conn = psycopg2.connect(host=ip, dbname=db, user=user, password=pwd, port=port)
    conn.set_session(autocommit=True)
    cur = conn.cursor()
    query='SELECT cod, host, port, operativa FROM concerteaux.stazioni_lowcost order by cod;'
    try:
        cur.execute(query)
        stazioni=cur.fetchall()
    except Exception as e:
        logging.error(e)

    logging.debug(query)
    logging.debug(stazioni)
    print(type(stazioni[0][-1]))
    if stazioni[0][-1]:
        status_old='t'
    else:
        status_old='f'
    print(status_old)
    operativo_old={}
    for j in stazioni:
        print(j[-1])
        if j[-1]:
            
            status_old='t'
        else:
           
            status_old='f'
        
        operativo_old[j[0]]=status_old
    #print(operativo_old)
    #sys.exit()

    #messaggio= "\033[1m"+'CONTROLLO OPERATIVITA\' STAZIONI GNSS\n\n'+"\033[0m"
    messaggio= 'CONTROLLO OPERATIVITA\' STAZIONI GNSS\n\n'
    #messaggio_cambio='\xE2\x9A\xA0 ATTENZIONE: UNA O PIU\' STAZIONI HANNO CAMBIATO LO STATO DI ATTIVITA\': \n'
    messaggio_cambio='{} ATTENZIONE: UNA O PIU\' STAZIONI HANNO CAMBIATO LO STATO DI ATTIVITA\': \n'.format(emoji.emojize(" :warning:", use_aliases=True))
    send_cambio=False
    for s in stazioni:
        host='http://{}:{}'.format(s[1],s[2])
        status=station_on(host)
        logging.info(status)
        if status=='t':
            messaggio+='{}:  operativa {}\n'.format(s[0],emoji.emojize(" :white_check_mark:", use_aliases=True))
            
        elif status=='f':
            messaggio+='{}:  non operativa {}\n'.format(s[0], emoji.emojize(" :x:", use_aliases=True))

        print(s[0], status)
        print(operativo_old[s[0]])
        if status==operativo_old[s[0]]:
            logging.info('non è cambiato nulla')
        else:
            send_cambio=True
            if status=='t':
                logging.info('la stazione {} è diventata operativa'.format(s[0]))
                messaggio_cambio+='la stazione {} è diventata operativa {}\n'.format(s[0],emoji.emojize(" :white_check_mark:", use_aliases=True))
            elif status=='f':
                logging.info('{} è diventata non operativa '.format(s[0]))
                messaggio_cambio+='{} è diventata non operativa {}\n'.format(s[0],emoji.emojize(" :x:", use_aliases=True))

        logging.info(messaggio)
        cur2 = conn.cursor()
        query='''UPDATE concerteaux.stazioni_lowcost 
        SET operativa='{}', update = now() 
        WHERE cod='{}';'''.format(status, s[0])
        logging.debug(query)
        try:
            cur2.execute(query)
        except Exception as e:
            logging.error(e)
        cur2.close()
        
    cur.close()
    conn.close()

    
    # telegram_bot_sendtext(messaggio,chatID_lorenzo)
    utenti_bot=chatIDlist()
    if send_cambio:
        for i in utenti_bot:
            telegram_bot_sendtext(messaggio_cambio,i)
        send_cambio=False
    
if __name__ == "__main__":
    main()