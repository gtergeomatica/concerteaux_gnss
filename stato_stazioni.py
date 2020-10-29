#!/usr/bin/env python3
# Copyleft Gter srl 2020
# # -*- coding: utf-8 -*-
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


def main():

    conn = psycopg2.connect(host=ip, dbname=db, user=user, password=pwd, port=port)
    conn.set_session(autocommit=True)
    cur = conn.cursor()
    query='SELECT cod, host, port FROM concerteaux.stazioni_lowcost;'
    try:
        cur.execute(query)
        stazioni=cur.fetchall()
    except Exception as e:
        logging.error(e)

    logging.debug(query)
    logging.debug(stazioni)

    #messaggio= "\033[1m"+'CONTROLLO OPERATIVITA\' STAZIONI GNSS\n\n'+"\033[0m"
    messaggio= 'CONTROLLO OPERATIVITA\' STAZIONI GNSS\n\n'
    for s in stazioni:
        host='http://{}:{}'.format(s[1],s[2])
        status=station_on(host)
        logging.info(status)
        if status=='t':
            messaggio+='{}:  operativa {}\n'.format(s[0],emoji.emojize(" :white_check_mark:", use_aliases=True))
            
        elif status=='f':
            messaggio+='{}:  non operativa {}\n'.format(s[0], emoji.emojize(" :x:", use_aliases=True))

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

    # da mettere ciclo con chatID UNIGE oppure da fare roba analoga ma con comando dal bot
    telegram_bot_sendtext(messaggio,chatID_lorenzo)
    
if __name__ == "__main__":
    main()