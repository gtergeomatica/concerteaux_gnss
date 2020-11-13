#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Roberto Marzocchi copyleft 2020


import os.path
from os import path
import asyncio

import sys, time
# da togliere
import random

import emoji
import telepot

#python 3
from telepot.aio.loop import MessageLoop
#from telepot.aio.namedtuple import InlineKeyboardMarkup, InlineKeyboardButton
from telepot.aio.delegate import pave_event_space, per_chat_id, create_open, per_callback_query_origin
#python2 
#from telepot.loop import MessageLoop
#questo per i tastini
from telepot.namedtuple import InlineKeyboardMarkup, InlineKeyboardButton
#from telepot.delegate import pave_event_space, per_chat_id, create_open


from pprint import pprint
import time
import datetime
import json

import logging
import tempfile

tmpfolder=tempfile.gettempdir() # get the current temporary directory
logfile='{}/bot_telegram.log'.format(tmpfolder)
if os.path.exists(logfile):
    os.remove(logfile)

logging.basicConfig(format='%(asctime)s\t%(levelname)s\t%(message)s',
    filename=logfile,
    level=logging.WARNING)




try:
    # Fall back to Python 2's urllib2
    from urllib2 import urlopen
except:
    # For Python 3.0 and later
    from urllib.request import urlopen

import psycopg2
import credenziali as p

#importo una funzione da altro script python
from stato_stazioni import station_on





# Il token è contenuto nel file config.py e non è aggiornato su GitHub per evitare utilizzi impropri
TOKEN=p.bot_token
link=p.link

check=0
testo_segnalazione=''
testo_segnalazione20=''
alllegato=''
chat_id=''
id_mira=''


def stato_stazioni():
    #conn = psycopg2.connect(host=ip, dbname=db, user=user, password=pwd, port=port) 
    # ora mi connetto al DB
    conn = psycopg2.connect(host=p.ip, dbname=p.db, user=p.user, password=p.pwd, port=p.port)
    conn.set_session(autocommit=True)
    cur = conn.cursor()
    query='SELECT cod, host, port FROM concerteaux.stazioni_lowcost;'
    try:
        cur.execute(query)
        stazioni=cur.fetchall()
    except Exception as e:
        logging.info(e)

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
    #logging.info(messaggio)
    return messaggio


# questa classe usa il ChatHandler telepot.aio.helper.ChatHandler (ossia è in ascolto della chat del BOT)
class MessageCounter(telepot.aio.helper.ChatHandler):
    def __init__(self, *args, **kwargs):
        super(MessageCounter, self).__init__(*args, **kwargs)
        self._count = 0


    async def on_chat_message(self, msg):
        #contatore messaggi
        self._count += 1
        global check
        global testo_segnalazione
        global testo_segnalazione20
        global allegato
        global chat_id
        global id_mira
        self._check1 = check
        logging.info(self._check1)
        content_type, chat_type, chat_id = telepot.glance(msg)
        #content_type, chat_type, chat_id = telepot.glance(msg) #get dei parametri della conversazione e del tipo di messaggio
        try:
            command = msg['text'] #get del comando inviato
        except:
            logging.info("Non è arrivato nessun messaggio")
        try:
            if content_type == 'photo':
                await self.bot.download_file(msg['photo'][-1]['file_id'], '\tmp\file_bot.png')
                allegato = '\tmp\file_bot.png'
                logging.info("Immagine recuperata")
                command="foto"
        except:
            logging.info("Non è arrivato nessuna immagine")

        try:
            nome = msg["from"]["first_name"]
        except:
            nome= ""
        try:
            cognome = msg["from"]["last_name"]
        except:
            cognome= ""
        is_bot = msg["from"]["is_bot"]
        if is_bot=='True':
            await self.sender.sendMessage("ERROR: questo Bot non risponde ad altri bot!")
        elif command == '/telegram_id':
            message = '''Gentile {1} {2} il tuo codice (telegram id) da comunicare a Gter srl è {3}. 
Per maggiori informazioni contatta l'Ing. Lorenzo Benvenuto'''.format(self._count,nome, cognome, chat_id)
            await self.sender.sendMessage(message)
        elif command == '/webgis':
            message = "Gentile {1} {2} il link al webGIS di Concerteaux è {3} ".format(self._count,nome, cognome, link)
            await self.sender.sendMessage(message)
        elif command == '/stato_stazioni':
            sent = '''Gentile {0} {1} il controllo delle stazioni è in corso,
attendi alcuni istanti e ne riceverai l'esito '''.format(nome, cognome)
            logging.info(sent)
            await self.sender.sendMessage(sent)
            testo = stato_stazioni()
            await self.sender.sendMessage(testo)
        else:
                keyboard = InlineKeyboardMarkup(inline_keyboard=[
                             #[InlineKeyboardButton(text='IP del server', callback_data='ip')],
                             #[InlineKeyboardButton(text='START', callback_data='start')],
                             #[InlineKeyboardButton(text='Demo comunicazione', callback_data='proposta')],
                             #[InlineKeyboardButton(text='Inserisci il CF', callback_data='codfisc')],
                             #[InlineKeyboardButton(text='Sito Gter', callback_data='info')],
                             #[InlineKeyboardButton(text='Demo Comunicazione', callback_data='demo_com')],
                             [InlineKeyboardButton(text='Link a webGIS Concerteaux', callback_data='sito')],
                             [InlineKeyboardButton(text='Recupera il tuo Telegram ID', callback_data='chat_id')],
                             [InlineKeyboardButton(text='Verifica lo stato delle stazioni', callback_data='stazioni')],
                             #[InlineKeyboardButton(text='Time', callback_data='time')],
                         ])
                #bot.sendMessage(chat_id, 'Gentile {0} {1} questo è un bot configurato per alcune operazioni minimali, quanto hai scritto non è riconosciuto, invece di fotterti prova con i seguenti tasti:'.format(nome,cognome), reply_markup=keyboard)
                message = "Gentile {} {}, questo è un bot configurato da Gter srl per alcune operazioni minimali e in fase di test.\n" \
                          "\nIl comando che hai inserito non è riconosciuto dal sistema, " \
                          "prova a usare i comandi definiti o i tasti seguenti:".format(nome, cognome)
                await self.sender.sendMessage(message, reply_markup=keyboard)


# questa classe usa il CallbackQueryOriginHandler telepot.aio.helper.CallbackQueryOriginHandler (ossia è in ascolto dei tasti schoacchiati dal BOT)
class Quizzer(telepot.aio.helper.CallbackQueryOriginHandler):
    def __init__(self, *args, **kwargs):
        super(Quizzer, self).__init__(*args, **kwargs)
        self._score = {True: 0, False: 0}
        self._answer = None
        self._messaggio = ''
        self.step = 1
        global check
        logging.info("sono dentro quizzer e check vale{}".format(check))

    async def _show_next_question(self):
        x = random.randint(1,50)
        y = random.randint(1,50)
        sign, op = random.choice([('+', lambda a,b: a+b),
                                  ('-', lambda a,b: a-b),
                                  ('x', lambda a,b: a*b)])
        answer = op(x,y)
        question = 'STEP  %d %d %s %d = ?' % (self.step, x, sign, y)
        choices = sorted(list(map(random.randint, [-49]*4, [2500]*4)) + [answer])

        await self.editor.editMessageText(question,
            reply_markup=InlineKeyboardMarkup(
                inline_keyboard=[
                    list(map(lambda c: InlineKeyboardButton(text=str(c), callback_data=str(c)), choices))
                ]
            )
        )
        return answer



    async  def _chatid(self):
        sent = '''Gentile {1} {2} il tuo codice (telegram id) da comunicare a Gter srl è {0}
Per maggiori informazioni contatta l'Ing. Lorenzo Benvenuto'''.format(self.chat_id,self.nome, self.cognome)
        logging.info(sent)
        logging.info(check)
        await self.editor.editMessageText(sent)
        
    
    
    async  def _webGIS(self):
        sent = "Gentile {0} {1} il link al webGIS di Concerteaux è {2} ".format(self.nome, self.cognome, link)
        logging.info(sent)
        logging.info(check)
        await self.editor.editMessageText(sent)       
    
    
    async def _stato_stazioni(self):
        sent = '''Gentile {0} {1} il controllo delle stazioni è in corso,
attendi alcuni istanti e ne riceverai l'esito '''.format(self.nome, self.cognome)
        logging.info(sent)
        logging.info(check)
        await self.editor.editMessageText(sent) 
        testo = stato_stazioni()
        await self.editor.editMessageText(testo)      
        




    async def on_callback_query(self, msg):
        global testo_segnalazione
        global testo_segnalazione20
        global check
        global id_mira
        query_id, from_id, query_data = telepot.glance(msg, flavor='callback_query')
        #content_type, chat_type, chat_id = telepot.glance(msg)
        self.chat_id=from_id
        #parte copiata
        logging.info('Callback Query:', query_id, query_data)
        try:
            command = msg['text'] #get del comando inviato
        except:
            command="Nessun comando"
        try:
            self.nome = msg["from"]["first_name"]
        except:
            self.nome= ""
        try:
            self.cognome = msg["from"]["last_name"]
        except:
            self.cognome= ""
        is_bot = msg["from"]["is_bot"]
        # non usato era solo un test
        if query_data=='ip':
            my_ip = urlopen('http://ip.42.pl/raw').read()
            message = "Gentile {} {}, l'indirizzo IP del server che ti sta rispondendo è {}".format(nome, cognome,my_ip)
            logging.info(message)
            #bot.sendMessage(chat_id, message)
            #await self.sender.sendMessage(message)
        elif query_data == 'chat_id':
            #message = "Gentile {1} {2} il tuo codice (telegram id) da inserire nell'applicazione è {3}".format(self._count,nome, cognome, chat_id)
            logging.info('Definire la chat_id')
            #chiamo la funzione chat_id
            logging.info(self.chat_id)
            self._answer = await self._chatid()
        elif query_data == 'sito':
            logging.info('ho effettivamente schiacciato il bottone sito')
            self._answer = await self._webGIS()
        elif query_data == 'stazioni':
            logging.info('ho effettivamente schiacciato il bottone sito')
            self._answer = await self._stato_stazioni()
    











# questo è il "main" del BOT che è in ascolto 
bot = telepot.aio.DelegatorBot(TOKEN, [
    #chat
    pave_event_space()(
        per_chat_id(), create_open, MessageCounter, timeout=120),
    # bottoni    
    pave_event_space()(
        per_callback_query_origin(), create_open, Quizzer, timeout=120),
])

loop = asyncio.get_event_loop()
loop.create_task(MessageLoop(bot).run_forever())
logging.info('Listening ...')

loop.run_forever()




# vecchio "main
#bot = telepot.Bot(TOKEN)
#MessageLoop(bot, {'chat': on_chat_message,
#                  'callback_query': on_callback_query}).run_as_thread() 
#stampa su server
#logging.info('Listening ...')
 
 
#while 1:
#    time.sleep(10)


