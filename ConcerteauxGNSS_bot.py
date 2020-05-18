from credenziali import *
import requests
import psycopg2


def telegram_bot_sendtext(bot_message,chat_id):
    
    send_text = 'https://api.telegram.org/bot' + bot_token + '/sendMessage?chat_id=' + chat_id + '&parse_mode=Markdown&text=' + bot_message

    response = requests.get(send_text)

    return response.json()
    

#test = telegram_bot_sendtext("Testing Telegram bot")


conn = psycopg2.connect(host=ip, dbname=db, user=user, password=pwd, port=port)

conn.set_session(autocommit=True)
cur = conn.cursor()

messaggio='Report Scaricamento dati GNSS da Stazioni Permaenti\n'

interval='hour' # o day

query1= "SELECT cod FROM concerteaux.stazioni_lowcost WHERE operativa=True"

try:
    cur.execute(query1)
except Exception as e:
    print('errore: ',e)

Stazioni_new=[i[0] for i in cur.fetchall()] 

Stazioni=['BEAN','CAMA']

for stz in Stazioni:

    query="SELECT rinex_data FROM meteognss_ztd.log_dw_rinexdata_{} where staz='{}' and cod_dw != 0 order by rinex_data desc limit 24;".format(interval,stz)
    try:
        cur.execute(query)
    except:
        print('errore.... scrivo nel log?')


    #arretrati_tbd=cur.fetchall()
    messaggio+="\nStazione "+stz+"\nFile arretrati:\n"
    for j in cur.fetchall():
        messaggio+=j[0]+'\n'
    messaggio+="\n"
    

telegram_bot_sendtext(messaggio,chatID_lorenzo)