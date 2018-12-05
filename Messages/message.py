import sys, os, json, codecs, requests, datetime, time, csv
from dateutil.parser import parse
from datetime import datetime, date, timedelta
import sqlite3 as lite
import pandas as pd

yy = os.path.realpath(__file__)
fof = os.path.split(yy)[0]
pathDB = fof + '\Message.db'

sys.path.append(os.path.realpath (fof)[:-11] + 'ML\\')

import ML
import ML_TOKKEN

s = ML_TOKKEN
ACCESS_TOKEN = s.tok()

con = lite.connect(pathDB)
cur = con.cursor()
df = pd.read_sql('select * from messages', con)
id_oder_list = list(df['id_oder'])

text_1 = '''¡Hola [Customer Name]! ¡Gracias por tu compra en UpShop, estamos muy contentos de que nos hayas elegido! Pronto recibirás noticias del estatus de tu pedido. Esperamos que hayas revisado detenidamente la descripción del [Product Title], nuestros Términos y Condiciones y fecha de entrega. Nuestros productos son de importación y tu pedido llegará en 10 a 15 días hábiles como máximo. Puedes retirar en nuestra oficina ubicada en Las Condes, Santiago una vez que confirmemos su llegada. También podemos ofrecerte la opción de enviar tu compra bajo la modalidad por pagar a través de Chilexpress (retiro en sucursal que tú nos indiques) o en algún transporte de tu preferencia (esta opción tiene un recargo de $ 3.000 dentro de Santiago). Si tu producto es eléctrico y/o electrónico, debes tener en cuenta que podría necesitar un transformador de 220 V a 110 V para su correcto funcionamiento. Nuestro horario de atención es de lunes a jueves de 10:00 a 18:00 hrs y viernes hasta las 14:00 hrs, excepto festivos.\n\n
¡Gracias nuevamente!\nEquipo Up-Shop.'''

text_1ME = '''¡Hola [Customer Name]! ¡Gracias por tu compra en Up Shop, estamos muy contentos de que nos hayas elegido! Pronto recibirás noticias del estatus de tu pedido. Esperamos que hayas revisado detenidamente la descripción del [Product Title], nuestros Términos y Condiciones y fecha de entrega. Nuestros productos son de importación y tu pedido llegará a tus manos en 10 a 15 días hábiles como máximo. Si tu producto es eléctrico y/o electrónico, debes tener en cuenta que podría necesitar un transformador de 220 V a 110 V para su correcto funcionamiento. Nuestro horario de atención es de lunes a jueves de 10:00 a 18:00 hrs y viernes hasta las 14:00 hrs, excepto festivos.\n\n
¡Gracias nuevamente!\nEquipo Up-Shop.'''

text_2 = '''¡Hola [Customer Name]! Queremos informarte que tu pedido [Product Title] ya viene en tránsito a Chile, pronto te daremos nuevas noticias de su seguimiento.\n\n
¡Gracias nuevamente por tu compra en Up-Shop!'''

text_3 = '''¡Hola [Customer Name] no nos hemos olvidado de ti! Te recordamos que tu número de venta es el [Sales ID Number] y que tu pedido está próximo a arribar a Chile. En los próximos días te confirmaremos su llegada y prepararemos su envío para que llegue cuanto antes a tus manos. Te invitamos a que mientras tanto visites nuestro e-Shop para que veas los miles de productos que tenemos publicados https://listado.mercadolibre.cl/_CustId_251157263\n\n
¡Gracias nuevamente por tu compra en Up-Shop!'''

def reform_date(d):
    a = d.split('T')[0]
    aa = datetime.date(parse(a))
    return aa

def message(con, cur, df, text, col):
    
    for index, row in df.iterrows():
        first_name = row['first_name']
        last_name = row['last_name']
        id_buyer = int(row['id_buyer'])
        id_oder = int(row['id_oder'])
        prod_name = row['prod_name']

        mess = text.replace('[Customer Name]', first_name + ' ' + last_name)
        mess = mess.replace('[Product Title]', '"' + prod_name + '"')
        mess = mess.replace('[Sales ID Number]', '"' + str(id_buyer) + '"')
        
        values = {"from": {"user_id": 251157263},
                  "to": [{"user_id": id_buyer,
                          "resource": "orders",
                          "resource_id": id_oder,
                          "site_id": "MLC"}],
                  "subject": "Equipo Up-Shop",
                  "text": {"plain": mess}}

        url = 'https://api.mercadolibre.com/messages?access_token=' + ACCESS_TOKEN
        r = requests.post(url, json = values).json()
##        print(values)
        cur.execute("UPDATE messages SET " + col + "='1' WHERE id_oder=?", [id_oder])
        con.commit()


def main():
    nn = 0
    all_id = []
    while True:
        url = 'https://api.mercadolibre.com/orders/search/recent?seller=251157263&offset=' \
               + str(nn) + '&limit=50&order.status=paid&access_token=' + ACCESS_TOKEN
        rows = requests.get(url).json()
        results = rows.get('results')
        for ii in results[:]:
            id_oder = ii.get('id')
            
            all_id.append(id_oder)
            
            payments = ii.get('payments')[0]
            prod_name = payments.get('reason').replace('\xa0','')
            
            aa = payments.get('date_last_modified')
            start_date = reform_date(aa)
            bb = ii.get('expiration_date')
            end_date = reform_date(bb)
            
            total_amount = int(ii.get('total_amount'))

            buyer = ii.get('buyer')
            id_buyer = buyer.get('id')
            first_name = buyer.get('first_name')
            last_name = buyer.get('last_name')
    
            shipping = ii.get('shipping')
            shipping_mode = shipping.get('shipping_mode')
            if shipping_mode == 'me2': me2 = 1
            else: me2 = 0

            if id_oder in id_oder_list: continue
            cur.execute("INSERT INTO messages VALUES(?,?,?,?,?,?,?,?,?,?,?)",
                        [start_date, end_date, '', '', '', first_name, last_name, id_buyer, id_oder, prod_name, me2])

        if len(results) == 0: break
        nn += 50
        
    for ID in id_oder_list:
        if ID in all_id: continue
        cur.execute("DELETE FROM messages WHERE id_oder=?", [ID])
    con.commit()

    df = pd.read_sql('select * from messages', con)
    
    m_1 = df[(df['mes_1'] == '') & (df['me2'] == 0)]
    message(con, cur, m_1, text_1, 'mes_1')

    m_1me = df[(df['mes_1'] != 1) & (df['me2'] == 1)]
    message(con, cur, m_1me, text_1ME, 'mes_1')

    contr_date = datetime.now().date() - timedelta(5)
    m_2 = df[(df['mes_2'] == '') & (df['start'] < str(contr_date))]
    message(con, cur, m_2, text_2, 'mes_2')

    contr_date = datetime.now().date() - timedelta(9)
    m_3 = df[(df['mes_3'] == '') & (df['start'] < str(contr_date))]
    message(con, cur, m_3, text_3, 'mes_3')
    
    
if __name__ == '__main__':
    main()
