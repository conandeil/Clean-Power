##https://developers.mercadolibre.com/en_us/messaging-after-sale
##https://developers.mercadolibre.com/en_us/manage-sales
##Benjamin order_id: 1857699114 Eduardo Rivera ODONIANO  erivera.45xz6p+2-oge4dknzwhe4tcnbz@mail.mercadolibre.cl
##klient_id: 140469461

import sys, os, json, codecs, requests, datetime, time, csv
yy = os.path.realpath(__file__)
fof = os.path.split(yy)[0]
##pathDB = fof + '\Message.db'

from dateutil.parser import parse
from datetime import datetime, date, timedelta

sys.path.append(os.path.realpath (fof)[:-11] + 'ML\\')
import ML
import ML_TOKKEN

s = ML_TOKKEN
ACCESS_TOKEN = s.tok()

##_________________________________________________________________________________________________________
########Это работает получить все последние активные продажи
url = 'http://free.currencyconverterapi.com/api/v3/convert?q=USD_CLP&compact=ultra'
rate = float(requests.get(url).json().get('USD_CLP'))

nn = 0
summa = 0
while True:
    url = 'https://api.mercadolibre.com/orders/search/recent?seller=251157263&offset=' \
           + str(nn) + '&limit=50&order.status=paid&access_token=' + ACCESS_TOKEN

    url = 'https://api.mercadolibre.com/orders/search?seller=251157263&offset=' + str(nn) + \
          '&limit=50&order.status=paid' \
          '&order.date_created.from=2018-12-01T00:00:00.000-00:00' \
          '&order.date_created.to=2018-12-31T00:00:00.000-00:00' \
          '&access_token=' + ACCESS_TOKEN
    
    rows = requests.get(url).json()
    
    results = rows.get('results')

    mm = 0
    for ii in results[:]:
##        print(ii)
##        print(ggggg)
        
        ID = ii.get('id')
        date_closed = ii.get('date_closed')
        expiration_date = ii.get('expiration_date')
        date_created = ii.get('date_created')
        total_amount = int(ii.get('total_amount'))
        buyer = ii.get('buyer')
        buyer_id = buyer.get('id')
        email = buyer.get('email')
        first_name = buyer.get('first_name')
        last_name = buyer.get('last_name')
        nickname = buyer.get('nickname')

        date_last_updated = ii.get('date_last_updated')

        shipping = ii.get('shipping')
        me2 = shipping.get('shipping_mode')

        payments = ii.get('payments')[0]
        prod_name = payments.get('reason').replace('\xa0','')
        aa = payments.get('date_last_modified')
        a = aa.split('T')[0]
        start_date = datetime.date(parse(a))
                
        buyer = buyer.get('buyer')
        
        order_items = ii.get('order_items')[0]
        item = order_items.get('item')
        title = item.get('title')
        
        print(start_date, first_name, total_amount)

        summa = summa + total_amount
        


        mm += 1
    if len(results) == 0: break
    nn += 50
print('_______________________________________________\n')

print('ИТОГО за период  ', int(summa*0.012/rate))


