import requests,json
import mysql.connector
from time import time, sleep

cnx = mysql.connector.connect(user='root', password='',host='localhost',database='shopee')
mycursor = cnx.cursor()
mycursor.execute("SELECT item_id,item_name,item_brand FROM product")

myresult = mycursor.fetchall()
for (item_id,item_name,item_brand) in myresult:
    print(item_name)
# while True:
#     sleep(60 - time() % 60)
# #     for x in myresult:
# #         print(x)
# #         payload={'name':"x.product_id",'job':"product_name"}
# #         r=requests.post('https://reqres.in/api/users',json=payload)
# #
# #         print(r)
# #         print(r.text)
# #
#
# payload={'client_id':'paramount','client_secret':'admin'}
# r = requests.post('https://idra-ump.com/test/app/extern/v1/authenticate', json=payload)
# access_para=json.loads(r.text)
# access_tokenpara=access_para['access_token']
# refresh_para=json.loads(r.text)
# refresh_tokenpara=refresh_para['refresh_token']
# token_para=json.loads(r.text)
# token_typepara=token_para['token_type']
# while True:
#     sleep(60 - time() % 60)
for item_id,item_name,item_brand  in myresult:
    print(item_id)
    mycursor.execute("update product set item_brand= 3")
    cnx.commit()
    print()
    # mycursor.execute(" UPDATE product SET b[i] = 2  WHERE '%s' is not null or '%s' !=''")
    # payloads={'mrSerialNumber':"x.product_id"}
    # ab = requests.post('https://idra-ump.com/app/extern/v1/money-receipt', json=payloads,headers={'Authorization': f"Bearer {access_tokenpara}"})
    # print(ab)

