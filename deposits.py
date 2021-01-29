import json
from bitcoinrpc.authproxy import AuthServiceProxy, JSONRPCException
import mysql.connector
import os
#SQL connection

mydb = mysql.connector.connect(
    host="localhost",
    user="stepollo",
    passwd="eagrKR6DCc7qnA",
    database="staking",
    autocommit=True
)


ticker='BTC'
#pool_address = 'ciao'

mycursor = mydb.cursor()

rpc = AuthServiceProxy("http://bitcoinrpc:password8@127.0.0.1:8332", timeout=120)

mycursor.execute("SELECT * FROM users")
users_db = mycursor.fetchall()

mycursor.execute("SELECT txid FROM deposits")
deposits_db = mycursor.fetchall()

deposits_db_array = []


mycursor.execute("SELECT * FROM coins WHERE ticker = 'BTC'")
coins_db = mycursor.fetchall()


for x in deposits_db:
        for y in x:
            deposits_db_array.append(y)


for user in users_db:
    deposits_btc = rpc.listtransactions(user[1], 100)
    deposits_btc_array = []

    for a in deposits_btc:
        deposits_btc_array.append(a['txid'])

    for txid in deposits_btc_array:
        if txid not in deposits_db_array:
            get_tx = rpc.gettransaction(txid)
        #print(get_tx)
            for key, value in get_tx.items():
            #print(key + " " + str(value))
                if key == "amount":
                #print(str(value))
                    tx_amount = (str(value))
                if key == "confirmations":
                    tx_conf = int(value)
                #print(tx_conf)
                if key == "txid":
                #print(value)
                    tx_txid = value
                if key == "details":
                    if value[0]["category"] == "receive" and tx_conf > 0:
                    #print(value[0]["address"])
                        tx_address = value[0]["address"]
                    #print(tx_address)
                        sql = "INSERT INTO deposits (email, ticker, address, txid, amount, time) VALUES ('%s', 'BTC', '%s', '%s', %s, CURRENT_TIMESTAMP())"%(user[1], tx_address, tx_txid, float(tx_amount))
                        mycursor.execute(sql)
                        mydb.commit()
                        mycursor.execute("SELECT balance FROM deposit_addresses WHERE address = '%s'"%(tx_address))
                        user_balance = mycursor.fetchone()



                        user_balance2 = float(user_balance[0])
                       
                        user_balance_new = user_balance2 + float(tx_amount)
                        user_balance_new = round(user_balance_new, 8)
                        
                        
                        
                        sql_balance_update = "UPDATE deposit_addresses SET balance = %s WHERE address = '%s'"%(user_balance_new, tx_address)
                        mycursor.execute(sql_balance_update)
                        mydb.commit()
                      
