#Consultas
 ##Listar métricas que foram observadas na tabela survey

import MySQLdb
from random import randint, uniform

db = MySQLdb.connect(host="127.0.0.1",
                        user="karen",
                        passwd="password",
                        db="Cliente")
cur = db.cursor()

cur.execute("SELECT DISTINCT quant from Survey ;")

l = cur.fetchall() ## busca todas as linhas ou;
db.close()

for linha in l:
        print (linha) # valor da primeira coluna, segunda e terceira
