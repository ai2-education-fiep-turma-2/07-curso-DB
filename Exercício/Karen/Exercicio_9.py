# Verifique quantas linhas possuem valor nulo na coluna quant na tabela survey

import MySQLdb
from random import randint, uniform

db = MySQLdb.connect(host="127.0.0.1",
			user="karen",
			passwd="password",
			db="Cliente")
cur = db.cursor()

cur.execute("SELECT count(*) from Survey where quant is NULL ;")

l = cur.fetchall() ## busca todas as linhas ou;
db.close()

for linha in l:
        print (linha[0]) # valor da primeira coluna, segunda e terceira
