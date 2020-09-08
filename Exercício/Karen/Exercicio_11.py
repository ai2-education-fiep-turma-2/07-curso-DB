# Retorne a quantidade de medições realizadas por cada pessoa na tabela person

import MySQLdb
from random import randint, uniform

db = MySQLdb.connect(host="127.0.0.1",
			user="karen",
			passwd="password",
			db="Cliente")
cur = db.cursor()

cur.execute("select id,count(person) from Person join Survey on Person.id=Survey.person group by person ;")

l = cur.fetchall() ## busca todas as linhas ou;
db.close()

for linha in l:
        print (linha[0], linha[1]) # valor da primeira coluna, segunda e terceira
