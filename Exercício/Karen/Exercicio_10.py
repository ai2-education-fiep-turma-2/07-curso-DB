# retorne a media de lat lon utilizando como parametro de busca um intervalo de datas

import MySQLdb
from random import randint, uniform

db = MySQLdb.connect(host="127.0.0.1",
			user="karen",
			passwd="password",
			db="Cliente")
cur = db.cursor()

cur.execute("select avg(lat), avg(lon) from Visited join Site on Visited.site=Site.name where ((date > '1930-02-08') and (date < '1932-12-12')) ;")

l = cur.fetchall() ## busca todas as linhas ou;
db.close()

for linha in l:
        print (linha[0], linha[1]) # valor da primeira coluna, segunda e terceira
