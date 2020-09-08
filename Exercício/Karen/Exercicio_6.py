#Consultas
 ##Listar pessoas que fizeram mais de dois levantamentos

import MySQLdb

db = MySQLdb.connect(host="127.0.0.1",
                        user="karen",
                        passwd="password",
                        db="Cliente")
cur = db.cursor()

cur.execute("SELECT person, count(*) from Survey group by person having count(*) > 2 ;")

l = cur.fetchall() ## busca todas as linhas ou;
db.close()

for linha in l:
        print (linha) # valor da primeira coluna, segunda e terceira


