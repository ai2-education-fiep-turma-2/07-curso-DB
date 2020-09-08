#Consultas
 ##Listar pessoas que o sobrenome possua DYR no meio da palvra

import MySQLdb

db = MySQLdb.connect(host="127.0.0.1",
                        user="karen",
                        passwd="password",
                        db="Cliente")
cur = db.cursor()

cur.execute("SELECT personal from Person where personal like '%DYR%' ; ;")

l = cur.fetchall() ## busca todas as linhas ou;

for linha in l:
        print (linha) # valor da primeira coluna, segunda e terceira
        
db.close()
