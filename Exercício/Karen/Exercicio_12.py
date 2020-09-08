# retorne a pessoa que tem a maior quantidade de medições de temperatura entre 10 e 30

import MySQLdb
from random import randint, uniform

db = MySQLdb.connect(host="127.0.0.1",
                        user="karen",
                        passwd="password",
                        db="Cliente")
cur = db.cursor()

cur.execute("select a_person, max(person_count) FROM (select person as a_person, count(person) as person_count from Survey where ((quant = 'temp') and (reading > 10 and reading < 30)) group by person order by person_count DESC) temptable group by a_person limit 1")

l = cur.fetchall() ## busca todas as linhas ou;
db.close()

for linha in l:
        print (linha[0], linha[1]) # valor da primeira coluna, segunda e tercei>

