# Cadastro de linhas com valores aleatórios em cada tabela

import MySQLdb
from random import randint, uniform

db = MySQLdb.connect(host="127.0.0.1",
			user="karen",
			passwd="password",
			db="Cliente")
cur = db.cursor()

#cur.execute("INSERT INTO Person VALUES('danforth', 'Frank', 'Danforth');")
#cur.execute("INSERT INTO Person VALUES('dyer', 'William', 'Dyer');")

valor1 = randint(1,100)
valor2 = randint(1,100)
valor3 = randint(1,100) 
valor4 = uniform(1,100)

cur.execute("INSERT INTO Person VALUES(%s,%s,%s)", (valor1, valor2, valor3))

db.commit()

cur.execute("SELECT * FROM Person")

l = cur.fetchall() ## busca todas as linhas ou;

print(type(l))

for linha in l:
        print (linha[0], linha[1], linha[2]) # valor da primeira coluna, segunda e terceira

#db.close()

valor11 = randint(1,100)
valor22 = uniform(1,100)
valor33 = uniform(1,100) 

cur.execute("INSERT INTO Site VALUES(%s,%s,%s)", (valor11, valor22, valor33))

db.commit()

cur.execute("SELECT * FROM Site")

l2 = cur.fetchall() ## busca todas as linhas ou;

print(type(l2))

for linha in l2:
        print (linha[0], linha[1], linha[2]) # valor da primeira coluna, segunda e terceira

#db.close()


cur.execute("INSERT INTO Visited VALUES(%s,%s,%s)", (valor1, valor2, valor3))

db.commit()

cur.execute("SELECT * FROM Visited")

l3 = cur.fetchall() ## busca todas as linhas ou;

print(type(l3))

for linha in l3:
        print (linha[0], linha[1], linha[2]) # valor da primeira coluna, segunda e terceira

#db.close()

cur.execute("INSERT INTO Survey VALUES(%s,%s,%s,%s)", (valor1, valor2, valor3, valor4))

db.commit()

cur.execute("SELECT * FROM Survey")

l4 = cur.fetchall() ## busca todas as linhas ou;

print(type(l4))

for linha in l4:
        print (linha[0], linha[1], linha[2], linha[3]) # valor da primeira coluna, segunda e terceira

db.close()
