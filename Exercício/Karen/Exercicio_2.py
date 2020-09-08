#Uma função de cadastro para cada tabela (pelo terminal) usando comando input

import MySQLdb
from random import randint, uniform

db = MySQLdb.connect(host="127.0.0.1",
			user="karen",
			passwd="password",
			db="Cliente")
cur = db.cursor()

#cur.execute("INSERT INTO Person VALUES('danforth', 'Frank', 'Danforth');")
#cur.execute("INSERT INTO Person VALUES('dyer', 'William', 'Dyer');")

valor1 = input("Digite o valor para o campo 'id' da Tabela Person: ")
valor2 = input("Digite o valor para o campo 'family' da Tabela Person: ")
valor3 = input("Digite o valor para o campo 'personal' da Tabela Person: ") 

cur.execute("INSERT INTO Person VALUES(%s,%s,%s)", (valor1, valor2, valor3))

db.commit()

cur.execute("SELECT * FROM Person")

l = cur.fetchall() ## busca todas as linhas ou;

print(type(l))

for linha in l:
        print (linha[0], linha[1], linha[2]) # valor da primeira coluna, segunda e terceira

#############db.close()

valor11 = input("Digite o valor para o campo 'name' da Tabela Site: ")
valor22 = float(input("Digite o valor para o campo 'lat' da Tabela Site: "))
valor33 = float(input("Digite o valor para o campo 'long' da Tabela Site: "))

cur.execute("INSERT INTO Site VALUES(%s,%s,%s)", (valor11, valor22, valor33))

db.commit()

cur.execute("SELECT * FROM Site")

l2 = cur.fetchall() ## busca todas as linhas ou;

print(type(l2))

for linha in l2:
        print (linha[0], linha[1], linha[2]) # valor da primeira coluna, segunda e terceira

############db.close()

valor111 = int(input("Digite o valor para o campo 'id' da Tabela Visited: "))
valor222 = input("Digite o valor para o campo 'family' da Tabela Visited: ")
valor333 = input("Digite o valor para o campo 'personal' da Tabela Visited: ") 

cur.execute("INSERT INTO Visited VALUES(%s,%s,%s)", (valor111, valor222, valor333))

db.commit()

cur.execute("SELECT * FROM Visited")

l3 = cur.fetchall() ## busca todas as linhas ou;

print(type(l3))

for linha in l3:
        print (linha[0], linha[1], linha[2]) # valor da primeira coluna, segunda e terceira

#db.close()

valor1111 = int(input("Digite o valor para o campo 'taken' da Tabela Survey: "))
valor2222 = input("Digite o valor para o campo 'person' da Tabela Survey: ")
valor3333 = input("Digite o valor para o campo 'quant' da Tabela Survey: ") 
valor4444 = float(input("Digite o valor para o campo 'reading' da Tabela Survey: ") )

cur.execute("INSERT INTO Survey VALUES(%s,%s,%s,%s)", (valor1111, valor2222, valor3333, valor4444))

db.commit()

cur.execute("SELECT * FROM Survey")

l4 = cur.fetchall() ## busca todas as linhas ou;

print(type(l4))

for linha in l4:
        print (linha[0], linha[1], linha[2], linha[3]) # valor da primeira coluna, segunda e terceira

db.close()
