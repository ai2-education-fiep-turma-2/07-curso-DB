#Listar visitacoes a uma lista de sites passados como parâmetro

import MySQLdb

db = MySQLdb.connect(host="127.0.0.1",
			user="karen",
			passwd="password",
			db="Cliente")
cur = db.cursor()

quant = input("Quantos sites você deseja verificar? ")

site = []

for i in range(quant):
	site[i] = input ("Digite o", i,"site")


cur.execute("SELECT * from Visited where site in ('DR-1', 'DR-3')  ;")

l = cur.fetchall() ## busca todas as linhas ou;


for linha in l:
        print (linha) # valor da primeira coluna, segunda e terceira

db.close()
