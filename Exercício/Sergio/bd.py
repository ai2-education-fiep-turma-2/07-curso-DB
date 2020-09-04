import MySQLdb
import names
from random import seed
from random import random
from random import randrange

#pip install names

db = MySQLdb.connect(host="127.0.0.1",    
                     user="admin",         
                     passwd="admin",  
                     db="Teste")       

cur = db.cursor()

def insere_aleatorios(): 
      
    nome = names.get_first_name()
    sobrenome = names.get_last_name()
    cur.execute("INSERT INTO Person (personal, family) VALUES('"+nome+"','"+sobrenome+"');")
    
    seed(20) 
    cur.execute("INSERT INTO Site VALUES('DR-"+str(int(randrange(0,30)))+"', '"+ str(random()) +"', '"+ str(random()) +"');")
    
    sites = ["DR-1", "DR-3", "DR-4"]
    indice_sites = int(random()*len(sites))
    cur.execute("INSERT INTO Visited (site, dated) VALUES('"+sites[indice_sites]+"', '1927-02-"+str(randrange(1,31))+"');")
    
    taken = [619, 620, 621]
    indice_taken = int(random()*len(taken))
    med = ['rad', 'sal', 'temp']
    indice_med = int(random()*len(med))
    cur.execute("INSERT INTO Survey VALUES("+str(taken[indice_taken])+", "+str(randrange(1,10))+", '"+str(med[indice_med])+"', '"+str(random())+"')")
    
    db.commit()
    
def insere_dados(tabela, col_1, col_2, col_3, col_4 = ''):
    if (col_4 != ''):
        cur.execute("INSERT INTO "+ tabela +" VALUES ('"+ col_1 +"','"+col_2+"','"+col_3+"','"+col_4+"');")
    else:
        cur.execute("INSERT INTO "+ tabela +" VALUES ('"+ col_1 +"','"+col_2+"','"+col_3+"');")
    db.commit()

def input_dados():
    tabela = input("Digite a tabela: ")
    col_1 = input("Digite a primeira coluna: ")
    col_2 = input("Digite a segunda coluna: ")
    col_3 = input("Digite a terceira coluna: ")

    if(tabela == 'Survey'):
        col_4 = input("Digite a quarta coluna:")
        insere_dados(tabela, col_1, col_2, col_3, col_4)
    else:
        insere_dados(tabela, col_1, col_2, col_3)
    
    print_dados(tabela)
        
def print_dados(tabela):    
    cur.execute("SELECT * FROM "+ tabela)
    l=cur.fetchall()

    print(type(l))
    
    for row in l:
        print (row[0],row[1],row[2])

    db.close()

def consultas():
    print("\n\n Consultas: \n\n")
    print("1- Quantidade de visitas que cada site recebeu")
    print("2- Sites que nao receberam visitas")
    print("3- Métricas que foram observadas na tabela survey")
    print("4- Pessoas que fizeram mais de dois levantamentos")
    print("5- Listar pessoas que o sobrenome possua DYR no meio da palvra")
    print("6- Listar visitacoes a uma lista de sites passados como parâmetro")
    print("7- Verifique quantas linhas possuem valor nulo na coluna quant na tabela survey")
    print("8- Retorne a media de lat lon utilizando como parametro de busca um intervalo de datas")
    print("9- Retorne a quantidade de medições realizadas por cada pessoa na tabela person")
    print("10- Retorne a pessoa que tem a maior quantidade de medições de temperatura entre 10 e 30")
    opcao = int(input())
    
    if(opcao == 1):
        cur.execute("SELECT site,count(*) FROM Visited GROUP BY Site;")
    elif(opcao == 2):
        cur.execute("SELECT Site.name FROM Site LEFT JOIN Visited ON Site.name = Visited.site WHERE Visited.site IS NULL")
    elif(opcao == 3):
        cur.execute("SELECT DISTINCT quant FROM Survey")
    elif(opcao == 4):
        cur.execute("SELECT person,count(*) FROM Survey GROUP BY Person HAVING count(*) >= 2")
    elif(opcao == 5):
        cur.execute("SELECT personal, family FROM Person WHERE family LIKE '%DYR%'")
    elif(opcao == 6):
        cur.execute("SELECT id, site, dated FROM Visited JOIN Site WHERE Site.name = Visited.site")
    elif(opcao == 7):
        cur.execute("SELECT taken, person, reading FROM Survey WHERE Survey.quant IS NULL")
    elif(opcao == 8):
        cur.execute("SELECT avg(lat),avg(lon) FROM Site LEFT JOIN Visited ON Site.name = Visited.site WHERE ((Visited.dated > '1927-01-01') and (Visited.dated < '1933-01-10'))")
    elif(opcao == 9):
        cur.execute("SELECT Person.*, count(Survey.person) FROM Person LEFT Join Survey ON (Person.id = Survey.person) GROUP BY Person.id")
    elif(opcao == 10):
        cur.execute("SELECT person, count(*) as maximo FROM Survey WHERE quant = 'temp' and (reading > 9 and reading < 31) GROUP BY person ORDER BY maximo DESC, person DESC LIMIT 1")
    
    for row in cur.fetchall():
        print(*row, sep = ', ')

insere_aleatorios()
continuar = "s"
while(continuar == "s"):
    print("\n\n Menu de consulta Banco de Dados: \n\n")
    print("1- Cadastrar Dados")
    print("2- Consultas \n")
    opcao = input()

    if(int(opcao) == 1):
        input_dados()
    else:
        consultas()
    
    continuar = input("Deseja continuar? (s p/ sim, n p/ não)")
    
db.close()