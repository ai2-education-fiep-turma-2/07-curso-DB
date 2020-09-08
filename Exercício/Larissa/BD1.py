import datetime
import MySQLdb
import math
import random


def connectDb():
    host="127.0.0.1"
    user="larissa"
    passwd="password"
    db="teste"

    return MySQLdb.connect(host=host,    
                     user=user,         
                     passwd=passwd,  
                     db=db)

def insertale():
    db = connectDb()
    cur = db.cursor()

    # Inserção Person
    nome_ale = ['Alícia', 'Bernardo', 'Caetano', 'Douglas', 'Elisa', 'Félix', 'Graciela', 
    'Helen', 'Ingrid', 'Juarez', 'Karina', 'Laila', 'Marcelo', 'Noemi', 'Olívia', 'Pedro',
    'Quinn', 'Romeo', 'Stefano', 'Teodoro', 'Ulisses', 'Vivian', 'William', 'Ximena', 'Yara',
    'Zara']
    family_ale = ['Abreu', 'Brito', 'Campos', 'Dalto', 'Esteves', 'Ferreira', 'Guerra',
    'Harper', 'Ignácio', 'Jacinto', 'Klein', 'Lins', 'Marín', 'Nunes', 'Oliveira', 'Porres',
    'Queiroz', 'Rabelo', 'Salles', 'Teixeira', 'Urbino', 'Viana', 'Walker', 'Xavier', 'Zuza']

    chosen_name = random.choice(nome_ale)
    chosen_family = random.choice(family_ale)

    uniqueId = createUniqueId(chosen_family,2,'Person','id')

    cur.execute("INSERT INTO Person VALUES('"+uniqueId+"','"+chosen_name+"','"+chosen_family+"');")
    db.commit()

    # Inserção Site
    site_ale = ['BR', 'USA', 'JA', 'CH', 'SU', 'RU', 'PA', 'LO', 'MA']
    
    chosen_site = random.choice(site_ale)
    lat = random.uniform(-125.0, 125.0)
    lon = random.uniform(-125.0, 125.0)

    uniqueSite = createUniqueId(chosen_site,2,'Site','nome')

    cur.execute("INSERT INTO Site VALUES('"+uniqueSite+"',%s,%s);", (lat, lon))
    db.commit()

    # Inserção de Visited

    cur.execute("SELECT nome FROM Site ORDER BY RAND() LIMIT 1")
    recordSiteId = cur.fetchall()
    
    cur.execute("SELECT MAX(id) FROM Visited")
    recordMaxId = cur.fetchall()

    date = datetime.datetime.strptime('{} {}'.format(random.randint(1, 366), 2020), '%j %Y')

    cur.execute("INSERT INTO Visited VALUES("+(str(int(recordMaxId[0][0]) + 1))+",'"+recordSiteId[0][0]+"','"+str(date)[:10]+"');")
    db.commit()

    # Inserção de Survey

    quant_ale = ["rad", "sad", "temp"]
    chosen_quant = random.choice(quant_ale)

    reading = random.uniform(0.1, 10.0)
    reading_data = "%.2f" % reading

    cur.execute("SELECT id FROM Visited ORDER BY RAND() LIMIT 1")
    recordVisitedId = cur.fetchall()
    
    cur.execute("SELECT id FROM Person ORDER BY RAND() LIMIT 1")
    recordPersonId = cur.fetchall()

    cur.execute("INSERT INTO Survey VALUES("+str(recordVisitedId[0][0])+",'"+recordPersonId[0][0]+"','"+chosen_quant+"',"+reading_data+");")
    db.commit()

    db.close()

def createUniqueId(id,broker,table,idColumn):
    db = connectDb()
    cur = db.cursor()

    sql_select_query = "SELECT COUNT(*) FROM "+table+" WHERE "+idColumn+" = '"+id[0:broker]+"'"
    cur.execute(sql_select_query)
    record = cur.fetchall()

    db.close()

    if(record[0][0] == 0):
        return id[0:broker]
    else:
        if(broker < len(id)):
            return createUniqueId(id,(broker+1),table,idColumn)
        else:
            return createUniqueId(id+'x',(broker+1),table,idColumn)
            
def consultas():
    db = connectDb()
    cur = db.cursor()

    # Listar quantidade de visitas que cada site recebeu
    cur.execute("select DISTINCT site, count(dated) from Visited group by site")
    record = cur.fetchall()

    print('1 - Listar quantidade de visitas que cada site recebeu:')
    for line in record:
        print(line[0],'teve',line[1],'visitas')

    # Listar sites que nao receberam visitas
    cur.execute("select nome from Site where nome not in (select DISTINCT site from Visited)")
    record = cur.fetchall()

    print('')
    print('2 - Listar sites que nao receberam visitas:')
    for line in record:
        print(line[0])

    # Listar métricas que foram observadas na tabela survey
    cur.execute("SELECT * FROM Survey")
    record = cur.fetchall()

    print('')
    print('3 - Listar métricas que foram observadas na tabela survey:')
    for line in record:
        print('Na vistia',line[0],line[1],'fez uma leitura de',line[3],'na medição de',line[2])

    # Listar pessoas que fizeram mais de dois levantamentos
    cur.execute("select b.person from (select DISTINCT ss.person, count(ss.quant) as quantity from Survey ss group by ss.person) b where b.quantity >= 2")
    record = cur.fetchall()

    print('')
    print('4 - Listar sites que nao receberam visitas:')
    for line in record:
        print(line[0])

    # Listar pessoas que o sobrenome possua DYR no meio da palvra
    cur.execute("select * from Person where family like '%DYR%'")
    record = cur.fetchall()

    print('')
    print('5 - Listar pessoas que o sobrenome possua DYR no meio da palvra:')
    for line in record:
        print(line[1], line[2])

    # Listar visitacoes a uma lista de sites passados como parâmetro
    sites = ['DR-1', 'USAx', 'LO']
    sites = str(sites).replace('[','(')
    sites = sites.replace(']',')')
    cur.execute("SELECT * FROM Visited WHERE site IN "+sites+"")
    record = cur.fetchall()

    print('')
    print('6 - Listar visitacoes a uma lista de sites passados como parâmetro:')
    for line in record:
        print(line[1],'foi visitado no',line[2])

    # verifique quantas linhas possuem valor nulo na coluna quant na tabela survey
    cur.execute("SELECT count(*) FROM Survey WHERE quant is null")
    record = cur.fetchall()

    print('')
    print('7 - verifique quantas linhas possuem valor nulo na coluna quant na tabela survey:')
    print(record[0][0])

    # retorne a media de lat lon utilizando como parametro de busca um intervalo de datas
    dateMin = '2020-01-01'
    dateMax = '2020-04-01'
    cur.execute("select avg(lat), avg(`long`) from Site where nome in (select distinct site from Visited where (dated BETWEEN '"+dateMin+"' AND '"+dateMax+"'))")
    record = cur.fetchall()

    print('')
    print('8 - retorne a media de lat lon utilizando como parametro de busca um intervalo de datas:')
    print('A media da Latitude foi:',record[0][0])
    print('A media da Longitude foi:',record[0][1])

    # Retorne a quantidade de medições realizadas por cada pessoa na tabela person
    cur.execute("select p.personal as person, COUNT(s.reading) as quantity from Person p left JOIN Survey s on s.person = p.id group by p.personal")
    record = cur.fetchall()

    print('')
    print('9 - Retorne a quantidade de medições realizadas por cada pessoa na tabela person:')
    for line in record:
        print(line[0],'fez',line[1],'medições')

    # retorne a pessoa que tem a maior quantidade de medições de temperatura entre 3 e 7
    cur.execute("select person, count(reading) as counter from Survey where reading >= 3 and reading <= 7  group by person order by counter desc limit 1")
    record = cur.fetchall()

    print('')
    print('10 - retorne a pessoa que tem a maior quantidade de medições de temperatura entre 10 e 30:')
    print(record[0][0])

    db.close()

#insertale()
consultas()