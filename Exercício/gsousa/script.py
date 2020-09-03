import argparse
import psycopg2 as db_driver

parser = argparse.ArgumentParser(description='Read from database')
parser.add_argument('--ex1', action="store_true", help="Exercício 1")
parser.add_argument('--ex2', action="store_true", help="Exercício 2")
parser.add_argument('--ex3', action="store_true", help="Exercício 3")
parser.add_argument('--ex4', action="store_true", help="Exercício 4")
parser.add_argument('--ex5', action="store_true", help="Exercício 5")
parser.add_argument('--ex6', action="store_true", help="Exercício 6")
parser.add_argument('--ex7', action="store_true", help="Exercício 7")
parser.add_argument('--ex8', action="store_true", help="Exercício 8")
parser.add_argument('--ex9', action="store_true", help="Exercício 9")
parser.add_argument('--ex10', action="store_true", help="Exercício 10")

opts = parser.parse_args()

with db_driver.connect("host='localhost' dbname='test' user='admin' password='admin'") as conn:
    cursor = conn.cursor()

    if opts.ex1:
        print('-----------\nExercício 01:\n')
        cursor.execute(
            'select s.name, count(v.id)  from visited as v \
            right join site as s on s.name = v.site \
            group by s.name;')
        for r in cursor.fetchall():
            print(r)

    if opts.ex2:
        print('-----------\nExercício 02:\n')
        cursor.execute(
            'select s.name, count(v.id)  from visited as v \
            right join site as s on s.name = v.site \
            group by s.name \
            having count(v.id) = 0;')
        for r in cursor.fetchall():
            print(r)

    if opts.ex3:
        print('-----------\nExercício 03:\n')
        cursor.execute(
            'select s.quant from survey as s \
            group by s.quant;')
        for r in cursor.fetchall():
            print(r)

    if opts.ex4:
        print('-----------\nExercício 04:\n')
        cursor.execute(
            'select p.id, count(s.taken) from person as p \
            inner join survey as s on s.person = p.id \
            group by p.id \
            having count(s.taken) > 2;')
        for r in cursor.fetchall():
            print(r)

    if opts.ex5:
        print('-----------\nexercício 05:\n')
        cursor.execute(
            "select * from person as p \
            where lower(p.family) like lower('%D%Y%R%');"
        )
        for r in cursor.fetchall():
            print(r)

    if opts.ex6:
        print('-----------\nexercício 06:\n')
        site = input('digite o código (ou expressão regular) do local: ')
        cursor.execute(
            'select * from list_visit(\'{}\');'.format(site))
        for r in cursor.fetchall():
            print(r)

    if opts.ex7:
        print('-----------\nexercício 07:\n')
        cursor.execute(
            'select count(s.quant) from survey as s \
            where s.quant is null;')
        for r in cursor.fetchall():
            print(r)

    if opts.ex8:
        print('-----------\nexercício 08:\n')
        m1 = input('digite a data m1: ')
        m2 = input('digite a data m2: ')
        cursor.execute(
            "select * from avg_location('{}','{}')".format(m1, m2))
        for r in cursor.fetchall():
            print(r)

    if opts.ex9:
        print('-----------\nexercício 09:\n')
        cursor.execute(
            "select p.id, p.personal || ' ' || p.family as \"Name\", count(s.taken) from person as p \
            left join survey as s on s.person = p.id \
            group by p.id;")
        for r in cursor.fetchall():
            print(r)

    if opts.ex10:
        print('-----------\nexercício 10:\n')
        cursor.execute(
            "select p.id, p.personal || ' ' || p.family as \"Name\", count(s.taken) from person as p \
            left join survey as s on s.person = p.id \
            where s.quant = 'temp' and s.reading >= 10 and s.reading < 30 \
            group by p.id \
            order by count(s.taken) desc \
            limit 1;")
        for r in cursor.fetchall():
            print(r)
