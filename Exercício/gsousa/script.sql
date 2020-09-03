drop table if exists person cascade;
create table person(
    id char(5) primary key,
    personal varchar(20),
    family varchar(30)
);
insert into person (id, personal, family) values
    ('dyer', 'William', 'Dyer'),
    ('pb', 'Frank', 'Pabodie');

drop table if exists site cascade;
create table site(
    name char(5) primary key,
    lat numeric,
    lon numeric
);
insert into site(name, lat, lon) values
    ('DR-1', -49.85, -128.57),
    ('DR-3', -47.15, -126.72);


drop table if exists visited cascade;
create table visited(
    id serial primary key,
    site char(5) references site(name),
    moment date
);
insert into visited(id, site, moment) values
    (619, 'DR-1', '1927-02-08'),
    (622, 'DR-1', '1927-02-10');

drop table if exists survey cascade;
create table survey(
    taken serial references visited(id),
    person char(5) references person(id),
    quant varchar(15),
    reading numeric
);
insert into survey(taken, person, quant, reading) values 
    (619, 'dyer', 'rad', 9.82),
    (619, 'dyer', 'sal', 0.13),
    (622, 'dyer', 'rad', 7.8),
    (622, 'pb', 'rad', 2.2),
    (622, 'pb', 'temp', 25.2);

select s.name, count(v.id)  from visited as v
right join site as s on s.name = v.site
group by s.name;

select s.name, count(v.id)  from visited as v
right join site as s on s.name = v.site
group by s.name
having count(v.id) = 0;

select s.quant from survey as s
group by s.quant;

select p.id, count(s.taken) from person as p
inner join survey as s on s.person = p.id
group by p.id
having count(s.taken) > 2;

select * from person as p
where lower(p.family) like lower('%D%Y%R%');

create or replace function list_visit(name char(5))
returns setof visited
language sql
as $$
    select * from visited as v
    where v.site ~ name;
$$;

select * from list_visit('DR-1');

select count(s.quant) from survey as s
where s.quant is null;

create or replace function avg_location(m1 date, m2 date)
returns table (lat numeric, lon numeric)
language sql
as $$
    select avg(s.lat), avg(s.lon) from site as s
    inner join visited as v on v.site = s.name
    where v.moment >= m1 and v.moment < m2;
$$;

select * from avg_location('1927-02-08'::date, '1927-02-09'::date);

select p.id, p.personal || ' ' || p.family as "Name", count(s.taken) from person as p
left join survey as s on s.person = p.id
group by p.id;

select p.id, p.personal || ' ' || p.family as "Name", count(s.taken) from person as p
left join survey as s on s.person = p.id
where s.quant = 'temp' and s.reading >= 10 and s.reading < 30
group by p.id
order by count(s.taken) desc
limit 1;
