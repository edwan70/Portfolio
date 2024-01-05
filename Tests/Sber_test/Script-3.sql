
/*
Задача 1 (Здесь по сути два задания). 

Таблица представлена следующими полями:
1) key
2) id
3) phone
4) mail

Тестовые данные:

1; 12345;89997776655;test@mail.ru
2; 54321;87778885566;two@mail.ru
3; 98765;87776664577;three@mail.ru
4; 66678;87778885566;four@mail.ru
5; 34567;84547895566;four@mail.ru
6; 34567;89087545678;five@mail.ru
*/
drop table if exists t1 ;
drop table if exists contacts1;
CREATE TABLE if not exists contacts1 (
key int,
id int,
phone varchar,
mail varchar);

insert into contacts1 values 
(1, 12345, 89997776655, 'test@mail.ru'),
(2, 54321, 87778885566, 'two@mail.ru'),
(3, 98765, 87776664577, 'three@mail.ru'),
(4, 66678, 87778885566, 'four@mail.ru'),
(5, 34567, 84547895566, 'four@mail.ru'),
(6, 34567, 89087545678, 'five@mail.ru')
;
/*

На основании заданного поля (это может быть id, phone, mail) получить все "связанные данные"

Например:
если задать поиск по условию phone = 87778885566;

Результат должен быть следующим:

2; 54321; 87778885566; two@mail.ru
4; 66678; 87778885566; four@mail.ru
5; 34567; 84547895566; four@mail.ru
6; 34567; 89087545678; five@mail.ru

Задание следует отдельно выполнить на python и на SQL (по сути два задания). 
Диалект SQL можно использовать любой.
*/


select *
from contacts1 c1  
where id = 66678 
or phone = '87778885566' 
or mail='four@mail.ru'
;

with a as (
select *
from contacts1 c1  
where id = 66678
or phone = '87778885566' 
or mail ='four@mail.ru'
)
SELECT c1.*
FROM contacts1 c1
left JOIN a ON c1.id = a.id
where c1.id = a.id
or c1.phone = a.phone
or c1.mail = a.mail
;
/*Задача 2.
    1. Таблица CLIENTS_TABLE1 (по клиентам)
идентификатор клиента
ФИО клиента
ДР клиента
пол клиента
остальные параметры

CLIENT_ID
CLIENT_NAME
BIRTHDAY
GENDER

    2. Таблица LOANS_TABLE1 (по договорам)
номер договора
идентификатор клиента
дата договора
сумма по договору
остальные параметры
LOAN_ID
CLIENT_ID
LOAN_DATE
LOAN_AMOUNT


Каждый клиент может обращаться в компанию несколько раз, 
соответственно в базе может храниться информация 
по нескольким договорам на одного клиента.
Договор, оформленный клиентом у нас впервые, 
будем называть первым договором; договор, 
оформленный после – вторым; далее – третьим; и так далее.
Необходимо написать SQL запрос к базе для представления 
его результатов в сводной таблице вида:

Количество 1 договоров, оформленных в 2020
Количество 2 договоров, оформленных в 2020
Количество 3 договоров, оформленных в 2020
Количество 4 договоров, оформленных в 2020

Мужчины


Женщины


Диалект SQL можно использовать любой.

Тестовые данные:
PostgreSQL*/

drop table if exists LOANS_TABLE1;

CREATE TABLE if not exists LOANS_TABLE1 (
LOAN_ID int,
CLIENT_ID int,
LOAN_DATE date,
LOAN_AMOUNT float);

drop table if exists CLIENTS_TABLE1;

CREATE TABLE if not exists CLIENTS_TABLE1 (
CLIENT_ID int,
CLIENT_NAME VARCHAR(20),
BIRTHDAY date,
GENDER VARCHAR(20));

INSERT INTO CLIENTS_TABLE1
VALUES
(1, 'bob', '20200115', 'male'),
(2, 'rocky', '20200215', 'female'),
(3, 'like', '20200215', 'female'),
(4, 'ricky', '20200215', 'male');

INSERT INTO LOANS_TABLE1
VALUES
(1, 1, '20200115', 10000),
(2, 2, '20200215', 20000),
(3, 3, '20200315', 30000),
(4, 4, '20200415', 40000),
(5, 1, '20200116', 15000),
(6, 2, '20200315', 35000),
(7, 3, '20200315', 5000),
(8, 1, '20200115', 1500),
(9, 2, '20200115', 500),
(10, 1, '20200115', 1500);


--Решение:


with a as (
  select
	*, 
    row_number() over(partition by c.client_id order by loan_date) as rn
  from loans_table1 l
  join clients_table1 c
  on l.client_id = c.client_id
)
select 
	case when gender = 'male' then 'Мужчины' else 'Женщины' end as "Пол", 
    count(case when rn = 1 then 1 end) as "Количество 1 договоров, 2020", 
    count(case when rn = 2 then 1 end) as "Количество 2 договоров, 2020", 
    count(case when rn = 3 then 1 end) as "Количество 3 договоров, 2020",
    count(case when rn = 4 then 1 end) as "Количество 4 договоров, 2020"
 from a 
 where extract(year from loan_date) = 2020
 group by gender
;

