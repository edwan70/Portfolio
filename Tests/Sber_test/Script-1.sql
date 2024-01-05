/*
Задача 1 

Формулировка:

Вывести все строки из таблицы контактов. А для тех ID, 
которые встречаются несколько раз, в отдельном столбце 
вывести через запятую в порядке убывания все значения key. 

Решение:
*/

WITH a AS (
  SELECT
    id,
    COUNT(*) AS id_count,
    STRING_AGG(key::text, ', ' ORDER BY key DESC) AS other_keys
  FROM
    contacts
  GROUP BY
    id
  HAVING
    COUNT(*) > 1
)
SELECT
  t1.*,
  COALESCE(a.other_keys, '') AS other_keys
FROM
  contacts t1
LEFT JOIN
  a ON t1.id = a.id
ORDER BY
  t1.id ASC, t1.key DESC;

/*

Задача 2

Формулировка: 

Для каждого клиента вывести информацию - суммарная стоимость 
его договоров выше средний для его пола или ниже. 

Решение:
*/

with A as (
  select
	gender, l.client_id, sum(loan_amount) as loan_sum
  from loans_table l
  join clients_table c
  on l.client_id = c.client_id
  group by 1, 2
),
b as (
  select gender, avg(loan_sum) as avg_loan_sum
  from a
  group by 1
)
 select *, loan_sum > avg_loan_sum as more_than_avg
 from A
 join b 
 on A.gender = b.gender
;

/*
Задача 3

Формулировка: 

Собрать информацию в виде сводной таблицы с разбивкой по полу - 
сколько первых договоров было заключено в 2020 году, 
сколько вторых и так далее. 

Решение:
*/

with a as (
  select
	*, 
    row_number() over(partition by c.client_id order by loan_date) as rn
  from loans_table l
  join clients_table c
  on l.client_id = c.client_id
)
select 
	gender, 
    count(case when rn = 1 then 1 end) as "1", 
    count(case when rn = 2 then 1 end) as "2", 
    count(case when rn = 3 then 1 end) as "3"
 from a 
 where extract(year from loan_date) = 2020
 group by gender
;

/*

Задача 4

Формулировка: 

Выведите для каждого клиента сумму каждого его договора 
и сумму договоров накопительным итогом. 
Перед каждым новым договором, кроме первого, 
добавьте строку с датой, которая предшествует дате договора 
и с суммой NULL - это значит, что обязательства 
по предыдущему договору исполнены. 


Решение: 
*/

with a as (
    select
    c.client_id,
    loan_date,
    loan_amount,
    row_number() over(partition by c.client_id order by loan_date) as rn
  from loans_table l
  join clients_table c
  on l.client_id = c.client_id
 ), 
 b as (
   select
   	client_id,
   	loan_date - interval '1 day' as loan_date,
   	NULL::numeric as loan_amount,
   	rn - 0.5 as rn
   from a 
   where rn != 1
), 
c as (  
  select
  *
  from a 
  union all
  select *
  from b 
)
select
 	*,
    sum(loan_amount) over(partition by client_id order by rn) as loan_cumsum
from c 
order by client_id, rn
;
   

/*

Задача 5

Формулировка: 

Предложите какой-нибудь метод аналитики эффективности работы компании на основании имеющихся данных. Какие выводы вы могли бы сделать - приведите несколько вариантов. Какие дополнительные данные вам нужны были бы, чтобы сделать свой анализ более качественно? 

Решение: 

Предлагаю провести когортный анализ LTV в разрезе гендеров. Выбираем этот метод, потому что он лучше всего в динамике показывает жизнь клиента в нашем продукте и насколько хорошо он приносит прибыль компании. 

В качестве когорты берем год первого договора. Интервалы разбиваем по полгода. Смотрим также на гендер, т.к. в других заданиях это было важно - видимо, это какой-то существенный фактор. 

Примеры выводов: 

    1. Если каждый полгода затишье, значит надо придумать, как сделать наш продукт более рекуррентным 
    2. Если после 1-2 договора люди перестают покупать (когорта быстро сгорает), надо подумать над тем, как сделать наш продукт более рекуррентным
    3. Если будем видеть, что сегмент мужчин/женщин показывает лучше результат (более высокий чек) - надо понять, как поднять чек в отстающем сегменте
    4. Если когорта какого-то года показывает сильно больший LTV, нужно понять, откуда пришли эти люди и чем привлечение этой когорты отличается от других, почему другие покупают меньше

Что может еще понадобиться: 

    1. Базовое понимание продукта (адекватный чек, адекватная частота, почему так фокусируемся на гендерах)
    2. Отчет о привлечении (откуда приходили клиенты, какой CAC каждой когорты)


Код:
*/

 with a as (
   select
    l.client_id, 
    gender as gender, 
    to_char(first_value(loan_date) over(partition by l.client_id order by loan_date), 'YYYY') as cohort, 
    loan_date - first_value(loan_date) over(partition by l.client_id order by loan_date) as diff, 
    loan_amount
  from loans_table l
  join clients_table c
  on l.client_id = c.client_id
 )
 select
  	gender, 
    cohort, 
    sum(case when diff >= 0 and diff < 180 then loan_amount end) as "0", 
    sum(case when diff >= 180 and diff < 365 then loan_amount end) as "0.5", 
    sum(case when diff >= 365 and diff < 545 then loan_amount end) as "1"
 from a 
 group by 1, 2
 order by 1, 2
;


