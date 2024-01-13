-- Databricks notebook source
use retail_db

-- COMMAND ----------

Drop table if exists users

-- COMMAND ----------

CREATE TABLE users 
( user_id INT,
  user_first_name VARCHAR (30),
  user_last_name VARCHAR(30),
  user_email_id VARCHAR (50),
  user_gender VARCHAR (1) ,
  user_unique_id VARCHAR(15),
  user_phone_no VARCHAR(20), 
  user_dob DATE, 
  created_ts TIMESTAMP
)

-- COMMAND ----------

insert into users 
values
(1,"Giuseppe","Bode",'gbodeo@imgur.com',"M",'88833-8759',
'+86 (764) 443-1967','1973-05-31','2018-04-15 12:13:38'),
(2, 'Lexy','Gisbey','lgisbey1@mail.ru','F','262501-029',
'+86 (751) 160-3742' ,'2003-05-31','2020-12-29 06:44:09'),
(3, "Karel",'Claringbold', "kclaringbold2eyale.edu","F","391-33-2823",
"+62 (445) 471-2682" , "1985-11-28","2018-11-19 00:04:08"),
(4, 'Marv','Tanswill','mtanswill3@dedecms.com','F',"11195413-80",
"+62 (497) 736-68021", '1958-05-24',"2018-11-19 16:29:43"),
(5, "Gertie",'Espinoza', "gespinoza4@nationalgeographic.com", 'M', "1471-24-6869",
"+249 (687) 506-2960","1997-10-30","2020-01-25 21:31:10"),
(6, 'Saleem', 'Danneil',"isdanneil5@guardian.co.uk", "F","1192374-933",
"+63 (810) 321-03311"," 1992-03-08", "2020-11-07 19:01:14"),
(7,'Rickert',"O''Shiels",'roshiels6@wikispaces.com','M',
'749-27-47-52','+86 (184) 759-39331',"1972-11-01","2018-03-20 10:53:24"),
(8, 'Cybil', 'Lissimore',"clissimore7@pinterest.com", 'M',
'461-75-4198','+54 (613) 939-6976', '1978-03-03','2019-12-09 14:08:30'),
(9, 'Melita','Rimington','mrimineton8@mozilla.org',"F",'892-36-676-2',
"+48 (322) 829-86381", '1995-12-15',"2018-04-03 04:21:33"),
(10,'Benetta','Nana','branag@google.com', 'M','197-54-1646',
'+420 (934) 611-0020','1971-12-07','2018-10-17 21:02:51'),
(11, 'Gregorius', 'Gullane','ggullanea@prnewswire.com',"F",'232-55-52-58',
"+62 (780) 859-1578" ," 1973-09-18","2020-01-14 23:38:53"),
(12, 'Una', 'Glavzer', 'uglayzerb@pinterest.com','M',"898-84-336-6",
"+380 (840) 437-3981","1983-05-26",'2019-09-17 03:24:21'),
(13,'Jamie','Vosper','ivosperc@umich.edu','M','247-95-68-44',
"+81 (205) 723-1942",'1972-03-18','2020-07-23 16:39:33'),
(14, 'Calley',"Tilson",'ctilsond@issuu.com',"F","415-48-894-3",
"+229 (698) 777-4904","1987-06-12","2020-06-05 12:10:50"),
(15, 'Peadar','Gregorowicz','pgregorowicze@omniture.com',"M",'403-39-5-869',
"+7 (267) 853-3262","1996-09-21",'2018-05-29 23:51:31'),
(16, 'Jeanie','Webling', 'jweblingfebooking.com', 'F','399-83-05-03',
'+351 (684) 413-0550','1994-12-27','2018-02-09 01:31:11'),
(17, 'Yankee','Jelf', 'yjelfg@wufoo.com',"F", '607-99-04111',
'+1 (864) 112-74321','1988-11-13','2019-09-16 16:09:12'),
(18,'Blair','Aumerle','baumer leh@toplist.cz','F','430-01-578-5',
'+7 (393) 232-1860','1979-11-09','2018-10-28 19:25:35'),
(19,'Pavlov', 'Steljes','tosteliesi@macromedia.com', 'F','571-09-6181',
'+598 (877) 881-3236', '1991-06-24','2020-09-18 05:34:31'),
(20, 'Darn','Hadeke',"dhadekej@last.fm", 'M','478-32-02-87'
,'+370 (347) 110-4270','1984-09-04','2018-02-10 12:56:00'),
(21,'Wendell','Spanton','wspantonk@de.vu','F', null 
,'+84 (301) 762-1316','1978-07-24','2018-01-30 01:20:11'),
(22,'Carlo','Yearby','yearbyl@comcast.net','F', null,
'+55 (288) 623-4067','1974-11-11','2018-06-24 03:18:40'),
(23, 'Sheila',"Evitts", "sevittsm@webmd.com", null,'830-40-5287', 
null,"1977-03-01",'2020-07-20 09:59:41'),
(24,'Sianna',"Lowdham","slowdhamn@stanford.edu", null,'778-0845',
null,"1985-12-23", '2018-06-29 02:42:49'),
(25,"Phylys",'Aslie','paslieo@qg.com', 'M', '368-44-4478',
'+86 (765) 152-8654','1984-03-22','2019-10-01 01:34:28')

-- COMMAND ----------

select * from users

-- COMMAND ----------

select date_format(created_ts,"yyyy"),count(*) as created_year from users
group by 1

order by 1

-- COMMAND ----------

select date_format(current_date,'yyyy')

-- COMMAND ----------

select user_id,user_dob,user_email_id,date_format(user_dob,"EEEE") from users
where date_format(user_dob,"MM")=05


-- COMMAND ----------

select date_format(current_date,"MM")

-- COMMAND ----------

select * from users

-- COMMAND ----------

select user_id,user_name,user_email_id,created_ts,year_created from(
  select user_id,concat(user_first_name," ",user_last_name) user_name,
        user_email_id,created_ts,date_format(created_ts,"yyyy") as year_created
  from users) q
  where year_created=2019

-- COMMAND ----------

select 
case
  when user_gender="F" then "Female"
  when user_gender="M" then "Male"
  else "Not Specified"
end user_gender,
count(*) gender_count
 from users
 group by 1

-- COMMAND ----------


  select user_id,user_unique_id,right(replace(user_unique_id,"-",''),4) as new_id
  from users

-- COMMAND ----------

select replace(split(user_phone_no," ")[0],"+",""),count(*) from users
group by 1
order by 1

-- COMMAND ----------

use itversity_retail_db

-- COMMAND ----------

select * from orders

-- COMMAND ----------

select 
case
      when date_format(order_date,"EEE") in ("Sun","Sat") then "Weekend days"
      when date_format(order_date,"EEE") not in ("Sun","Sat") then "Week days"
      end, count(order_id) from orders
      where date_format(order_date,"yyyy-MM")="2014-01"
group by 1
