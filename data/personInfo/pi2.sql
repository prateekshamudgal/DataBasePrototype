CREATE TABLE personInfo(id int, first_name string, last_name string, Gender string, age int, State string)
select p1.first_name,p1.last_name, p2.first_name,p2.last_name, p1.age-p2.age  from personInfo p1, personInfo p2 where p1.first_name=p2.first_name
