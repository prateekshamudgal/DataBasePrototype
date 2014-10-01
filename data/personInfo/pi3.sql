CREATE TABLE personInfo(id int, first_name string, last_name string, Gender string, age int, State string)
select p1.id, p2.id, p1.age,p2.age from personInfo p1, personInfo p2 where p2.age-p1.age>10
