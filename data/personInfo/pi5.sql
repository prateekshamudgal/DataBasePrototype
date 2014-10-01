CREATE TABLE personInfo(id int, first_name string, last_name string, Gender string, age int, State string)
SELECT AVG(age) from personInfo where Gender= 'Male' and age>35
