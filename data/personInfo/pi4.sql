CREATE TABLE personInfo(id int, first_name string, last_name string, Gender string, age int, State string)
select first_name, last_name, age, State from personInfo where age > 30 and State ='NJ'
