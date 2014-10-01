CREATE TABLE personInfo(id int, first_name string, last_name 
string, Gender string, age int, State string)
CREATE TABLE person(id int, first_name string, last_name 
string, Gender string, age int, State string)
SELECT personInfo.id, personInfo.first_name, personInfo.last_name, personInfo.age from personInfo, person where personInfo.age = person.age