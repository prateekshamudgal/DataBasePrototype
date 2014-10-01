CREATE TABLE personInfo(id int, first_name string, last_name string, Gender string, age int, State string)
SELECT COUNT(id), State from personInfo Group by State
