select
sum(extendedprice*discount) as revenue
from
lineitem
where
shipdate >= DATE('1994-01-01')
and shipdate < date ('1995-01-01')
and discount >0.06 - 0.01 and discount<0.06+ 0.01 and quantity < 24;

