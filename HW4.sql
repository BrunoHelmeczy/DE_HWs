use classicmodels;

select o2.ordernumber, priceeach, quantityordered, productname, productline, city, country, orderdate
from customers c
inner join orders o1
on c.customernumber = o1.customernumber
inner join orderdetails o2
on o1.ordernumber = o2.ordernumber
inner join products p
on o2.productCode = p.productCode;