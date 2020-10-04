-- Exercise 1
-- What state figures in the 145th line of our database?
use birdstrikes;
select state from birdstrikes where id = 146;
-- A: Tennessee

-- 2) What is the flight date of the latest birdstrike in the database?
select * from birdstrikes order by flight_date desc limit 5;
-- A: 2000, 18th April - 3 birdstrikes encountered

-- 3) What was the cost of the 50th most expensive damage?
select * from birdstrikes order by cost desc limit 49,1;
-- A: Cost = 6014 (units)

-- 4) What state figures in the 2nd Record, if filtered out all records with no state & bird-size specified?
use birdstrikes;
select * from birdstrikes where state <> '' and bird_size <> '' limit 1,1;
-- A: Colorado

-- 5) How many days passed betw current date & flights happening in Wk52 for incidents from COLORADO?
SELECT flight_date, 
weekofyear(flight_date),
date(now()), datediff(date(now()),flight_date) FROM birdstrikes where weekofyear(flight_date) = 52
order by datediff(date(now()),flight_date) desc limit 1;
-- A: 7582 days i.e. ca. 20yrs, 9 Months, 12 Days (365day/yr & 30day/Month)


