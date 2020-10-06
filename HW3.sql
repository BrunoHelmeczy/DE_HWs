use birdstrikes;

-- Exercise 1: Repeat the above w SPEED: Low speed < 100; else High SPeed - use IF ELSE
SELECT aircraft, 
		airline, 
		speed, 
		if (speed < 100, 'Low Speed', 'High Speed') as speed_cat
from birdstrikes;

-- eXERCISE 2: How many distinct aircraft-s are there in the database?
SELECT COUNT(DISTINCT(AIRLINE)) FROM BIRDSTRIKES;
-- Answer: 74

-- eXERCISE 3: What is the lowest speed of aircrafts starting w H
select distinct(aircraft), min(speed) from birdstrikes where aircraft like 'H%';
-- Answer: Min_Speed = 9 - Helicopter

-- Exercise 4: Flight_phase w least incidents?
select 	phase_of_flight, 
		count(phase_of_flight) from birdstrikes 
        group by phase_of_flight 
        order by count(phase_of_flight) asc limit 1;
-- A: Taxi - 2

-- Exercise 5: What is the rounded highest average cost by phase_of_flight ? 
select 	phase_of_flight, 
		count(phase_of_flight), 
		round(avg(cost)) from birdstrikes group by phase_of_flight ORDER BY ROUND(AVG(COST)) DESC limit 1;
-- Answer: Climb - 54673

-- Exer. 6: Higest AVG speed of states w names less the 5 characters
select 	state, 
		avg(speed) from birdstrikes 
        where 	length(state) < 6 and 
				state <>'' 
		group by state 
        order by avg(speed) desc limit 1;
-- Answer: 2862.5 - Iowa 
