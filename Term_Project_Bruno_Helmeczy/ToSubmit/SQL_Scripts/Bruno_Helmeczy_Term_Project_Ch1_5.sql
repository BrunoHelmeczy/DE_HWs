
-- Ch1) Loading the Dataset
CREATE SCHEMA HW1_NAB_Dataset;
use HW1_NAB_Dataset;

drop table if exists fpickup_NAB_1718;
CREATE TABLE fpickup_NAB_1718
(Prim_key varchar(32) NOT NULL,
Date_of_Res DATE NOT NULL,
Date_of_Stay DATE NOT NULL,
Days_Before_Arrival integer NOT NULL,
RMLs CHAR(3) NOT NULL,
OTB_Rooms Integer,
OTB_Rev float,
Interval_Length integer,
Pickup_Rooms Integer,
Pickup_Rev 	float,
NAB_Rates Integer,
NAB_PricePoint Integer,
NAB_vs_Comp_ABS INTEGER,
PRIMARY KEY(Prim_key));

SHOW VARIABLES LIKE "secure_file_priv";
LOAD DATA INFILE 'c:/ProgramData/MySQL/MySQL Server 8.0/Uploads/fPickup_NAB_1718.txt' 
INTO TABLE fPickup_NAB_1718 
FIELDS TERMINATED BY '	' 
LINES TERMINATED BY '\r\n' 
IGNORE 1 LINES 
	(Prim_key, Date_of_Res, Date_of_Stay, Days_before_Arrival, RMLs, OTB_Rooms, OTB_Rev, Interval_Length, Pickup_Rooms, Pickup_Rev,
@NAB_Rates, @NAB_PricePoint, @NAB_vs_Comp_ABS)
set 	NAB_Rates = nullif(@NAB_Rates, '(blank)'),
		NAB_PricePoint = nullif(@NAB_PricePoint, '(blank)'),
		NAB_vs_Comp_ABS = nullif(@NAB_vs_Comp_ABS, '(blank)');


Drop TABLE if exists dSegmentation;
CREATE TABLE dSegmentation 
(RMLs Varchar(4) NOT NULL,
Bucket_Name VARCHAR(25) NOT NULL,
Bucket_ID VarCHAR(5) NOT NULL,
Sub_Segment_A VARCHAR(32),
Sub_Segment_B VARCHAR(32),
Segm_Detailed VARCHAR(32),
PRIMARY KEY(RMLs));

SHOW VARIABLES LIKE "secure_file_priv";
LOAD DATA INFILE 'c:/ProgramData/MySQL/MySQL Server 8.0/Uploads/dSegmentation.txt' 
INTO TABLE dSegmentation 
FIELDS TERMINATED BY '	' 
LINES TERMINATED BY '\r\n' 
IGNORE 1 LINES 
	(RMLs, Bucket_Name, Bucket_ID, Sub_Segment_A, Sub_Segment_B, Segm_Detailed);

CREATE TABLE dCalendar_DOS 
(Dates_DOS date NOT NULL,
Season VARCHAR(10),
Major_Events VARCHAR(16),
Date_of_Week CHAR(3),
DoW_Nr Integer,
WD_WE Char(2),
Months VARCHAR(10),
PRIMARY KEY(Dates_DOS));

SHOW VARIABLES LIKE "secure_file_priv";
LOAD DATA INFILE 'c:/ProgramData/MySQL/MySQL Server 8.0/Uploads/dCalendar_DOS.txt' 
INTO TABLE dCalendar_DOS 
FIELDS TERMINATED BY '	' 
LINES TERMINATED BY '\r\n' 
IGNORE 1 LINES 
(Dates_DOS, Season, Major_Events, Date_of_Week, DoW_Nr, WD_WE, Months);

DROP TABLE IF EXISTS DDBA;
CREATE TABLE dDBA 
(dDBA INTEGER NOT NULL,
Wk_BA INTEGER NOT NULL,
Bk_Wd VARCHAR(16) NOT NULL,
PRIMARY KEY(dDBA));

SHOW VARIABLES LIKE "secure_file_priv";
LOAD DATA INFILE 'c:/ProgramData/MySQL/MySQL Server 8.0/Uploads/dDBA.txt' 
INTO TABLE dDBA 
FIELDS TERMINATED BY '	' 
LINES TERMINATED BY '\r\n' 
IGNORE 1 LINES 
(dDBA, Wk_BA, Bk_WD);

CREATE TABLE dCalendar_DOR 
(Dates_DOR date NOT NULL,
PRIMARY KEY(Dates_DOR));

SHOW VARIABLES LIKE "secure_file_priv";
LOAD DATA INFILE 'c:/ProgramData/MySQL/MySQL Server 8.0/Uploads/dCalendar_DOR.txt' 
INTO TABLE dCalendar_DOR 
FIELDS TERMINATED BY '	' 
LINES TERMINATED BY '\r\n' 
IGNORE 1 LINES 
(Dates_DOR);


-- Ch2) Dataset Denormalization * Consolidated Data Tables

-- Consolidated Data Tables: daily kpis (1) total/ (2) segment -> (3) total+bk_wd / (4) segment+bk_wd
		-- Needed for pre-aggregation for StDev calculations 
	
	-- (0) De-normalized Dataset
-- Daily KPIs VIEW: Occ%, ADR, RevPAR
drop view if exists DeNormalized_Dataset;
create view DeNormalized_Dataset as
select 	
	Date_of_Res as Res_Date, Date_of_Stay as Stay_Date,  
		-- ResDate = Time of Observation, Stay_Date = Object of Obervation
    dd.dDba as DBA_Start, Interval_Length, (dd.dDBA - Interval_Length) as DBA_End, Dow_NR, Wk_BA, dd.Bk_Wd,
		-- IntervalLength = Span of Observation in Days, DBA_Start = Start of Observation, DBA_End = End of Observation
        -- Bk_WD = Bin Categories to analyse booking behaviour in dinstinct booking windows
    WD_WE, Season, Major_events,
		-- Stay_Date Characterizations: Weekends/Weekdays; Season in Dubai Mkt.; Demand-influenceing Events on the StayDate
    Bucket_name as Segments, Sub_Segment_A, Sub_Segment_B, Segm_Detailed, f.RMLs, 
		-- Customer Characterizations: Segments = Transients/Groups/Corporate/Contracted Leisure
	OTB_Rooms, OTB_Rev, Pickup_Rooms, Pickup_Rev,
		-- Performance Metrics: OTB_Rooms/Rev = Nrs Sold for the stayDate, to the Segment upto this point()
        -- Performance Metrics: Pickup_Rooms/Rev = Nrs Sold for the stayDate, to the Segment in the current Interval_Length
	NAB_Rates, NAB_PricePoint, NAB_vs_Comp_ABS
		-- Pricing Information 4 Transients: PricePoint controls all Rates specific to an RML
			-- vs_COMP_ABS: NAB Pricepoint versus the average pricpoint of competitors
	from fpickup_nab_1718 f
			left join dcalendar_dos d 			on f.date_of_stay = d.dates_dos 
			left join ddba dd					on dd.dDBA = f.Days_Before_Arrival
            left join dsegmentation	s			on s.RMLs = f.RMLs;


		-- (1) daily kpis total
-- Daily KPIs VIEW: Occ%, ADR, RevPAR
drop view if exists 0_CDT_1_total_Daily_KPIs;
create view 0_CDT_1_Total_Daily_KPIs as
select 	Res_Date, year(Stay_date) as Years, month(Stay_date) as Months, Stay_date, Dow_nr, WD_WE, BK_WD, Season, DBA_end, 
		sum(otb_rooms+pickup_rooms) 													as Rooms_Sold,
        sum(Pickup_Rooms)																as Pickup_RNs,
        round(sum(otb_rev+pickup_rev),2) 												as Total_Revenue,
        (count(distinct(Stay_date))*465) 												as Availability,
		round((sum(otb_rooms+pickup_rooms)*100)/(count(distinct(Stay_date))*465),2) 	as Occ,
        round(sum(otb_rev+pickup_rev)/sum(otb_rooms+pickup_rooms),2) 					as ADR,
        round(sum(otb_rev+pickup_rev)/(count(distinct(Stay_date))*465),2) 				as Rev_PAR
	from denormalized_dataset
		group by Stay_Date, dba_end order by Stay_date desc, dba_end;
    
		-- (2) daily kpis segments -> REDUNDANT
drop view if exists 0_CDT_2_Segments_Daily_KPIs;
create view 0_CDT_2_Segments_Daily_KPIs as
select 	Res_Date, DBA_Start, DBA_End, Stay_date,  WD_WE, BK_WD, Season, Segments, year(Stay_date) as Years,
		sum(otb_rooms+pickup_rooms) 													as Rooms_Sold,
        sum(Pickup_Rooms)																as Pickup_RNs,
        round(sum(otb_rev+pickup_rev),2) 												as Total_Revenue,
        (count(distinct(Stay_date))*465) 												as Availability,
		round((sum(otb_rooms+pickup_rooms)*100)/(count(distinct(Stay_date))*465),2) 	as Occ,
        round(sum(otb_rev+pickup_rev)/sum(otb_rooms+pickup_rooms),2) 					as ADR,
        round(sum(otb_rev+pickup_rev)/(count(distinct(Stay_date))*465),2) 				as Rev_PAR
	from denormalized_dataset
		group by Segments, Stay_Date, Res_Date order by Stay_date, Res_Date;


        -- (3) daily kpis total by booking windows
drop view if exists 0_CDT_3_Total_Daily_Sales_bk_wd;
create view 0_CDT_3_Total_Daily_Sales_bk_wd as
select 	year(Stay_date) 	as Years, month(Stay_date) 	as Months, 
		Stay_date, 	DOW_nr, 	WD_WE, 		BK_WD, 		Season,
        sum(Pickup_Rooms)									as Pickup_RNs,
        round(sum(pickup_rev),2) 							as Total_Revenue
	from denormalized_dataset
		group by Stay_date, Bk_Wd order by Stay_date, Dba_Start;
    
        -- (4) daily kpis segments by booking windows
drop view if exists 0_CDT_4_Segment_Daily_KPIs_bk_wd; 		
create view 0_CDT_4_Segment_Daily_KPIs_bk_wd as
select 	Res_date, 
		year(Stay_date) 		as Years, 
		month(Stay_date) 	as Months, 
		Stay_date,  	WD_WE, 		BK_WD,	Season, 	Segments,
        sum(OTB_Rooms)									as OTB_RNs,
        round(sum(otb_rev),2) 							as OTB_Revenue,
        sum(Pickup_Rooms)								as Pickup_RNs,
        round(sum(pickup_rev),2) 						as Pickup_Revenue
	from denormalized_dataset
        group by res_date, Segments, Stay_date, BK_WD 
		order by res_date, Stay_date, Segments, bk_wd;

-- Ch3) Exploratory Data Analysis - By Weekdays, By Seasons, By Booking Windows

-- 2) -> KEEP -> #1
drop view if exists _1_EDA_01_Actual_KPIs_by_Segments;
		create view _1_EDA_01_Actual_KPIs_by_Segments as
select 	Segments,
	    round((sum(rooms_sold)/sum(availability))*100,2) 	as 'Occ_%',
        round(sum(total_revenue)/sum(rooms_sold),2) 		as ADR,
        round(sum(total_revenue)/sum(availability),2)		as RevPAR,
		round(avg(rooms_sold),2)							as Avg_Rooms_Sold,
		round(std(rooms_sold),2)							as StDev_Rooms_Sold
from 0_cdt_2_segments_daily_kpis f
left join dcalendar_dos c 					on c.Dates_dos = f.Stay_Date
	where f.dba_end = -1
	group by Segments order by Segments, c.dow_nr ;

-- 2)
drop view if exists _1_EDA_02_Actual_KPIs_by_Segment_YOY;
		create view _1_EDA_02_Actual_KPIs_by_Segment_YOY as		
select 	  f.Segments,
		round((sum(rooms_sold)/sum(availability))*100,2) 	as 'Occ_%', ly.Occ LY_OCC
        ,round((sum(rooms_sold)/sum(availability))*100,2) 	- ly.Occ					as Occ_vs_LY
        ,round(sum(total_revenue)/sum(rooms_sold),2) 									as ADR
        ,round(sum(total_revenue)/sum(rooms_sold),2) 		- ly.ADR					as ADR_vs_LY
        ,round(sum(total_revenue)/sum(availability),2)									as RevPAR
        ,round(sum(total_revenue)/sum(availability),2) 		- ly.RevPAR					as RevPAR_vs_LY
        ,round((sum(total_revenue)/sum(availability) - ly.RevPAR)*sum(availability),0) 	as Rev_YoY
from 0_cdt_2_segments_daily_kpis f
left join (select Segments,
	    round((sum(rooms_sold)/sum(availability))*100,2) 	as Occ,
        round(sum(total_revenue)/sum(rooms_sold),2) 		as ADR,
        round(sum(total_revenue)/sum(availability),2)		as RevPAR,
		round(avg(rooms_sold),2)							as Avg_RNs_Sold,
		round(std(rooms_sold),2)							as StDev_RNs_Sold,
        round(std(rooms_sold)/avg(rooms_sold),2)			as Rel_StDev,
        count(*) 											as Days 
from 0_cdt_2_segments_daily_kpis f
	where f.dba_end = -1 and f.years = 2017
	group by    Segments) ly 
on f.Segments = ly.Segments
	where f.dba_end = -1 and f.years = 2018
	group by Segments order by  Segments;

-- 3) KPIs by Weekdays 
drop view if exists _1_EDA_03_Actual_KPIs_by_Weekdays_4_TotalHotel;
		create view _1_EDA_03_Actual_KPIs_by_Weekdays_4_TotalHotel as
select 	Date_of_week, f.WD_We,
	    round((sum(rooms_sold)/sum(availability))*100,2) 	as 'Occ_%',
        round(sum(total_revenue)/sum(rooms_sold),2) 		as ADR,
        round(sum(total_revenue)/sum(availability),2)		as RevPAR,
		round(avg(rooms_sold),2)							as Avg_Rooms_Sold,
		round(std(rooms_sold),2)							as StDev_Rooms_Sold
from 0_cdt_1_total_daily_kpis f
	left join dcalendar_dos c 	on c.Dates_dos = f.Stay_date
		where f.dba_end = -1
		group by date_of_Week order by f.Dow_nr;


-- 4) 
drop view if exists _1_EDA_04_Actual_KPIs_by_Seasons;
		create view _1_EDA_04_Actual_KPIs_by_Seasons as		
select 	Season, 
	    round((sum(rooms_sold)/sum(availability))*100,2) 	as 'Occ_%',
        round(sum(total_revenue)/sum(availability),2)		as RevPAR,
        round(sum(total_revenue)/sum(rooms_sold),2) 		as ADR,
		round(avg(rooms_sold),2)							as Avg_Rooms_Sold,
		round(std(rooms_sold),2)							as StDev_Rooms_Sold,
        round(std(rooms_sold)/avg(rooms_sold),2)			as Rel_StDev,
        count(*) 											as Nr_Days 
from 0_cdt_1_total_daily_kpis f
	where f.dba_end = -1
	group by Season					order by Revpar DESC;
    

-- 5) Show KPI changes & Revenue Gain/Loss YOY by Season
drop view if exists _1_EDA_05_Actual_KPIs_by_Season_YOY;
		create view _1_EDA_05_Actual_KPIs_by_Season_YOY as		
select 	f.Season, 
	    round((sum(rooms_sold)/sum(availability))*100,2) 								as 'Occ_%'
        ,round((sum(rooms_sold)/sum(availability))*100,2) 	- ly.Occ					as Occ_vs_LY
        ,round(sum(total_revenue)/sum(rooms_sold),2) 									as ADR
        ,round(sum(total_revenue)/sum(rooms_sold),2) 		- ly.ADR					as ADR_vs_LY
        ,round(sum(total_revenue)/sum(availability),2)									as RevPAR
        ,round(sum(total_revenue)/sum(availability),2) 		- ly.RevPAR					as RevPAR_vs_LY
        ,round((sum(total_revenue)/sum(availability) - ly.RevPAR)*sum(availability),0) 	as Rev_YoY
from 0_cdt_1_total_daily_kpis f
	left join (
			select 	Season, 
					round((sum(rooms_sold)/sum(availability))*100,2) 	as Occ,
					round(sum(total_revenue)/sum(rooms_sold),2) 		as ADR,
					round(sum(total_revenue)/sum(availability),2)		as RevPAR,
					round(avg(rooms_sold),2)							as Avg_RNs,
					round(std(rooms_sold),2)							as StDev_RNs,
					round(std(rooms_sold)/avg(rooms_sold),2)			as Rel_StDev,
					count(*) 											as Days 
from 0_cdt_1_total_daily_kpis f
	where f.dba_end = -1 and years = 2017
	group by Season					order by Stay_date) 
    ly on ly.Season = f.Season
	where f.dba_end = -1 and years = 2018
	group by Season					order by Ly.RevPAR DESC;



-- 6) Okay -> Select by Seaason to see what happened
drop view if exists _1_EDA_06_Actual_KPIs_YOY_by_Seasons_n_Segments;
		create view _1_EDA_06_Actual_KPIs_YOY_by_Seasons_n_Segments as	
select 	f.Segments, d.Season, 
	    round((sum(rooms_sold)/sum(availability))*100,2) 								as Occ	
        ,round((sum(rooms_sold)/sum(availability))*100,2) 	- ly.Occ					as Occ_vs_LY
        ,round(sum(total_revenue)/sum(rooms_sold),2) 									as ADR	
        ,round(sum(total_revenue)/sum(rooms_sold),2) 		- ly.ADR					as ADR_vs_LY
        ,round(sum(total_revenue)/sum(availability),2)									as RevPAR		
		,round(sum(total_revenue)/sum(availability),2) 		- ly.RevPAR					as RevPAR_vs_LY
        ,round(((sum(total_revenue)/sum(availability) - ly.RevPAR)*sum(availability))/1000000,3) 	as Rev_M_YoY	
from 0_cdt_2_segments_daily_kpis f
	left join dcalendar_dos d 		on d.dates_dos = f.Stay_date
	left join (
		select 	f.Season, Segments,
				round((sum(rooms_sold)/sum(availability))*100,2) 	as Occ,
				round(sum(total_revenue)/sum(rooms_sold),2) 		as ADR,
				round(sum(total_revenue)/sum(availability),2)		as RevPAR,
				round(avg(rooms_sold),2)							as Avg_RNs,
				round(std(rooms_sold),2)							as StDev_RNs,
				round(std(rooms_sold)/avg(rooms_sold),2)			as Rel_StDev,
				count(*) 											as Days 
from 0_cdt_2_segments_daily_kpis f
	left join dcalendar_dos d 				on d.dates_dos = f.Stay_date
    where f.dba_end = -1 and years = 2017 
    group by Season, Segments	order by Stay_Date)
    ly	on ly.Season = d.season and ly.Segments = f.Segments
    where f.dba_end = -1 and years = 2018
	group by Season, Segments 	order by Segments, 	Stay_date;

-- 7) Totals by Booking Windows
    -- by booking windows
drop view if exists _1_EDA_07_Revenue_n_Rooms_Sold_by_BookingWindow;
		create view _1_EDA_07_Revenue_n_Rooms_Sold_by_BookingWindow as		
select 	Bk_wd,
		round(sum(total_revenue)/sum(Pickup_RNs),2) 		as ADR
        ,round(sum(total_revenue)/1000000,2)				as Rev_Picked_up_M
		,round(avg(Pickup_RNs),2)							as Avg_Rooms_Sold
		,round(std(Pickup_RNs),2)							as StDev_Rooms_Sold
        ,round(std(Pickup_RNs)/avg(Pickup_RNs),2)			as Rel_StDev
        ,count(*) 											as Days 
from 0_cdt_3_total_daily_sales_bk_wd f
	group by Bk_wd	order by  Bk_wd;

 -- 8)
drop view if exists _1_EDA_08_Revenue_n_Rooms_Sold_YOY_per_BkWd_by_Segments;
		create view _1_EDA_08_Revenue_n_Rooms_Sold_YOY_per_BkWd_by_Segments as		
select 	f.Segments, f.Bk_wd
		,round(sum(Pickup_Revenue)/sum(Pickup_RNs),2)	 			as ADR
        ,round(avg(Pickup_RNs),2)									as Avg_RNs
        ,round(sum(Pickup_revenue)/1000,2)							as Rev_Picked_up_K
        ,round(sum(Pickup_Revenue)/sum(Pickup_RNs),2) - ly.ADR  	as ADR_vs_LY        
		,round(avg(Pickup_RNs),2) 			- ly.Avg_RNs			as Avg_RNs_vs_LY
		,round(sum(Pickup_revenue)/1000,2)	- ly.Rev_Picked_up_K	as Rev_vs_LY_K
from 0_cdt_4_segment_daily_kpis_bk_wd f
left join (
		select 		Segments, Bk_wd,
					round(sum(Pickup_Revenue)/sum(Pickup_RNs),2) 			as ADR,
					round(avg(Pickup_RNs),2)							as Avg_RNs,
					round(std(Pickup_RNs),2)							as StDev_RNs,
					round(std(Pickup_RNs)/avg(Pickup_RNs),2)			as Rel_StDev,
                    round(sum(Pickup_revenue)/1000,2)					as Rev_Picked_up_K
					,count(*) 											as Days 
from 0_cdt_4_segment_daily_kpis_bk_wd f
	where years = 2017
    group by Segments, Bk_wd	order by Segments, Bk_wd)
	ly on ly.Segments = f.Segments and ly.bk_wd = f.bk_wd
	where years = 2018
    group by f.Segments, f.Bk_wd	order by Segments, Bk_wd;	

-- Ch4) Data Mart & Stored Procedures: 
-- 4.1) Monthly KPI Progression Month_2_Month

use hw1_nab_dataset;

-- Final figures Monthly
drop view if exists Final_Figures_Monthly_Segments;
create view Final_Figures_Monthly_Segments as
Select 	date_format(Stay_date, '%b %y') 	as Stay_Month, Segments,
		sum(rooms_sold) 																	as Final_Rooms_Sold, 
		round(((sum(rooms_sold))/(count(distinct(Stay_date))*465))*100,2) 				as Final_Occ_Percent,
        sum(total_revenue) 																	as Final_Revenue,
        round((sum(total_revenue))/(count(distinct(Stay_date))*465),2) 					as Final_RevPAR
	from 0_cdt_2_segments_daily_kpis 
    where dba_end = -1
    group by stay_month, Segments order by Stay_date;

	-- List of 1st report dates by Stay_month & Res_month plus Rooms_sold & Revenue -> GOOD
Drop view if exists Monthly_OTB_by_Res_Months_Start;
create view Monthly_OTB_by_Res_Months_Start as
select 	date_format(Stay_date, '%b %y') as Stay_Month, 
        min(date(k.res_date)) as Chosen_Res_Date,
	RANK() OVER (PARTITION BY date_format(Stay_date, '%b %y')
    ORDER BY min(date(k.res_date)) desc) Res_rank
	from 0_cdt_1_total_daily_kpis K
    group by date_format(Stay_date, '%b %y'), date_format(k.res_date, '%b %y')
    order by Stay_date, res_date desc;

call _Progression_Month_2_Month(1,'%');

DROP PROCEDURE IF EXISTS _Progression_Month_2_Month;
DELIMITER //
CREATE PROCEDURE _Progression_Month_2_Month( 
					IN _1st_Month integer, 
                    In SelectedSegment varchar(30))
Begin

select F.Stay_month, SelectedSegment
			,sum(Final_Occ_Percent)		as Final_Occ
            ,sum(Occ_Month_Start)		as OccMonthStart
            ,sum(Occ_1Month_Prev)		as OccMonthPrev
            ,sum(Occ_2Month_Prev) 		as Occ2MonthPrev
            ,sum(Final_RevPAR)			as Final__RevPAR
            ,sum(RevPAR_Month_Start) 	as RevPARMonthStart
            ,sum(RevPAR_1Month_Prev)	as RevPAR1MonthPrev
            ,sum(RevPAR_2Month_Prev)	as RevPAR2MonthPrev
	
-- 			,sum(Final_Occ_Percent)		as Final_Occ
--            ,sum(Occ_Month_Start)		as OccMonthStart
--             ,sum(Occ_1Month_Prev)		as OccMonthPrev
--             ,sum(Occ_2Month_Prev) 		as Occ2MonthPrev
--             ,sum(Final_RevPAR)			as Final__RevPAR
--             ,sum(RevPAR_Month_Start) 	as RevPARMonthStart
--             ,sum(RevPAR_1Month_Prev)	as RevPAR1MonthPrev
--             ,sum(RevPAR_2Month_Prev)	as RevPAR2MonthPrev
	
    
    from final_figures_monthly_segments F
left join (
		select stay_month, Chosen_res_date as Month_Start
			from monthly_otb_by_res_months_start 
				where res_rank = 1)
				M1 on f.stay_month = M1.stay_month
left join (
		select 	date_format(Stay_Date, '%b %y') as Stay_month, res_date, Segments,
				round(((sum(rooms_sold))/(count(distinct(Stay_Date))*465))*100,2) 	as Occ_Month_Start, 
                round((sum(total_revenue))/(count(distinct(Stay_Date))*465),2) 		as RevPAR_Month_Start
			from 0_cdt_2_segments_daily_kpis
            group by date_format(Stay_Date, '%b %y'), res_date, Segments) 
				L1 on L1.stay_month = m1.stay_month and l1.res_date = Month_Start and L1.Segments= F.Segments
left join (
		select stay_month, Chosen_res_date as Prev_1Month
			from monthly_otb_by_res_months_start 
				where res_rank = 2)
				M2 on f.stay_month = M2.stay_month
left join (
		select 	date_format(Stay_Date, '%b %y') as Stay_month, res_date, Segments,
				round(((sum(rooms_sold))/(count(distinct(Stay_Date))*465))*100,2) as Occ_1Month_Prev, 
                round((sum(total_revenue))/(count(distinct(Stay_Date))*465),2) as RevPAR_1Month_Prev
			from 0_cdt_2_segments_daily_kpis
            group by date_format(Stay_Date, '%b %y'), res_date, Segments) 
				L2 on f.stay_month = L2.stay_month and Prev_1Month = l2.res_date and F.Segments = l2.Segments 
left join (
		select stay_month, Chosen_res_date as _2Month_Prev
			from monthly_otb_by_res_months_start 
				where res_rank = 3)
				M3 on f.stay_month = M3.stay_month
left join (
		select 	date_format(Stay_Date, '%b %y') as Stay_month, res_date, Segments,
				round(((sum(rooms_sold))/(count(distinct(Stay_Date))*465))*100,2) as Occ_2Month_Prev, 
                round((sum(total_revenue))/(count(distinct(Stay_Date))*465),2) as RevPAR_2Month_Prev
			from 0_cdt_2_segments_daily_kpis
            group by date_format(Stay_Date, '%b %y'), res_date, Segments ) 
				L3 on f.stay_month = L3.stay_month  and _2Month_Prev = l3.res_date and F.Segments = l3.Segments 
where 
	month(Stay_date) >= _1st_Month and
	f.Segments like SelectedSegment
group by month(Stay_date), Year(Stay_date)
order by month(Stay_date) 
limit 6;
END //
DELIMITER ; 

	
-- 4.2) -- Forecast 14 Days ahead - Most Likely Scenario
-- Report Dates: On Some dates, data was not collected - so no data exists to call  

DROP PROCEDURE IF EXISTS Daily_2wk_Forecast;
DELIMITER //
CREATE PROCEDURE Daily_2wk_Forecast( IN Report_Date date )
BEGIN

with 
	_2wk_OTB_at_CURRENTDATE as (
		with Daily_OTBs as (
				select Stay_Date, res_date, Bk_Wd, DBA_start, WD_WE, Segments, Season, DBA_End,
								sum(otb_rooms+pickup_rooms)  													as Rooms_sold, 
                                round(sum(otb_rev+pickup_rev),2) 												as Total_revenue,
                                (count(distinct(Stay_date))*465) 												as Availability,
									if(DBA_start between 14 and 8, 7, if(DBA_start between  7 and 4, 3, -1)) 	as Next_Threshold,
									if(DBA_Start <= 7, 4, 7) 													as Current_BkWd_Length,
									if(DBA_start between 14 and 8, 3, -1) 										as _2nd_Threshold
							from denormalized_Dataset F
							where res_date = Report_Date or dba_end = -1
                            group by segments, Stay_date, Res_date)
		select 	Res_date, Stay_Date, otb.Season, otb.WD_WE, otb.Segments, 
				otb.Bk_Wd as BkWd_Now, Current_BkWd_Length,  
				DBA_Start, (DBA_Start - Next_threshold) as BkWd_Daysleft, 
				sum(Rooms_sold) 	as Rooms_OTB, d.Bk_Wd as BkWd_Next, dd.Bk_Wd as BkWd_2Next
			from Daily_OTBs OTB
		left join ddba D on d.dDBA = next_Threshold
		left join ddba dd on dd.ddba = _2nd_Threshold
			where res_date = Report_Date and Dba_Start <= 14
			group by Stay_date, Res_date, Segments
			order by res_date, DBA_Start),
	Current_Projections as (
		with Current_Data as (
				select 	
					Res_date, DBA_Start,  Stay_Date, 
					WD_WE, BK_WD,	Season, Segments,
					sum(Pickup_Rooms)					as Pickup_RNs,
					round(sum(pickup_rev),2) 			as Total_Revenue
				from denormalized_dataset
					where Res_date = Report_Date or Stay_Date <= '2018-03-01'
					group by Segments, Stay_Date, BK_WD 
					order by res_date, Stay_Date, Segments, bk_wd)

			select 	f.Season, f.WD_WE, Segments, f.Bk_wd, max(f.Dba_Start) AS DBA,
				round(sum(total_revenue)/sum(Pickup_RNs),2) 		as ADR,
				round(avg(Pickup_RNs),2)							as Avg_Rooms_Sold,
				round(std(Pickup_RNs),2)							as StDev_Rooms_Sold 
			from Current_Data f
				group by Season, WD_WE, Segments, Bk_wd		
                order by Segments, Season, WD_WE, Bk_wd)
select 		res_date as Reporting_Date , Stay_date as Forecasted_Date, DBA_Start, sum(Rooms_OTB) as Current_OTB,
                        
                        
		round(sum(Rooms_OTB) 
			+ round(if(DBA_Start between 14 and 8, 
							(sum(P1.Avg_Rooms_Sold) + sum(P2.Avg_Rooms_Sold)) 
							+ (sum(P3.Avg_Rooms_Sold)*(Bkwd_DaysLeft/Current_BkWd_Length)),
						if(DBA_Start between 7 and 4, 
								(sum(P1.Avg_Rooms_Sold)+(sum(P2.Avg_Rooms_Sold)*(Bkwd_DaysLeft/Current_BkWd_Length))),
						(sum(P1.Avg_Rooms_Sold)*(Bkwd_DaysLeft/Current_BkWd_Length)))),2),0) 
as FCST_Most_Likely,

		if((round(sum(Rooms_OTB) 
			+ round(if(DBA_Start between 14 and 8, 
							(sum(P1.Avg_Rooms_Sold) + sum(P2.Avg_Rooms_Sold)) 
							+ (sum(P3.Avg_Rooms_Sold)*(Bkwd_DaysLeft/Current_BkWd_Length)),
						if(DBA_Start between 7 and 4, 
								(sum(P1.Avg_Rooms_Sold)+(sum(P2.Avg_Rooms_Sold)*(Bkwd_DaysLeft/Current_BkWd_Length))),
						(sum(P1.Avg_Rooms_Sold)*(Bkwd_DaysLeft/Current_BkWd_Length)))),2),0)/93) <=2,2,
			ceiling(round(sum(Rooms_OTB) 
			+ round(if(DBA_Start between 14 and 8, 
							(sum(P1.Avg_Rooms_Sold) + sum(P2.Avg_Rooms_Sold)) 
							+ (sum(P3.Avg_Rooms_Sold)*(Bkwd_DaysLeft/Current_BkWd_Length)),
						if(DBA_Start between 7 and 4, 
								(sum(P1.Avg_Rooms_Sold)+(sum(P2.Avg_Rooms_Sold)*(Bkwd_DaysLeft/Current_BkWd_Length))),
						(sum(P1.Avg_Rooms_Sold)*(Bkwd_DaysLeft/Current_BkWd_Length)))),2),0)/93)
                        )
as Rec_FTEs
                    
                
    from _2wk_OTB_at_CURRENTDATE C

left join Current_Projections P1 on c.Segments = p1.segments and c.season = p1.season and c.wd_We = p1.WD_WE and 
        c.BkWd_NOW =  p1.BK_WD 
left join Current_Projections P2 on c.Segments = p2.segments and c.season = p2.season and c.wd_We = p2.WD_WE and 
        c.BkWd_Next =  p2.BK_WD        
left join Current_Projections P3 on c.Segments = p3.segments and c.season = p3.season and c.wd_We = p3.WD_WE and 
        c.BkWd_2Next =  p3.BK_WD
group by stay_date;
END //
DELIMITER ;

call Daily_2wk_Forecast('2018-05-14');

-- 4.3) Forecast 91 Days ahead - Most Likely, Pessimistic, Optimistic Scenarios
	-- Use Wildcard characters to filter by segment or see total hotel
DROP PROCEDURE IF EXISTS _3Month_Forecast;
DELIMITER //
CREATE PROCEDURE _3Month_Forecast( IN Report_Date date, In SelectedSegment varchar(30))
BEGIN

with 
	_OTB_at_CURRENTDATE as (
		with Daily_OTBs as (
				select Stay_Date, res_date, Bk_Wd, DBA_start, WD_WE, Segments, Season, DBA_End,
							sum(otb_rooms+pickup_rooms)  													as Rooms_sold, 
                            round(sum(otb_rev+pickup_rev),2) 												as Total_revenue,
                            (count(distinct(Stay_date))*465) 												as Availability,
							if(DBA_Start <= 7, 4, 
								if(DBA_Start between 8 and 28, 7,
									if(DBA_Start between 29 and 42, 14,
										if(DBA_Start between 43 and 63, 21, 28))))						
                                        as Current_BkWd_Length,
				
							if(DBA_start between 64 and 91, 63, 
								if(DBA_start between  43 and 63, 42,
									if(DBA_start between 29 and 42, 28,
										if(DBA_start between 22 and 28 , 21,
											if(DBA_start between  15 and 21 , 14,
												if(DBA_start between  8 and 14, 7,
													if(DBA_start between 4 and 7, 3,-1))))))) 	
										as Next_Threshold,
								
							if(DBA_start between 64 and 91, 42,
								if(DBA_start between  43 and 63, 28,
									if(DBA_start between  29 and 42, 21,
										if(DBA_start between  22 and 28 , 14,
											if(DBA_start between   15 and 21 , 7,
												if(DBA_start between   8 and 14, 3, -1))))))		
										as _2nd_Threshold,
                                                
							if(DBA_start between 64 and 91, 28,
								if(DBA_start between  43 and 63 , 21,
									if(DBA_start between  29 and 42 , 14,
										if(DBA_start between  22 and 28 , 7,
											if(DBA_start between  15 and 21 , 3, -1)))))			
										as _3rd_Threshold,

							if(DBA_start between 64 and 91 , 21,
								if(DBA_start between  43 and 63 , 14,
									if(DBA_start between  29 and 42 , 7,
										if(DBA_start between  22 and 28 , 3, -1))))						
                                        as _4th_Threshold,						
                        
							if(DBA_start between 64 and 91 , 14,
								if(DBA_start between  43 and 63 , 7,
									if(DBA_start between  29 and 42 , 3, -1)))						
										as _5th_Threshold,
                        
							if(DBA_start between 64 and 91 , 7,
								if(DBA_start between  43 and 63 , 3, -1))	
										as _6th_Threshold,
                                
							if(DBA_start between 64 and 91 , 3, -1)								
										as _7th_Threshold
								
                        from denormalized_Dataset F
						where res_date = Report_Date or dba_end = -1
                        group by segments, Stay_date, Res_date)
		select 	Res_date, Stay_Date, otb.Season, otb.WD_WE, otb.Segments, otb.Bk_Wd as BkWd_Now, Current_BkWd_Length,  
				DBA_Start, (DBA_Start - Next_threshold) as BkWd_Daysleft, 
				sum(Rooms_sold) 	as Rooms_OTB, 
                d1.Bk_Wd as BkWd_Next, 
                d2.Bk_Wd as BkWd_2Next,
                d3.Bk_Wd as BkWd_3Next,
                d4.Bk_Wd as BkWd_4Next,
                d5.Bk_Wd as BkWd_5Next,
                d6.Bk_Wd as BkWd_6Next,
                d7.Bk_Wd as BkWd_7Next
			
            from Daily_OTBs OTB
		left join ddba D1 on d1.dDBA 	= Next_Threshold
		left join ddba d2 on d2.ddba 	= _2nd_Threshold
        left join ddba d3 on d3.ddba 	= _3rd_Threshold
        left join ddba d4 on d4.ddba 	= _4th_Threshold
        left join ddba d5 on d5.ddba 	= _5th_Threshold
        left join ddba d6 on d6.ddba 	= _6th_Threshold
        left join ddba d7 on d7.ddba 	= _7th_Threshold
        
			where res_date = Report_Date and Dba_Start <= 91
			group by Stay_date, Res_date, Segments
			order by res_date, DBA_Start),
	Current_Projections as (
		with Current_Data as (
				select 	
					Res_date, DBA_Start,  Stay_Date, 
					WD_WE, BK_WD,	Season, Segments,
					sum(Pickup_Rooms)					as Pickup_RNs,
					round(sum(pickup_rev),2) 			as Total_Revenue
				from denormalized_dataset
					where Res_date = Report_Date or Stay_Date < Report_Date
					group by Segments, Stay_Date, BK_WD 
					order by res_date, Stay_Date, Segments, bk_wd)

			select 	f.Season, f.WD_WE, Segments, f.Bk_wd, max(f.Dba_Start) AS DBA,
				round(sum(total_revenue)/sum(Pickup_RNs),2) 		as ADR,
				round(avg(Pickup_RNs),2)							as Avg_Rooms_Sold,
				round(std(Pickup_RNs),2)							as StDev_Rooms_Sold 
			from Current_Data f
				group by Season, WD_WE, Segments, Bk_wd		
                order by Segments, Season, WD_WE, Bk_wd),
	Historical_Averages as (
		select 	Season, Segments, Wd_WE 
				,round(avg(rooms_sold) + (std(rooms_sold)/1.5),0)		as Strong_Demand_Above
                ,round(avg(rooms_sold) - (std(rooms_sold)/1.5),0)		as Soft_Demand_Below
		from 0_cdt_2_segments_daily_kpis
			where dba_end = -1 and stay_date <= Report_Date
		group by Season, Segments, Wd_WE )
select 		Res_date as FCST_Date, Stay_date, DBA_Start,

		round(sum(Rooms_OTB) 
			+ round(  if(DBA_Start between 64 and 91 , 
							(sum(P1.Avg_Rooms_Sold)+(sum(P1.StDev_Rooms_Sold)/2))
						+ 	(sum(P2.Avg_Rooms_Sold)+(sum(P2.StDev_Rooms_Sold)/2)) 
						+ 	(sum(P3.Avg_Rooms_Sold)+(sum(P3.StDev_Rooms_Sold)/2))
                        + 	(sum(P4.Avg_Rooms_Sold)+(sum(P4.StDev_Rooms_Sold)/2))
                        + 	(sum(P5.Avg_Rooms_Sold)+(sum(P5.StDev_Rooms_Sold)/2))
                        + 	(sum(P6.Avg_Rooms_Sold)+(sum(P6.StDev_Rooms_Sold)/2))
                        + 	(sum(P7.Avg_Rooms_Sold)+(sum(P7.StDev_Rooms_Sold)/2))
                        + (	(sum(P8.Avg_Rooms_Sold)+(sum(P8.StDev_Rooms_Sold)/2))*(Bkwd_DaysLeft/Current_BkWd_Length)),
            
					  if(DBA_Start between 43 and 63 , 
							(sum(P1.Avg_Rooms_Sold)+(sum(P1.StDev_Rooms_Sold)/2))
						+ 	(sum(P2.Avg_Rooms_Sold)+(sum(P2.StDev_Rooms_Sold)/2)) 
						+ 	(sum(P3.Avg_Rooms_Sold)+(sum(P3.StDev_Rooms_Sold)/2))
                        + 	(sum(P4.Avg_Rooms_Sold)+(sum(P4.StDev_Rooms_Sold)/2))
                        + 	(sum(P5.Avg_Rooms_Sold)+(sum(P5.StDev_Rooms_Sold)/2))
                        + 	(sum(P6.Avg_Rooms_Sold)+(sum(P6.StDev_Rooms_Sold)/2))
                        + (	(sum(P7.Avg_Rooms_Sold)+(sum(P7.StDev_Rooms_Sold)/2))*(Bkwd_DaysLeft/Current_BkWd_Length)),
                        
					  if(DBA_Start between 29 and 42 , 
							(sum(P1.Avg_Rooms_Sold)+(sum(P1.StDev_Rooms_Sold)/2))
						+ 	(sum(P2.Avg_Rooms_Sold)+(sum(P2.StDev_Rooms_Sold)/2)) 
						+ 	(sum(P3.Avg_Rooms_Sold)+(sum(P3.StDev_Rooms_Sold)/2))
                        + 	(sum(P4.Avg_Rooms_Sold)+(sum(P4.StDev_Rooms_Sold)/2))
                        + 	(sum(P5.Avg_Rooms_Sold)+(sum(P5.StDev_Rooms_Sold)/2))
                        + (	(sum(P6.Avg_Rooms_Sold)+(sum(P6.StDev_Rooms_Sold)/2))*(Bkwd_DaysLeft/Current_BkWd_Length)),
                        
					  if(DBA_Start between 22 and 28 , 
							(sum(P1.Avg_Rooms_Sold)+(sum(P1.StDev_Rooms_Sold)/2))
						+ 	(sum(P2.Avg_Rooms_Sold)+(sum(P2.StDev_Rooms_Sold)/2)) 
						+ 	(sum(P3.Avg_Rooms_Sold)+(sum(P3.StDev_Rooms_Sold)/2))
                        + 	(sum(P4.Avg_Rooms_Sold)+(sum(P4.StDev_Rooms_Sold)/2))
                        + (	(sum(P5.Avg_Rooms_Sold)+(sum(P5.StDev_Rooms_Sold)/2))*(Bkwd_DaysLeft/Current_BkWd_Length)),
                        
					  if(DBA_Start between 15 and 21 , 
							(sum(P1.Avg_Rooms_Sold)+(sum(P1.StDev_Rooms_Sold)/2))
						+ 	(sum(P2.Avg_Rooms_Sold)+(sum(P2.StDev_Rooms_Sold)/2)) 
						+ 	(sum(P3.Avg_Rooms_Sold)+(sum(P3.StDev_Rooms_Sold)/2))
                        + (	(sum(P4.Avg_Rooms_Sold)+(sum(P4.StDev_Rooms_Sold)/2))*(Bkwd_DaysLeft/Current_BkWd_Length)),
            
					  if(DBA_Start between 8 and 14 , 
							(sum(P1.Avg_Rooms_Sold)+(sum(P1.StDev_Rooms_Sold)/2))
						+ 	(sum(P2.Avg_Rooms_Sold)+(sum(P2.StDev_Rooms_Sold)/2)) 
						+ (	(sum(P3.Avg_Rooms_Sold)+(sum(P3.StDev_Rooms_Sold)/2))*(Bkwd_DaysLeft/Current_BkWd_Length)),
					  if(DBA_Start between 4 and 7 , 
						((sum(P1.Avg_Rooms_Sold)+(sum(P1.StDev_Rooms_Sold)/2)) 
						+ (sum(P2.Avg_Rooms_Sold)+(sum(P2.StDev_Rooms_Sold)/2))*(Bkwd_DaysLeft/Current_BkWd_Length)),
					  ((sum(P1.Avg_Rooms_Sold)+(sum(P1.StDev_Rooms_Sold)/2))*(Bkwd_DaysLeft/Current_BkWd_Length))))))))),2),0) 
			as FCST_Optimistic,
        
        round(sum(Rooms_OTB) 
			+ round(  if(DBA_Start between 64 and 91 , 
							sum(P1.Avg_Rooms_Sold)
						+ 	sum(P2.Avg_Rooms_Sold)
						+ 	sum(P3.Avg_Rooms_Sold)
                        + 	sum(P4.Avg_Rooms_Sold)
                        + 	sum(P5.Avg_Rooms_Sold)
                        + 	sum(P6.Avg_Rooms_Sold)
                        + 	sum(P7.Avg_Rooms_Sold)
                        + (	sum(P8.Avg_Rooms_Sold)*(Bkwd_DaysLeft/Current_BkWd_Length)),
            
					  if(DBA_Start between 43 and 63 , 
							sum(P1.Avg_Rooms_Sold)
						+ 	sum(P2.Avg_Rooms_Sold)
						+ 	sum(P3.Avg_Rooms_Sold)
                        + 	sum(P4.Avg_Rooms_Sold)
                        + 	sum(P5.Avg_Rooms_Sold)
                        + 	sum(P6.Avg_Rooms_Sold)
                        + (	sum(P7.Avg_Rooms_Sold)*(Bkwd_DaysLeft/Current_BkWd_Length)),
                        
					  if(DBA_Start between 29 and 42 , 
							sum(P1.Avg_Rooms_Sold) 
						+ 	sum(P2.Avg_Rooms_Sold)
						+ 	sum(P3.Avg_Rooms_Sold)
                        + 	sum(P4.Avg_Rooms_Sold)
                        + 	sum(P5.Avg_Rooms_Sold)
                        + (	sum(P6.Avg_Rooms_Sold)*(Bkwd_DaysLeft/Current_BkWd_Length)),
                        
					  if(DBA_Start between 22 and 28 , 
							sum(P1.Avg_Rooms_Sold)
						+ 	sum(P2.Avg_Rooms_Sold)
						+ 	sum(P3.Avg_Rooms_Sold)
                        + 	sum(P4.Avg_Rooms_Sold)
                        + (	sum(P5.Avg_Rooms_Sold)*(Bkwd_DaysLeft/Current_BkWd_Length)),
                        
					  if(DBA_Start between 15 and 21 , 
							sum(P1.Avg_Rooms_Sold) 
						+ 	sum(P2.Avg_Rooms_Sold)  
						+ 	sum(P3.Avg_Rooms_Sold) 
                        + (	sum(P4.Avg_Rooms_Sold)*(Bkwd_DaysLeft/Current_BkWd_Length)),
            
					  if(DBA_Start between 8 and 14 , 
							sum(P1.Avg_Rooms_Sold) 
						+ 	sum(P2.Avg_Rooms_Sold)
						+ (	sum(P3.Avg_Rooms_Sold)*(Bkwd_DaysLeft/Current_BkWd_Length)),
					  if(DBA_Start between 4 and 7 , 
							sum(P1.Avg_Rooms_Sold) 
						+ (	sum(P2.Avg_Rooms_Sold)*(Bkwd_DaysLeft/Current_BkWd_Length)),
					  (sum(P1.Avg_Rooms_Sold)*(Bkwd_DaysLeft/Current_BkWd_Length))))))))),2),0) 
			as FCST_Most_Likely,
     
		round(sum(Rooms_OTB) 
			+ round(  if(DBA_Start between 64 and 91 , 
						(sum(P1.Avg_Rooms_Sold) - (sum(P1.StDev_Rooms_Sold)/2))
						+ (sum(P2.Avg_Rooms_Sold) - (sum(P2.StDev_Rooms_Sold)/2)) 
						+ (sum(P3.Avg_Rooms_Sold) - (sum(P3.StDev_Rooms_Sold)/2))
                        + (sum(P4.Avg_Rooms_Sold) - (sum(P4.StDev_Rooms_Sold)/2))
                        + (sum(P5.Avg_Rooms_Sold) - (sum(P5.StDev_Rooms_Sold)/2))
                        + (sum(P6.Avg_Rooms_Sold) - (sum(P6.StDev_Rooms_Sold)/2))
                        + (sum(P7.Avg_Rooms_Sold) - (sum(P7.StDev_Rooms_Sold)/2))
                        + ((sum(P8.Avg_Rooms_Sold) - (sum(P8.StDev_Rooms_Sold)/2))*(Bkwd_DaysLeft/Current_BkWd_Length)),
            
					  if(DBA_Start between 43 and 63 , 
						(sum(P1.Avg_Rooms_Sold) - (sum(P1.StDev_Rooms_Sold)/2))
						+ (sum(P2.Avg_Rooms_Sold) - (sum(P2.StDev_Rooms_Sold)/2)) 
						+ (sum(P3.Avg_Rooms_Sold) - (sum(P3.StDev_Rooms_Sold)/2))
                        + (sum(P4.Avg_Rooms_Sold) - (sum(P4.StDev_Rooms_Sold)/2))
                        + (sum(P5.Avg_Rooms_Sold) - (sum(P5.StDev_Rooms_Sold)/2))
                        + (sum(P6.Avg_Rooms_Sold) - (sum(P6.StDev_Rooms_Sold)/2))
                        + ((sum(P7.Avg_Rooms_Sold) - (sum(P7.StDev_Rooms_Sold)/2))*(Bkwd_DaysLeft/Current_BkWd_Length)),
                        
					  if(DBA_Start between 29 and 42 , 
						(sum(P1.Avg_Rooms_Sold) - (sum(P1.StDev_Rooms_Sold)/2))
						+ (sum(P2.Avg_Rooms_Sold) - (sum(P2.StDev_Rooms_Sold)/2)) 
						+ (sum(P3.Avg_Rooms_Sold) - (sum(P3.StDev_Rooms_Sold)/2))
                        + (sum(P4.Avg_Rooms_Sold) - (sum(P4.StDev_Rooms_Sold)/2))
                        + (sum(P5.Avg_Rooms_Sold) - (sum(P5.StDev_Rooms_Sold)/2))
                        + ((sum(P6.Avg_Rooms_Sold) - (sum(P6.StDev_Rooms_Sold)/2))*(Bkwd_DaysLeft/Current_BkWd_Length)),
                        
					  if(DBA_Start between 22 and 28 , 
						(sum(P1.Avg_Rooms_Sold) - (sum(P1.StDev_Rooms_Sold)/2))
						+ (sum(P2.Avg_Rooms_Sold) - (sum(P2.StDev_Rooms_Sold)/2)) 
						+ (sum(P3.Avg_Rooms_Sold) - (sum(P3.StDev_Rooms_Sold)/2))
                        + (sum(P4.Avg_Rooms_Sold) - (sum(P4.StDev_Rooms_Sold)/2))
                        + ((sum(P5.Avg_Rooms_Sold) - (sum(P5.StDev_Rooms_Sold)/2))*(Bkwd_DaysLeft/Current_BkWd_Length)),
                        
					  if(DBA_Start between 15 and 21 , 
						(sum(P1.Avg_Rooms_Sold) - (sum(P1.StDev_Rooms_Sold)/2))
						+ (sum(P2.Avg_Rooms_Sold) - (sum(P2.StDev_Rooms_Sold)/2)) 
						+ (sum(P3.Avg_Rooms_Sold) - (sum(P3.StDev_Rooms_Sold)/2))
                        + ((sum(P4.Avg_Rooms_Sold) - (sum(P4.StDev_Rooms_Sold)/2))*(Bkwd_DaysLeft/Current_BkWd_Length)),
            
					  if(DBA_Start between 8 and 14 , 
						(sum(P1.Avg_Rooms_Sold) - (sum(P1.StDev_Rooms_Sold)/2))
						+ (sum(P2.Avg_Rooms_Sold) - (sum(P2.StDev_Rooms_Sold)/2)) 
						+ ((sum(P3.Avg_Rooms_Sold) - (sum(P3.StDev_Rooms_Sold)/2))*(Bkwd_DaysLeft/Current_BkWd_Length)),
					  if(DBA_Start between 4 and 7 , 
						((sum(P1.Avg_Rooms_Sold) - (sum(P1.StDev_Rooms_Sold)/2)) 
						+ (sum(P2.Avg_Rooms_Sold) - (sum(P2.StDev_Rooms_Sold)/2))*(Bkwd_DaysLeft/Current_BkWd_Length)),
					  ((sum(P1.Avg_Rooms_Sold) - (sum(P1.StDev_Rooms_Sold)/2))*(Bkwd_DaysLeft/Current_BkWd_Length))))))))),2),0) 
			as FCST_Pessimistic

from _OTB_at_CURRENTDATE C

left join Current_Projections P1 on c.Segments = p1.segments and c.season = p1.season and c.wd_We = p1.WD_WE and 
        c.BkWd_NOW =  p1.BK_WD 
left join Current_Projections P2 on c.Segments = p2.segments and c.season = p2.season and c.wd_We = p2.WD_WE and 
        c.BkWd_Next =  p2.BK_WD        
left join Current_Projections P3 on c.Segments = p3.segments and c.season = p3.season and c.wd_We = p3.WD_WE and 
        c.BkWd_2Next =  p3.BK_WD
left join Current_Projections P4 on c.Segments = p4.segments and c.season = p4.season and c.wd_We = p4.WD_WE and 
        c.BkWd_3Next =  p4.BK_WD
left join Current_Projections P5 on c.Segments = p5.segments and c.season = p5.season and c.wd_We = p5.WD_WE and 
        c.BkWd_4Next =  p5.BK_WD
left join Current_Projections P6 on c.Segments = p6.segments and c.season = p6.season and c.wd_We = p6.WD_WE and 
        c.BkWd_5Next =  p6.BK_WD
left join Current_Projections P7 on c.Segments = p7.segments and c.season = p7.season and c.wd_We = p7.WD_WE and 
        c.BkWd_6Next =  p7.BK_WD
left join Current_Projections P8 on c.Segments = p8.segments and c.season = p8.season and c.wd_We = p8.WD_WE and 
        c.BkWd_7Next =  p8.BK_WD
left join Historical_Averages HA on c.Segments = HA.Segments and c.Season = HA.Season and c.wd_we = HA.WD_WE

where  c.Segments like SelectedSegment
group by stay_date order by Stay_date;

END //
DELIMITER ;

-- Selected Segments: Choose from: 'Transients','Groups','Corporate','Contracted Leisure'
-- Use only Wildcards to Forecast ALL segments
call _3month_Forecast('2018-05-14','Trans%');

-- 4.4) Categorize Days 91 days ahead into ahead / behind schedule
	-- Use Wildcards to filter segments or Strong / Weak demand
DROP PROCEDURE IF EXISTS _3Month_Demand_Categ;
DELIMITER //
CREATE PROCEDURE _3Month_Demand_Categ( 
					IN Report_Date date, 
                    In SelectedSegment varchar(30),
                    in vs_Pace varchar(10))
BEGIN


with 
	_OTB_at_CURRENTDATE as (
		with Daily_OTBs as (
				select Stay_Date, res_date, Bk_Wd, DBA_start, WD_WE, Segments, Season, DBA_End,
							sum(otb_rooms+pickup_rooms)  													as Rooms_sold, 
                            round(sum(otb_rev+pickup_rev),2) 												as Total_revenue,
                            (count(distinct(Stay_date))*465) 												as Availability,
							if(DBA_Start <= 7, 4, 
								if(DBA_Start between 8 and 28, 7,
									if(DBA_Start between 29 and 42, 14,
										if(DBA_Start between 43 and 63, 21, 28))))						
                                        as Current_BkWd_Length,
				
							if(DBA_start between 64 and 91, 63, 
								if(DBA_start between  43 and 63, 42,
									if(DBA_start between 29 and 42, 28,
										if(DBA_start between 22 and 28 , 21,
											if(DBA_start between  15 and 21 , 14,
												if(DBA_start between  8 and 14, 7,
													if(DBA_start between 4 and 7, 3,-1))))))) 	
										as Next_Threshold,
								
							if(DBA_start between 64 and 91, 42,
								if(DBA_start between  43 and 63, 28,
									if(DBA_start between  29 and 42, 21,
										if(DBA_start between  22 and 28 , 14,
											if(DBA_start between   15 and 21 , 7,
												if(DBA_start between   8 and 14, 3, -1))))))		
										as _2nd_Threshold,
                                                
							if(DBA_start between 64 and 91, 28,
								if(DBA_start between  43 and 63 , 21,
									if(DBA_start between  29 and 42 , 14,
										if(DBA_start between  22 and 28 , 7,
											if(DBA_start between  15 and 21 , 3, -1)))))			
										as _3rd_Threshold,

							if(DBA_start between 64 and 91 , 21,
								if(DBA_start between  43 and 63 , 14,
									if(DBA_start between  29 and 42 , 7,
										if(DBA_start between  22 and 28 , 3, -1))))						
                                        as _4th_Threshold,						
                        
							if(DBA_start between 64 and 91 , 14,
								if(DBA_start between  43 and 63 , 7,
									if(DBA_start between  29 and 42 , 3, -1)))						
										as _5th_Threshold,
                        
							if(DBA_start between 64 and 91 , 7,
								if(DBA_start between  43 and 63 , 3, -1))	
										as _6th_Threshold,
                                
							if(DBA_start between 64 and 91 , 3, -1)								
										as _7th_Threshold
								
                        from denormalized_Dataset F
						where res_date = Report_Date or dba_end = -1
                        group by segments, Stay_date, Res_date)
		select 	Res_date, Stay_Date, otb.Season, otb.WD_WE, otb.Segments, otb.Bk_Wd as BkWd_Now, Current_BkWd_Length,  
				DBA_Start, (DBA_Start - Next_threshold) as BkWd_Daysleft, 
				sum(Rooms_sold) 	as Rooms_OTB, 
                d1.Bk_Wd as BkWd_Next, 
                d2.Bk_Wd as BkWd_2Next,
                d3.Bk_Wd as BkWd_3Next,
                d4.Bk_Wd as BkWd_4Next,
                d5.Bk_Wd as BkWd_5Next,
                d6.Bk_Wd as BkWd_6Next,
                d7.Bk_Wd as BkWd_7Next
			
            from Daily_OTBs OTB
		left join ddba D1 on d1.dDBA 	= Next_Threshold
		left join ddba d2 on d2.ddba 	= _2nd_Threshold
        left join ddba d3 on d3.ddba 	= _3rd_Threshold
        left join ddba d4 on d4.ddba 	= _4th_Threshold
        left join ddba d5 on d5.ddba 	= _5th_Threshold
        left join ddba d6 on d6.ddba 	= _6th_Threshold
        left join ddba d7 on d7.ddba 	= _7th_Threshold
        
			where res_date = Report_Date and Dba_Start <= 91
			group by Stay_date, Res_date, Segments
			order by res_date, DBA_Start),
	Current_Projections as (
		with Current_Data as (
				select 	
					Res_date, DBA_Start,  Stay_Date, 
					WD_WE, BK_WD,	Season, Segments,
					sum(Pickup_Rooms)					as Pickup_RNs,
					round(sum(pickup_rev),2) 			as Total_Revenue
				from denormalized_dataset
					where Res_date = Report_date or Stay_Date < Report_date
					group by Segments, Stay_Date, BK_WD 
					order by res_date, Stay_Date, Segments, bk_wd)

			select 	f.Season, f.WD_WE, Segments, f.Bk_wd, max(f.Dba_Start) AS DBA,
				round(sum(total_revenue)/sum(Pickup_RNs),2) 		as ADR,
				round(avg(Pickup_RNs),2)							as Avg_Rooms_Sold,
				round(std(Pickup_RNs),2)							as StDev_Rooms_Sold 
			from Current_Data f
				group by Season, WD_WE, Segments, Bk_wd		
                order by Segments, Season, WD_WE, Bk_wd),
	Historical_Averages as (
		select 	Season, Segments, Wd_WE 
				,round(avg(rooms_sold),0) 		as Avg_RNs_Sold
		from 0_cdt_2_segments_daily_kpis
			where dba_end = -1 and stay_date <= Report_date
		group by Season, Segments, Wd_WE )
select 		SelectedSegment, Res_date as FCST_Date, Stay_date, DBA_Start,
-- 			c.Segments, c.Season, c.WD_WE,   
	-- 		BKWD_Now, BkWD_Next, BkWd_2next,

round(sum(Rooms_OTB) 
			+ round(  if(DBA_Start between 64 and 91 , 
							sum(P1.Avg_Rooms_Sold)
						+ 	sum(P2.Avg_Rooms_Sold)
						+ 	sum(P3.Avg_Rooms_Sold)
                        + 	sum(P4.Avg_Rooms_Sold)
                        + 	sum(P5.Avg_Rooms_Sold)
                        + 	sum(P6.Avg_Rooms_Sold)
                        + 	sum(P7.Avg_Rooms_Sold)
                        + (	sum(P8.Avg_Rooms_Sold)*(Bkwd_DaysLeft/Current_BkWd_Length)),
            
					  if(DBA_Start between 43 and 63 , 
							sum(P1.Avg_Rooms_Sold)
						+ 	sum(P2.Avg_Rooms_Sold)
						+ 	sum(P3.Avg_Rooms_Sold)
                        + 	sum(P4.Avg_Rooms_Sold)
                        + 	sum(P5.Avg_Rooms_Sold)
                        + 	sum(P6.Avg_Rooms_Sold)
                        + (	sum(P7.Avg_Rooms_Sold)*(Bkwd_DaysLeft/Current_BkWd_Length)),
                        
					  if(DBA_Start between 29 and 42 , 
							sum(P1.Avg_Rooms_Sold) 
						+ 	sum(P2.Avg_Rooms_Sold)
						+ 	sum(P3.Avg_Rooms_Sold)
                        + 	sum(P4.Avg_Rooms_Sold)
                        + 	sum(P5.Avg_Rooms_Sold)
                        + (	sum(P6.Avg_Rooms_Sold)*(Bkwd_DaysLeft/Current_BkWd_Length)),
                        
					  if(DBA_Start between 22 and 28 , 
							sum(P1.Avg_Rooms_Sold)
						+ 	sum(P2.Avg_Rooms_Sold)
						+ 	sum(P3.Avg_Rooms_Sold)
                        + 	sum(P4.Avg_Rooms_Sold)
                        + (	sum(P5.Avg_Rooms_Sold)*(Bkwd_DaysLeft/Current_BkWd_Length)),
                        
					  if(DBA_Start between 15 and 21 , 
							sum(P1.Avg_Rooms_Sold) 
						+ 	sum(P2.Avg_Rooms_Sold)  
						+ 	sum(P3.Avg_Rooms_Sold) 
                        + (	sum(P4.Avg_Rooms_Sold)*(Bkwd_DaysLeft/Current_BkWd_Length)),
            
					  if(DBA_Start between 8 and 14 , 
							sum(P1.Avg_Rooms_Sold) 
						+ 	sum(P2.Avg_Rooms_Sold)
						+ (	sum(P3.Avg_Rooms_Sold)*(Bkwd_DaysLeft/Current_BkWd_Length)),
					  if(DBA_Start between 4 and 7 , 
							sum(P1.Avg_Rooms_Sold) 
						+ (	sum(P2.Avg_Rooms_Sold)*(Bkwd_DaysLeft/Current_BkWd_Length)),
					  (sum(P1.Avg_Rooms_Sold)*(Bkwd_DaysLeft/Current_BkWd_Length))
                      )
                      )
                      )
                      )
                      )
                      )
                      )
                      ,2)
                      ,0)
	as FCST_Most_Likely,

if(round(sum(Rooms_OTB) 
			+ round(  if(DBA_Start between 64 and 91 , 
							sum(P1.Avg_Rooms_Sold)
						+ 	sum(P2.Avg_Rooms_Sold)
						+ 	sum(P3.Avg_Rooms_Sold)
                        + 	sum(P4.Avg_Rooms_Sold)
                        + 	sum(P5.Avg_Rooms_Sold)
                        + 	sum(P6.Avg_Rooms_Sold)
                        + 	sum(P7.Avg_Rooms_Sold)
                        + (	sum(P8.Avg_Rooms_Sold)*(Bkwd_DaysLeft/Current_BkWd_Length)),
            
					  if(DBA_Start between 43 and 63 , 
							sum(P1.Avg_Rooms_Sold)
						+ 	sum(P2.Avg_Rooms_Sold)
						+ 	sum(P3.Avg_Rooms_Sold)
                        + 	sum(P4.Avg_Rooms_Sold)
                        + 	sum(P5.Avg_Rooms_Sold)
                        + 	sum(P6.Avg_Rooms_Sold)
                        + (	sum(P7.Avg_Rooms_Sold)*(Bkwd_DaysLeft/Current_BkWd_Length)),
                        
					  if(DBA_Start between 29 and 42 , 
							sum(P1.Avg_Rooms_Sold) 
						+ 	sum(P2.Avg_Rooms_Sold)
						+ 	sum(P3.Avg_Rooms_Sold)
                        + 	sum(P4.Avg_Rooms_Sold)
                        + 	sum(P5.Avg_Rooms_Sold)
                        + (	sum(P6.Avg_Rooms_Sold)*(Bkwd_DaysLeft/Current_BkWd_Length)),
                        
					  if(DBA_Start between 22 and 28 , 
							sum(P1.Avg_Rooms_Sold)
						+ 	sum(P2.Avg_Rooms_Sold)
						+ 	sum(P3.Avg_Rooms_Sold)
                        + 	sum(P4.Avg_Rooms_Sold)
                        + (	sum(P5.Avg_Rooms_Sold)*(Bkwd_DaysLeft/Current_BkWd_Length)),
                        
					  if(DBA_Start between 15 and 21 , 
							sum(P1.Avg_Rooms_Sold) 
						+ 	sum(P2.Avg_Rooms_Sold)  
						+ 	sum(P3.Avg_Rooms_Sold) 
                        + (	sum(P4.Avg_Rooms_Sold)*(Bkwd_DaysLeft/Current_BkWd_Length)),
            
					  if(DBA_Start between 8 and 14 , 
							sum(P1.Avg_Rooms_Sold) 
						+ 	sum(P2.Avg_Rooms_Sold)
						+ (	sum(P3.Avg_Rooms_Sold)*(Bkwd_DaysLeft/Current_BkWd_Length)),
					  if(DBA_Start between 4 and 7 , 
							sum(P1.Avg_Rooms_Sold) 
						+ (	sum(P2.Avg_Rooms_Sold)*(Bkwd_DaysLeft/Current_BkWd_Length)),
					  (sum(P1.Avg_Rooms_Sold)*(Bkwd_DaysLeft/Current_BkWd_Length))
                      )
                      )
                      )
                      )
                      )
                      )
                      )
                      ,2)
                      ,0) >= Avg_Rns_Sold, 'Strong', 'Weak')  
	as Forecasted_Demand_vs_LY
                
    from _OTB_at_CURRENTDATE C

left join Current_Projections P1 on c.Segments = p1.segments and c.season = p1.season and c.wd_We = p1.WD_WE and 
        c.BkWd_NOW =  p1.BK_WD 
left join Current_Projections P2 on c.Segments = p2.segments and c.season = p2.season and c.wd_We = p2.WD_WE and 
        c.BkWd_Next =  p2.BK_WD        
left join Current_Projections P3 on c.Segments = p3.segments and c.season = p3.season and c.wd_We = p3.WD_WE and 
        c.BkWd_2Next =  p3.BK_WD
left join Current_Projections P4 on c.Segments = p4.segments and c.season = p4.season and c.wd_We = p4.WD_WE and 
        c.BkWd_3Next =  p4.BK_WD
left join Current_Projections P5 on c.Segments = p5.segments and c.season = p5.season and c.wd_We = p5.WD_WE and 
        c.BkWd_4Next =  p5.BK_WD
left join Current_Projections P6 on c.Segments = p6.segments and c.season = p6.season and c.wd_We = p6.WD_WE and 
        c.BkWd_5Next =  p6.BK_WD
left join Current_Projections P7 on c.Segments = p7.segments and c.season = p7.season and c.wd_We = p7.WD_WE and 
        c.BkWd_6Next =  p7.BK_WD
left join Current_Projections P8 on c.Segments = p8.segments and c.season = p8.season and c.wd_We = p8.WD_WE and 
        c.BkWd_7Next =  p8.BK_WD
left join Historical_Averages HA on c.Segments = HA.Segments and c.Season = HA.Season and c.wd_we = HA.WD_WE

where   	c.Segments like SelectedSegment			 
group by 	stay_date 
having Forecasted_Demand_vs_LY like vs_Pace
order by Stay_date;


END //
DELIMITER ;

call _3Month_Demand_Categ('2018-03-01','Transie%', 'Str%');
