	-- by DOW / WE_WD 
    -- 1) KPIs by Weekdays -> Good to Keep -> Maybe drilldown by Segments -> 2)
drop view if exists _1_EDA_01_Actual_KPIs_by_Weekdays_4_TotalHotel;
		create view _1_EDA_01_Actual_KPIs_by_Weekdays_4_TotalHotel as
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

-- 2) Show Segment Tendencies by Weekdays
drop view if exists _1_EDA_02_Actual_KPIs_by_Weekdays_n_Segments;
		create view _1_EDA_02_Actual_KPIs_by_Weekdays_n_Segments as
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

-- 3) Shows tendency YOY -? Maybe keep 2-4
drop view if exists _1_EDA_03_Actual_KPIs_by_Weekdays_YOY;
		create view _1_EDA_03_Actual_KPIs_by_Weekdays_YOY as	
select  Date_of_Week,
        round((sum(rooms_sold)/sum(availability))*100,2) 	as 'Occ_%'
        ,round((sum(rooms_sold)/sum(availability))*100,2) 	- ly.Occ					as Occ_vs_LY
        ,round(sum(total_revenue)/sum(rooms_sold),2) 									as ADR
        ,round(sum(total_revenue)/sum(rooms_sold),2) 		- ly.ADR					as ADR_vs_LY
        ,round(sum(total_revenue)/sum(availability),2)									as RevPAR
        ,round(sum(total_revenue)/sum(availability),2) 		- ly.RevPAR					as RevPAR_vs_LY
        ,round((sum(total_revenue)/sum(availability) - ly.RevPAR)*sum(availability),0) 	as Rev_YoY
from 0_cdt_1_total_daily_kpis f
left join dcalendar_dos c on c.Dates_dos = f.Stay_date
left join  (select 	f.years, f.DOW_nr, f.WD_We,
					round((sum(rooms_sold)/sum(availability))*100,2) 	as Occ,
					round(sum(total_revenue)/sum(rooms_sold),2) 		as ADR,
					round(sum(total_revenue)/sum(availability),2)		as RevPAR,
					round(avg(rooms_sold),2)							as Avg_Rooms_Sold,
					round(std(rooms_sold),2)							as StDev_Rooms_Sold,
					round(std(rooms_sold)/avg(rooms_sold),2)			as Rel_StDev,
					count(*) 											as Days 
				from 0_cdt_1_total_daily_kpis f 
					where f.dba_end = -1 and f.years = 2017
						group by DOW_nr) ly on ly.DOW_nr = f.DOW_nr 
	where f.dba_end = -1 and f.years = 2018
	group by f.DOW_nr order by f.dow_nr;

-- 4) Show YOY Segment Performance KPIs & Revenue_Loss/Gain_vs_LY
drop view if exists _1_EDA_04_Actual_KPIs_by_Segment_YOY;
		create view _1_EDA_04_Actual_KPIs_by_Segment_YOY as		
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


-- 5) Combine Segment & Weekday breakdown to see KPIs - also YOY & Revenue Loss/Gain
drop view if exists _1_EDA_05_Actual_KPIs_YOY_by_Weekdays_4_Segments;
		create view _1_EDA_05_Actual_KPIs_YOY_by_Weekdays_4_Segments as	
select 	f.WD_We, f.Segments,
	    round((sum(rooms_sold)/sum(availability))*100,2) 	as 'Occ_%', ly.Occ LY_OCC
        ,round((sum(rooms_sold)/sum(availability))*100,2) 	- ly.Occ					as Occ_vs_LY
        ,round(sum(total_revenue)/sum(rooms_sold),2) 									as ADR
        ,round(sum(total_revenue)/sum(rooms_sold),2) 		- ly.ADR					as ADR_vs_LY
        ,round(sum(total_revenue)/sum(availability),2)									as RevPAR
        ,round(sum(total_revenue)/sum(availability),2) 		- ly.RevPAR					as RevPAR_vs_LY
        ,round((sum(total_revenue)/sum(availability) - ly.RevPAR)*sum(availability),0) 	as Rev_YoY
from 0_cdt_2_segments_daily_kpis f
left join ( select 	WD_We, Segments,
				round((sum(rooms_sold)/sum(availability))*100,2) 	as Occ,
				round(sum(total_revenue)/sum(rooms_sold),2) 		as ADR,
				round(sum(total_revenue)/sum(availability),2)		as RevPAR,
				round(avg(rooms_sold),2)							as Avg_Rooms_Sold,
				round(std(rooms_sold),2)							as StDev_Rooms_Sold,
				round(std(rooms_sold)/avg(rooms_sold),2)			as Rel_StDev,
				count(*) 											as Days 
from 0_cdt_2_segments_daily_kpis f
	where f.dba_end = -1 and years = 2017
    group by Segments, WD_WE) ly 
    on f.wd_we = ly.WD_WE and f.Segments = ly.Segments
	where f.dba_end = -1 and years = 2018
	group by Segments, WD_WE 		order by Segments, WD_WE;



	-- by Season
-- 6) Intro Table (If there is such) - How hotel performs throughout the year
drop view if exists _1_EDA_06_Actual_KPIs_by_Seasons;
		create view _1_EDA_06_Actual_KPIs_by_Seasons as		
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
    
-- 7) Show KPI changes & Revenue Gain/Loss YOY by Season
drop view if exists _1_EDA_07_Actual_KPIs_by_Season_YOY;
		create view _1_EDA_07_Actual_KPIs_by_Season_YOY as		
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

-- 8) Okay -> Select by Seaason to see what happened
drop view if exists _1_EDA_08_Actual_KPIs_YOY_by_Seasons_n_Segments;
		create view _1_EDA_08_Actual_KPIs_YOY_by_Seasons_n_Segments as	
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

-- 9)
drop view if exists _1_EDA_09_Actual_KPIs_YOY_by_Weekdays_n_Seasons;
		create view _1_EDA_09_Actual_KPIs_YOY_by_Weekdays_n_Seasons as		
select 	f.Season, f.WD_WE,
	    round((sum(rooms_sold)/sum(availability))*100,2) 								as Occ	
        ,round((sum(rooms_sold)/sum(availability))*100,2) 	- ly.Occ					as Occ_vs_LY
        ,round(sum(total_revenue)/sum(rooms_sold),2) 									as ADR	
        ,round(sum(total_revenue)/sum(rooms_sold),2) 		- ly.ADR					as ADR_vs_LY
        ,round(sum(total_revenue)/sum(availability),2)									as RevPAR		
		,round(sum(total_revenue)/sum(availability),2) 		- ly.RevPAR					as RevPAR_vs_LY
        ,round(((sum(total_revenue)/sum(availability) - ly.RevPAR)*sum(availability))/1000,1) 	as Rev_K_YoY	
from 0_cdt_1_total_daily_kpis f
	left join (
			select 	Season, WD_WE,
					round((sum(rooms_sold)/sum(availability))*100,2) 	as Occ,
					round(sum(total_revenue)/sum(rooms_sold),2) 		as ADR,
					round(sum(total_revenue)/sum(availability),2)		as RevPAR,
					round(avg(rooms_sold),2)							as Avg_RNs,
					round(std(rooms_sold),2)							as StDev_RNs,
					round(std(rooms_sold)/avg(rooms_sold),2)			as Rel_StDev,
					count(*) 											as Days 
from 0_cdt_1_total_daily_kpis f
	where f.dba_end = -1
	group by Season, WD_WE					order by Stay_date
    )
    ly on ly.season = f.season and ly.WD_WE = f.WD_WE
	where f.dba_end = -1 and years = 2018
	group by Season, WD_WE					order by Stay_date;

-- 10)
drop view if exists _1_EDA_10_Actual_KPIs_YOY_by_Seasons_Weekdays_n_Segments;
	create view _1_EDA_10_Actual_KPIs_YOY_by_Seasons_Weekdays_n_Segments as		
select 	f.Segments, f.Season, f.WD_WE, 
	    round((sum(rooms_sold)/sum(availability))*100,2) 				as 'Occ_%'
        ,round((sum(rooms_sold)/sum(availability))*100,2) 	- ly.Occ 	as Occ_vs_LY
        ,round(sum(total_revenue)/sum(rooms_sold),2) 					as ADR
        ,round(sum(total_revenue)/sum(rooms_sold),2) 		- ly.ADR 	as ADR_vs_LY
        ,round(sum(total_revenue)/sum(availability),2)					as RevPAR
        ,round(sum(total_revenue)/sum(availability),2) 		- ly.RevPAR	as RevPAR_vs_LY
from 0_cdt_2_segments_daily_kpis f
	left join (
			select 	f.Season, f.WD_WE, Segments,
					round((sum(rooms_sold)/sum(availability))*100,2) 	as Occ,
					round(sum(total_revenue)/sum(rooms_sold),2) 		as ADR,
					round(sum(total_revenue)/sum(availability),2)		as RevPAR,
					round(avg(rooms_sold),2)							as Avg_RNs,
					round(std(rooms_sold),2)							as StDev_RNs,
					round(std(rooms_sold)/avg(rooms_sold),2)			as Rel_StDev,
					count(*) 											as Days 
from 0_cdt_2_segments_daily_kpis f
	where f.dba_end = -1 and years = 2017
	group by Season, WD_WE, Segments order by Segments, Stay_date, WD_WE) 
    ly on ly.season = f.season and f.wd_WE = ly.WD_WE and f.Segments  = ly.Segments 
    where f.dba_end = -1 and years = 2018
	group by Season, WD_WE, Segments order by Segments , Stay_date, WD_WE;

-- 11) Totals by Booking Windows
    -- by booking windows
drop view if exists _1_EDA_11_Revenue_n_Rooms_Sold_by_BookingWindow;
		create view _1_EDA_11_Revenue_n_Rooms_Sold_by_BookingWindow as		
select 	Bk_wd,
		round(sum(total_revenue)/sum(Pickup_RNs),2) 		as ADR
        ,round(sum(total_revenue)/1000000,2)				as Rev_Picked_up_M
		,round(avg(Pickup_RNs),2)							as Avg_Rooms_Sold
		,round(std(Pickup_RNs),2)							as StDev_Rooms_Sold
        ,round(std(Pickup_RNs)/avg(Pickup_RNs),2)			as Rel_StDev
        ,count(*) 											as Days 
from 0_cdt_3_total_daily_sales_bk_wd f
	group by Bk_wd	order by  Bk_wd;

-- 12) Segments by Booking Window
drop view if exists _1_EDA_12_Revenue_n_Rooms_Sold_per_BkWd_by_Segments;
		create view _1_EDA_12_Revenue_n_Rooms_Sold_per_BkWd_by_Segments as			
select 	Segments, Bk_wd,
		round(sum(Pickup_Revenue)/sum(Pickup_RNs),2) 		as ADR
        ,round(sum(pickup_revenue)/1000000,2)				as Rev_Picked_up_M        
		,round(avg(Pickup_RNs),2)							as Avg_Rooms_Sold
		,round(std(Pickup_RNs),2)							as StDev_Rooms_Sold
        ,round(std(Pickup_RNs)/avg(Pickup_RNs),2)			as Rel_StDev
from 0_cdt_4_segment_daily_kpis_bk_wd f
	group by Segments, Bk_wd	order by Segments, Bk_wd;
 
 
 -- 13)
drop view if exists _1_EDA_13_Revenue_n_Rooms_Sold_YOY_per_BkWd_by_Segments;
		create view _1_EDA_13_Revenue_n_Rooms_Sold_YOY_per_BkWd_by_Segments as		
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
    

-- 13+1) -> Used in With clause for Forecasting
drop view if exists _1_EDA_Plus1_AvgRooms_Sold_per_BkWd_by_Season_n_Segments;
		create view _1_EDA_Plus1_AvgRooms_Sold_per_BkWd_by_Season_n_Segments as		
select 	f.Season, f.WD_WE, Segments, Bk_wd,
        round(sum(Pickup_revenue)/sum(Pickup_RNs),2) 		as ADR,
		round(avg(Pickup_RNs),2)							as Avg_Rooms_Sold,
		round(std(Pickup_RNs),2)							as StDev_Rooms_Sold,
        round(std(Pickup_RNs)/avg(Pickup_RNs),2)			as Rel_StDev,
        count(distinct(stay_date)) 							as Days 
from 0_cdt_4_segment_daily_kpis_bk_wd f
	group by Season, WD_WE, Segments, Bk_wd		order by Segments, Season, WD_WE, Bk_wd;


