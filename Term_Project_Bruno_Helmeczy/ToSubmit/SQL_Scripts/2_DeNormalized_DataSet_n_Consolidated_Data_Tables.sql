-- Consolidated Data Tables: daily kpis (1) total/ (2) segment -> (3) total+bk_wd / (4) segment+bk_wd
		-- Needed for pre-aggregation for StDev calculations 
			-- Final vs by_DBA ? -> by_DBA -> if(Rep_date > DOS, Dba = -1, rep_Date = DOR)
	
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
