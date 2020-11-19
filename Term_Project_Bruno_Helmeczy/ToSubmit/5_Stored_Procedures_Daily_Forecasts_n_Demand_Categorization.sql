

-- Prcedure 1) Forecast 14 Days ahead - Most Likely, Pessimistic, Optimistic Scenarios
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
						(sum(P1.Avg_Rooms_Sold)+(sum(P1.StDev_Rooms_Sold)/2))
						+ (sum(P2.Avg_Rooms_Sold)+(sum(P2.StDev_Rooms_Sold)/2)) 
						+ ((sum(P3.Avg_Rooms_Sold)+(sum(P3.StDev_Rooms_Sold)/2))*(Bkwd_DaysLeft/Current_BkWd_Length)),
					  if(DBA_Start between 7 and 4, 
						((sum(P1.Avg_Rooms_Sold)+(sum(P1.StDev_Rooms_Sold)/2)) 
						+ (sum(P2.Avg_Rooms_Sold)+(sum(P2.StDev_Rooms_Sold)/2))*(Bkwd_DaysLeft/Current_BkWd_Length)),
					  ((sum(P1.Avg_Rooms_Sold)+(sum(P1.StDev_Rooms_Sold)/2))*(Bkwd_DaysLeft/Current_BkWd_Length)))),2),0) 
as FCST_Optimistic,
                        
		round(sum(Rooms_OTB) 
			+ round(if(DBA_Start between 14 and 8, 
							(sum(P1.Avg_Rooms_Sold) + sum(P2.Avg_Rooms_Sold)) 
							+ (sum(P3.Avg_Rooms_Sold)*(Bkwd_DaysLeft/Current_BkWd_Length)),
						if(DBA_Start between 7 and 4, 
								(sum(P1.Avg_Rooms_Sold)+(sum(P2.Avg_Rooms_Sold)*(Bkwd_DaysLeft/Current_BkWd_Length))),
						(sum(P1.Avg_Rooms_Sold)*(Bkwd_DaysLeft/Current_BkWd_Length)))),2),0) 
as FCST_Most_Likely,
                    
		round(sum(Rooms_OTB) 
        + round(if(DBA_Start between 14 and 8, 
						(sum(P1.Avg_Rooms_Sold)-(sum(P1.StDev_Rooms_Sold)/2)
						+ (sum(P2.Avg_Rooms_Sold)-(sum(P2.StDev_Rooms_Sold)/2)) 
						+ (sum(P3.Avg_Rooms_Sold)-(sum(P3.StDev_Rooms_Sold)/2))*(Bkwd_DaysLeft/Current_BkWd_Length)),
					if(DBA_Start between 7 and 4, 
							((sum(P1.Avg_Rooms_Sold)-(sum(P1.StDev_Rooms_Sold)-2)) 
							+ (sum(P2.Avg_Rooms_Sold)-(sum(P2.StDev_Rooms_Sold)/2))*(Bkwd_DaysLeft/Current_BkWd_Length)),
						((sum(P1.Avg_Rooms_Sold)-(sum(P1.StDev_Rooms_Sold)/2))*(Bkwd_DaysLeft/Current_BkWd_Length)))),2),0) 
as FCST_Pessimistic
                
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

-- Report Dates: On Some dates, data was not collected - so no data exists to call  
call Daily_2wk_Forecast('2018-05-14');

-- Prcedure 2) Forecast 91 Days ahead - Most Likely, Pessimistic, Optimistic Scenarios
	-- Wildcard filter by segment
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


-- Prcedure 3) Categorize Days 91 days ahead into ahead / behind schedule
DROP PROCEDURE IF EXISTS _3Month_Demand_Categ;
DELIMITER //
CREATE PROCEDURE _3Month_Demand_Categ( IN Report_Date date, In SelectedSegment varchar(30))
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
select 		c.Segments, Res_date as FCST_Date, Stay_date, DBA_Start,
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
                      ,0) >= Avg_Rns_Sold, 'AHEAD', 'BEHIND')  
	as Pace_vs_LY
                
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
group by 	stay_date order by Stay_date;


END //
DELIMITER ;

call _3Month_Demand_Categ('2018-03-01','Tran%');
