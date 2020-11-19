use hw1_nab_dataset;

-- 1st Data Mart: Stay_Months KPIs at start of earlier months - by Segments:
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

drop view if exists Monthly_KPIs_by_Reservation_Month_Segment;
create view Monthly_KPIs_by_Reservation_Month_Segment as
select F.Stay_month, F.Segments,
			Final_Occ_Percent, Occ_Month_Start, Occ_1Month_Prev, Occ_2Month_Prev,
            Final_RevPAR, RevPAR_Month_Start, RevPAR_1Month_Prev, RevPAR_2Month_Prev
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
order by stay_month, Segments;
    
select * from Monthly_KPIs_by_Reservation_Month_Segment; 