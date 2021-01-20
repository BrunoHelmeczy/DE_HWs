## Data Engineering 1 SQL for Analysts Term Project - Bruno Helmeczy

### Preface
The dataset is 2 years’ room sales data from a 465 room, 4-star hotel in Dubai, operated by AccorHotels under a Management Agreement, a distinguishing factor, as the Revenue Management (RM) process is executed in-house, not from central locations. The RM Process is a perpetual cycle of analysing data obtained from a variety of sources in light of a pre-defined goal (usually maximizing operating profits), calculating forecasts on Strategic-, Tactical-, & Operational levels, & optimizing decision-making with regards to pricing & room availability controls. 

The RM functions’ purpose is to maximize Gross Operating Profit (GOP) using disciplined, data-driven supply- & demand management tactics, manifested as optimizing room prices & room availability, subject to seasons, markets, room types, customer segments, booking channels & the number of available rooms. Thus, structured & reliable information is indispensable for attaining RM’s objectives.  Indeed, the RM department has increasingly been responsible for managing & providing all necessary information in a condensed format to many departments & stakeholders, most crucially the commercial team (Sales & Marketing), leadership (General Managers, Ownership representatives), & the front office team (Reception). This Project presents a MySQL-based solution to informing some of the stakeholders with the data available, after a diagnostic analysis extracting a series of queries stored as views. 

### 1) Data Source - Dubai Hotel Daily Sales January 2017 - October 2018
<img width="400" alt="NAB_Datamodel" src="https://user-images.githubusercontent.com/71438198/99824141-91026580-2b55-11eb-850c-ea70cd79cfe4.png">

The dataset comprises 4 tables: 3 dimension tables & 1 fact table. The 3 dimensions analyzed are that of Stay Dates (dCalendar_DOS i.e. Date of Stay), Customer Segments (dSegmentation) & Relative Booking behaviour, measured in terms of Days Before Arrival (dDBA). Major - demand influencing - Events are noted for stay dates (as these increase the value of certain room nights), besides the weekdays & season any stay date falls into. The segmentation table specifies a segment hierarchy, from which the highest level is used for analysis. The dDBA table bins specific instances of days before arrival into booking windows.

The fact table comprises booking information on stay dates between January 1st 2017 & October 22nd 2018. Each stay date is observed from perspective of many reservation dates, at most 91 days in advance many times & once after the stay date has concluded to note the final result. 1 row observation accounts for 1 stay date at 1 reservation date, for 1 customer segment (the smallest unit of which are RMLs), within a booking interval (the time interval between 2 consecutive reports from which the dataset was derived). Key variables are OTB_Rooms & OTB_Rev (the Number of rooms & revenue sold upto this reservation date), Pickup_Rooms & Pickup_Rev (the Number of rooms & revenue sold in the booking interval observed). 

Additional variables (not analysed here) are the PricePoint the hotel chose in the observed booking interval, how that price point compares to competitors' average price & the Rate the pricepoint maps to for a specific sub-segment. As only transients are assumed to be price-sensitive segments theoretically, omitting these variables in further analysis does not prevent exploring what happened to the hotel in the observed stay period.


### 2) Dataset Denormalization & Consolidated Data Tables
The Denormalized Dataset is joined in a view of the conveniently same name. Also, 4 Consolidated Data Tables (CDTs) were created as views, to simplify standard deviation calculations, where used. While this was also possible via WITH clauses, this would have been needed in every View for Exploratory data analysis. 2 CDTs summarize On-The_Book figures per stay date by reservation date, 1-1 by segments & the whole hotel. 2 more CDTs focus on rooms sold by booking windows, again either by segment or the total hotel. 


### 3) Exploratory Data Analysis - By Weekdays / By Seasons / By Booking Windows

Typical Key Performance Indicators (KPIs) to measure Hotels' management success are: 
- **Occupancy %** (Occ%): The percentage of available room nights sold in a given time period: 
    - (Sum of Room Nights sold) / ((Count of Distinct Stay Nights) * (Nr of rooms in the hotel i.e. Availability))  
- **Average Daily Rate** (ADR): The Average Price at which all rooms were sold:
    - (Sum of Revenue Earned) / (Sum of Room Nights sold)
- **Revenue per Available Room** (RevPAR): The amount of money earned per available Room Night on average:
    - (Sum of Revenue Earned) / ((Count of Distinct Stay Nights) * (Nr of rooms in the hotel i.e. Availability)) = ADR * Occupancy % 
 
 #### 3.1) Analytical Questions
 
 The KPIs above are of interest from perspective of multiple dimensions, or their combinations:
 - **By Customer Segments:** Any business's 1st question is **How do we make money & Selling to whom ?** More precisely, in what proportion? The hotel serves 4 segments: 
    - Transients, privately booking their holidays through e.g. Expedia & Booking.com.
    - Corporate guests, whose corporations negotiate room prices with the hotel ca. a year in advance, in light of how many rooms they expect to use.
    - Groups, arriving for certain conferences in the area, requiring large Nr of rooms for few days.
    - Contracted Leisure guests booking through wholesalers.
 - **By Stay Dates:** Days of the Week, Season, month, year the stay date of interest falls into. As the product hotels sell, similar stay dates are aggregated together for further analysis, in hopes of extracting generalizable insights onto similar situations in the future.  Hotels show 2 types of seasonality consistently: Weekly & Annual
 - **By Relative Booking Date** (i.e. Days Before Arrival): Nr of days / weeks ahead, or in a specific Booking Window/Period. As the product is perishable (past stay nights cannot be sold) & the objective is to maximize total revenues for a stay date, season, or year, a key dimension is to observe guests' buying behaviour in terms of how far in advance do they book relative to their arrival. Research has shown, that influencing guests' purchase timing behaviour is possible only to a minimal extent, due to the abundance of alternatives available at any given moment (e.g. the number of hotels in Dubai). Thus, Sales performance can be seperately evaluated by booking windows, as different customers are booking in different windows. Though lagging KPIs might be compsensated closer to arrival, potential customers choosing to book elsewhere cannot be recovered later, as they already chosen a competitor.


#### 3.2) By Customer Segments

**How many Rooms Sold & RevPAR come from which segment ?**

**Which are the most profitable segments ?**

<img width="300" alt="EDA_2_KPIs_by_Segments" src="https://user-images.githubusercontent.com/71438198/99818190-3285b900-2b4e-11eb-85a7-7c1d7aef46c9.png">

As visible, Transients account for both highest number of average rooms sold & ADR, resulting in the most Revenue contributed to the hotel. One can also see, Transients are the highest paying (i.e. most profitable) customers, with Corporate guests average being slightly below average. Their booking behaviour however tends to shift closer to arrival date, enabled by tech advancements. Based on this query alone, 1 could assume this hotel is Leisure focused. Standard Deviations are calculated to to help characterize these segments. Most notably, standard deviation / average of rooms are high for Groups, indicating the segments' ad-hoc nature. 

**How did these segments perform Year-over-Year ?**

<img width="400" alt="EDA_4_KPIs_Changes_YOY_by_Segment" src="https://user-images.githubusercontent.com/71438198/99818192-331e4f80-2b4e-11eb-999e-2432a4498d1d.png">

This view shows a grimm picture. Corporate guest accounted for 5% less occupancy, though this loss was compensated by Transients & Groups. Average Prices however, decreased by ca. 13% for Corporates & by ca. 20% for other segments. This resulted in a ca. 7.5 Million AED Annualized Revenue loss YOY (ca. 1.9M USD - November & December 2018 data is not available). 1 can see Corporates & Transients are disproportionately responsible for these losses. As significant changes are visible year-over-year, such comparisons are prioratized.

#### 3.3) By Days of the Week & By Season

**Where did these year-over-year losses come from ? Which Season / Weekday ?**

<img width="301" alt="EDA_1_KPIs_by_Weekdays" src="https://user-images.githubusercontent.com/71438198/99775990-7c4fae80-2b10-11eb-9d81-7b9dfe7cd315.png">
 
From this query by weekdays, 2 things can be observed: 1st, the hotel is rather business-focused, based on boasting higher Occupancies & RevPAR Monday - Thursday. 2nd, Year-Over-Year revenue losses are not disproportionately high for any day of the week, though highest for Thursday & Friday, the 2 weekend days in Dubai.

**How can the Dubai Markets' annual seasonality be characterized ?**
 
 <img width="400" alt="EDA_2_KPIs_by_Seasons" src="https://user-images.githubusercontent.com/71438198/99818189-3285b900-2b4e-11eb-8034-6932b9010b83.png"> <img width="400" alt="EDA_7_KPIs_Changes_YOY_by_Season" src="https://user-images.githubusercontent.com/71438198/99818197-33b6e600-2b4e-11eb-976e-90f50e142e94.png">
 
 1st, note from the query left-hand side, that Seasons were designated based on RevPAR, i.e. on how much money can be made per available room in any given period. Also note the Nr of days in each season (considering the 22 months observed). Annual seasonality in Dubai can thus be characterized with longer High & Low Seasons, with a ca. Month-long transition period, noted as Mid season. Low season are those of the Arab summer May - September (40 degrees celsius in shade). What complicates this (also the reason for not denoting Months' seasons) is the lunar month of Ramadan (shifting 2 weeks / year), which during this time was in June & May, resulting in RevPARs ca. 50% lower vs even Low Season. 2nd, heaviest RevPAR losses were during the transition period in Mid season & most revenue was lost during High season, despite Occupancy increasing with 3.2%.
 
 **During which season did Corporates' & Transients' lost the most Revenue ?**
 
 <img width="400" alt="EDA_8_KPIs_Changes_YOY_by_Segments_n_Seasons" src="https://user-images.githubusercontent.com/71438198/99818198-33b6e600-2b4e-11eb-8892-c22622d463b7.png">
 
 Having noted the most revenue was lost on Corporates & Transients, focus is dedicated to these segments. From the table above, note that Transient RevPAR losses during low season & Ramadan are relatively small, however during high season the decrease in average price netted 1.3M AED Revenue loss annualized. 
 
 **So What might have happened in this hotel year over year?**

 What should be noted, is the 2 segments' behaviour: Corporate contracts are negotiated a year in advance, while with transients, managers cannot take action so far in advance. Noting Corporate occupancy decreased 4.3% - 7.4% in 3/4 seasons, one could wonder how have these negotiations (termed RFP i.e. Request for Proposal season) went. In this light, year-over-year performance by segments could be seen as Transients & Groups segments trying to compensate for the lost performance in the Corporate segment. 
 
 
#### 3.4) By Booking Windows 

**In which booking windows does the hotel make the most money ?**

<img width="350" alt="EDA_11_KPIs_by_Booking_Windows" src="https://user-images.githubusercontent.com/71438198/99818203-344f7c80-2b4e-11eb-95d1-d06e63824f66.png">

Observing the query result above, note that of the ca. 64 Million AED Revenue earned in the 22 Month period, 33 Million AED, more than 50% of all Revenues are earned in the last 2 weeks before arrival.

**How did Revenue per Booking Window Change Year over Year for Corporate & Transient guests ?**

<img width="350" alt="EDA_13_KPIs_Changes_YOY_by_Segment_BkWd" src="https://user-images.githubusercontent.com/71438198/99818205-344f7c80-2b4e-11eb-9abf-7a0818fd8330.png">





- Monthly KPI Progression Month_2_Month
### 5) Stored Procedures
- Daily Forecast 2 weeks ahead
- Daily Forecast 91 days ahead - Choose Segment | Total Hotel
- Daily Forecast 91 days ahead & categorize Forecast versus Historical Average Room Sold by Season & Weekday 


### 4) Data Marts as Stored Procedures

#### 4.1) With Static Data 

**How stay months' KPIs (Occupancy % & RevPAR) Progress Month-to-Month ?**

<img width="650" alt="DataMart_Monthly_Occ_n_RevPAR_Progression_Month_2_Month" src="https://user-images.githubusercontent.com/71438198/99818188-31548c00-2b4e-11eb-993b-351417b202a9.png">

The 1st Data Mart informs questions like **'How did we get here?'** arising during strategy meetings, by showing not only final results for a month (e.g. January 2018) after the month has concluded (Feb 1st), but what the months' status was at the begining of the current month (January 1st) & of earlier months (e.g. December 1st & November 1st). Outputs are faster if stored as a procedure & limited to observing 3 stay months (Plase note that query duration was timed at upto 90 seconds).   

#### 4.2) With Data available as of a certain point in time

The stored procedures below are all based on forecasts, which are informed by data from similar stay dates, available at the time the report is created calling the stored procedure. Forecasts use historical averages & standard deviations, observed by booking windows for similar stay dates. These are continuously refined statistics, as new data comes available. This is resolved by sub-querying data from relevant tables available as of the data the stored procedures are called. 

**How many Employees should the Reception be staffed with ?**

<img width="400" alt="Stored_Proc_2Wk_Forecast_4_FO" src="https://user-images.githubusercontent.com/71438198/99818206-34e81300-2b4e-11eb-89e5-f1955ba634b2.png">

This Stored Procedure informs the Front Office Team (i.e. Reception) on **How many staff members should be scheduled on each day in the next 2 weeks?** by forecasting the number of rooms sold per day for the next 2 weeks' stay dates (with current date as an input parameter). Reception teams' workload are proportionate to how many guests are in-house, therefore the forecast informs the number of receptionists to schedule together on a shift. For simplicity, it is assumed no less than 2 employees should by scheduled & that 1 employee can handle ca. 93 rooms (i.e. 20% of available rooms - not all rooms check-in/out & not all guests need employee assistance).

**How many rooms is the hotel likely to sell for each day 3 months ahead, at the total hotel or segment level, given a date on which the forecast is calculated?**

<img width="400" alt="Stored_Proc_3Mnth_Forecast_Snippet" src="https://user-images.githubusercontent.com/71438198/99818208-34e81300-2b4e-11eb-90fd-a81cf019d800.png">

This procedure extends the 1st Procedure by calculating daily forecasts for 91 days into the future (from an inputed report creation date), while accounting for uncertainty (by calculating Most Likely, Optimistic & Pessimistic scenarios) & enabling forecasting either for the hotel in total, or a specific segment. Forecasts are calculated by adding rooms sold at the moment & expected rooms to be sold in the future. This expectation is calculated by summing historical averages & standard deviations of rooms sold in each future booking window of **Similar Stay Dates**. A similar stay date is defined as that which falls in the same Season & same Weekday-type (i.e. WD for Weekday, or WE for Weekend). 

**Example:** When forecasting **Transient** demand for the stay date of **31st March, 2018** (a weekend day in High Season), on the report date of 1st March 2018 (i.e. 31 days before arrival), expected most likely future room sales are calculated as the sum of average room sales in each future booking window of 31st March, 2018. To calculate optimistic future room sales half a standard deviation observed in each future booking window are added to average room sales per booking window, while for pessimistic forecasts half a standard deviation is subtracted from the average room sales by booking window observed for similar stay dates.  

**Which future stay dates' forecasted 'Most Likely' demand is Stronger / Weaker vs Historical Average results ?**

<img width="400" alt="Stored_Proc_3Mnth_DemandCateg_Snippet" src="https://user-images.githubusercontent.com/71438198/99820082-94472280-2b50-11eb-9e94-b1b3e956e8d4.png">

The 3rd Stored Procedure provides 1 use case for daily forecasts, categorizing stay dates' forecasted rooms sold (i.e. Demand) as 'Strong' or 'Weak' compared to historical average rooms sold. Calling this procedure for the total hotel informs Management teams on how the hotel is most likely to perform in the future, while filtering for specific segments informs the personell responsible for the segment on where performance is leading / lagging vs last year. Finally, filtering specifically the Demand Categorization column directly uncovers periods the respective sales team should focus on (if Demand is Weaker), or periods where there is increased opportunity to earn more revenue (if Demand is Stronger). 

Overall, questions this procedure can answer, based on the most likely forecasted outcome, with all available data considered, are:
- **Will the hotel most likely perform better or worse in the next 91 days** vs average past results observed?
- **Will either segment most likely perform better or worse in the next 91 days** vs average past results observed?
- **Which future stay dates represent an opportunity to make additional revenues?**
- **Which future stay dates should sales teams focus on, to decrease lagging performance vs historical average results?**
