## Data Engineering 1 SQL for Analysts Term Project - Bruno Helmeczy

### Preface
The dataset is 2 years’ room sales data from Novotel Al Barsha, a 465 room, 4-star hotel in Dubai, operated by AccorHotels under a Management Agreement, a distinguishing factor, as the Revenue Management (RM) process is executed in-house, not from central locations. The RM Process is a perpetual cycle of analysing data obtained from a variety of sources in light of a pre-defined goal (usually maximizing operating profits), calculating forecasts on Strategic-, Tactical-, & Operational levels, & optimizing decision-making with regards to pricing & room availability controls. 

The RM functions’ purpose is to maximize Gross Operating Profit (GOP) using disciplined, data-driven supply- & demand management tactics, manifested as optimizing room prices & room availability, subject to seasons, markets, room types, customer segments, booking channels & the number of available rooms. Thus, structured & reliable information is indispensable for attaining RM’s objectives.  Indeed, the RM department has increasingly been responsible for managing & providing all necessary information in a condensed format to many departments & stakeholders, most crucially the commercial team (Sales & Marketing), leadership (General Managers, Ownership representatives), & the front office team (Reception). This Project presents a MySQL-based solution to informing some of the stakeholders with the data available, after a diagnostic analysis extracting a series of queries stored as views. 

### 1) Data Source - Novotel Al Barsha, Dubai Daily Sales January 2017 - October 2018
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
 - **By Customer Segments:** Any business's 1st question is **How do we make money & Selling to who ?**
 - **By Stay Dates:** Days of the Week, Season, month, year the stay date of interest falls into. As the product hotels sell, similar stay dates are aggregated together for further analysis, in hopes of extracting generalizable insights onto similar situations in the future.  Hotels show 2 types of seasonality consistently: Weekly & Annual
 - **By Relative Booking Date** (i.e. Days Before Arrival): Nr of days / weeks ahead, or in a specific Booking Window/Period. As the product is perishable (past stay nights cannot be sold) & the objective is to maximize total revenues for a stay date, season, or year, a key dimension is to observe guests' buying behaviour in terms of how far in advance do they book relative to their arrival. Research has shown, that influencing guests' purchase timing behaviour is possible only to a minimal extent, due to the abundance of alternatives available at any given moment (e.g. the number of hotels in Dubai). Thus, Sales performance can be seperately evaluated by booking windows, as different customers are booking in different windows. Though lagging KPIs might be compsensated closer to arrival, potential customers choosing to book elsewhere cannot be recovered later, as they already chosen a competitor.


**By Customer Segments**




**By Days of the Week & By Season**  



<img width="301" alt="EDA_1_KPIs_by_Weekdays" src="https://user-images.githubusercontent.com/71438198/99775990-7c4fae80-2b10-11eb-9d81-7b9dfe7cd315.png">
 View Title: EDA_1: KPIs by Weekdays
 
**By Booking Windows** 
 
 
 

The 1st Data Mart answers **How stay months' KPIs (Occupancy % & RevPAR) Progress Month-to-Month ?** This informs questions like **How did we get here?** by showing not only final results for a month (e.g. January 2018) after the month has concluded (Feb 1st), but what was the months' status at the begining of the month in question (January 1st), & at the beginning of earlier months (e.g. December 1st & November 1st). Outputs are faster if stored as a procedure & limiting 

The 1st Stored Procedure informs the Front Office Team (i.e. Reception) on **How many staff members should be scheduled on each day ?** by forecasting the number of rooms sold per day for the next 2 weeks' stay dates (with current date as an input parameter). Reception teams' workload are proportionate to how many guests are in-house, therefore the forecast informs the number of receptionists to schedule together on a shift. For simplicity, it is assumed no less than 2 employees should by scheduled & that 1 employee can handle ca. 93 rooms (i.e. 20% of available rooms - not all rooms check-in/out & not all guests need employee assistance).

The 2nd Stored Procedure answers **How many rooms is the hotel likely to sell for each day 3 months ahead, at the total hotel or segment level, given a date on which the forecast is calculated?** This extends the 1st Procedure by calculating daily forecasts for 91 days into the future (from an inputed report creation date), while accounting for uncertainty (by calculating Most Likely, Optimistic & Pessimistic scenarios) & enabling forecasting either for the hotel in total, or a specific segment. Forecasts are calculated by adding rooms sold at the moment & expected rooms to be sold in the future. This expectation is calculated by summing historical averages & standard deviations of rooms sold in each future booking window of **Similar Stay Dates**. A similar stay date is defined as that which falls in the same Season & same Weekday-type (i.e. WD for Weekday, or WE for Weekend). 

**Example:** When forecasting **Transient** demand for the stay date of **31st March, 2018** (a weekend day), on the report date of 1st March 2018 (i.e. 31 days before arrival), 


, calculated as conditioned on the segment forecasted, the Season & the Weekday-type the stay date falls into,  

It is assumed these outputs are extracted & loaded to other software tools for further processing / reporting / visualization. 1 such example is provided with the 3rd Stored Procedure. 

The 3rd Stored Procedure provides 1 such use case for the 2nd, categorizing stay dates' forecasted rooms sold (i.e. Demand) as 'Strong' or 'Weak' compared to historical average rooms sold  





### 4) Data Marts
- Monthly KPI Progression Month_2_Month
### 5) Stored Procedures
- Daily Forecast 2 weeks ahead
- Daily Forecast 91 days ahead - Choose Segment | Total Hotel
- Daily Forecast 91 days ahead & categorize Forecast versus Historical Average Room Sold by Season & Weekday 



