## Data Engineering 1 SQL for Analysts Term Project - Bruno Helmeczy

### Preface
The dataset is 2 years’ room sales data from Novotel Al Barsha, a 465 room, 4-star hotel in Dubai, operated by AccorHotels under a Management Agreement, a distinguishing factor, as the Revenue Management (RM) process is executed in-house, not from central locations. The RM Process is a perpetual cycle of analysing data obtained from a variety of sources in light of a pre-defined goal (usually maximizing operating profits), calculating forecasts on Strategic-, Tactical-, & Operational levels, & optimizing decision-making with regards to pricing & room availability controls. 

The RM functions’ purpose is to maximize Gross Operating Profit (GOP) using disciplined, data-driven supply- & demand management tactics, manifested as optimizing room prices & room availability, subject to seasons, markets, room types, customer segments, booking channels & the number of available rooms. Thus, structured & reliable information is indispensable for attaining RM’s objectives.  Indeed, the RM department has increasingly been responsible for managing & providing all necessary information in a condensed format to many departments & stakeholders, most crucially the commercial team (Sales & Marketing), leadership (General Managers, Ownership representatives), & the front office team (Reception). This Project presents a MySQL-based solution to informing some of the stakeholders with the data available, after a diagnostic analysis extracting a series of queries stored as views. Typical Key Performance Indicators (KPIs) to measure Hotels' management success are: 
- **Occupancy %** (Occ%): The percentage of available room nights sold in a given time period: 
    - (Sum of Room Nights sold) / ((Count of Distinct Stay Nights) * (Nr of rooms in the hotel i.e. Availability))  
- **Average Daily Rate** (ADR): The Average Price at which all rooms were sold:
    - (Sum of Revenue Earned) / (Sum of Room Nights sold)
- **Revenue per Available Room** (RevPAR): The amount of money earned per available Room Night on average:
    - (Sum of Revenue Earned) / ((Count of Distinct Stay Nights) * (Nr of rooms in the hotel i.e. Availability)) = ADR * Occupancy % 
 
 These measures are of interest from perspective of multiple dimensions, or their combinations:
 - By Customer Segments: 
 - By Stay Dates' dimensions: Days of the Week, Season, month, year the stay date of interest falls into
 - By Relative Booking Date dimensions (i.e. Days Before Arrival): Nr of days / weeks ahead, or in a specific Booking Window/Period 


**By Customer Segments**




**By Days of the Week & By Season**

 
 <img width="401" alt="EDA_1_KPIs_by_Weekdays" src="https://user-images.githubusercontent.com/71438198/99775990-7c4fae80-2b10-11eb-9d81-7b9dfe7cd315.png">
 View Title: EDA_1: KPIs by Weekdays
 
**By Booking Windows** 
 
 
 

The 1st Data Mart answers **How stay months' KPIs (Occupancy % & RevPAR) Progress Month-to-Month ?** This informs questions like **How did we get here?** by showing not only final results for a month (e.g. January 2018) after the month has concluded (Feb 1st), but what was the months' status at the begining of the month in question (January 1st), & at the beginning of earlier months (e.g. December 1st & November 1st). Outputs are faster if stored as a procedure & limiting 

The 1st Stored Procedure informs the Front Office Team (i.e. Reception) on **How many staff members should be scheduled on each day ?** by forecasting the number of rooms sold per day for the next 2 weeks' stay dates (with current date as an input parameter). Reception teams' workload are proportionate to how many guests are in-house, therefore the forecast informs the number of receptionists to schedule together on a shift. For simplicity, it is assumed no less than 2 employees should by scheduled & that 1 employee can handle ca. 93 rooms (i.e. 20% of available rooms - not all rooms check-in/out & not all guests need employee assistance).

The 2nd Stored Procedure answers **How many rooms is the hotel likely to sell for each day 3 months ahead, at the total hotel or segment level, given a date on which the forecast is calculated?** This extends the 1st Procedure by calculating daily forecasts for 91 days into the future (from an inputed report creation date), while accounting for uncertainty (by calculating Most Likely, Optimistic & Pessimistic scenarios) & enabling forecasting either for the hotel in total, or a specific segment. Forecasts are calculated by adding rooms sold at the moment & expected rooms to be sold in the future. This expectation is calculated by summing historical averages & standard deviations of rooms sold in each future booking window of **Similar Stay Dates**. A similar stay date is defined as that which falls in the same Season & same Weekday-type (i.e. WD for Weekday, or WE for Weekend). 

**Example:** When forecasting **Transient** demand for the stay date of **31st March, 2018** (a weekend day), on the report date of 1st March 2018 (i.e. 31 days before arrival), 


, calculated as conditioned on the segment forecasted, the Season & the Weekday-type the stay date falls into,  

It is assumed these outputs are extracted & loaded to other software tools for further processing / reporting / visualization. 1 such example is provided with the 3rd Stored Procedure. 

The 3rd Stored Procedure provides 1 such use case for the 2nd, categorizing stay dates' forecasted rooms sold (i.e. Demand) as 'Strong' or 'Weak' compared to historical average rooms sold  



### 1) Data Source - Novotel Al Barsha, Dubai Daily Sales January 2017 - October 2018
- Loading Data - Purpose per table
### 2) Dataset Denormalization & Consolidated Data Tables
- brief variable descriptions
### 3) Analytical Questions
- Questions to answer with EDA / Data Marts / Stored Procedures
### 3) Exploratory Data Analysis - By Weekdays / By Seasons / By Booking Windows
- Findings & tables by topic
### 4) Data Marts
- Monthly KPI Progression Month_2_Month
### 5) Stored Procedures
- Daily Forecast 2 weeks ahead
- Daily Forecast 91 days ahead - Choose Segment | Total Hotel
- Daily Forecast 91 days ahead & categorize Forecast versus Historical Average Room Sold by Season & Weekday 



