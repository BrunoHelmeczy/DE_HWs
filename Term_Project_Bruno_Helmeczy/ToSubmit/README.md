# Data Engineering 1 SQL for Analysts Term Project - Bruno Helmeczy

### Preface
The dataset is 2 years’ room sales data from Novotel Al Barsha, a 465 room, 4-star hotel in Dubai, operated by AccorHotels under a Management Agreement, a distinguishing factor, as the Revenue Management (RM) process is executed in-house, not from central locations. The RM Process is a perpetual cycle of analysing data obtained from a variety of sources in light of a pre-defined goal (usually maximizing operating profits), calculating forecasts on Strategic-, Tactical-, & Operational levels, & optimizing decision-making with regards to pricing & room availability controls. 

The RM functions’ purpose is to maximize Gross Operating Profit (GOP) using disciplined, data-driven supply- & demand management tactics, manifested as optimizing room prices & room availability, subject to seasons, markets, room types, customer segments, booking channels & the number of available rooms. Thus, structured & reliable information is indispensable for attaining RM’s objectives.  Indeed, the RM department has increasingly been responsible for managing & providing all necessary information in a condensed format to many departments & stakeholders, most crucially the commercial team (Sales & Marketing), leadership (General Managers, Ownership representatives), & the front office team (Reception). Typical Key Performance Indicators (KPIs) to measure management success are: 
- Occupancy % (Occ%): The percentage of available room nights sold in a given time period: 
    - (Sum of Room Nights sold) / ((Count of Distinct Stay Nights) * (Nr of rooms in the hotel i.e. Availability))  
- Average Daily Rate (ADR): The Average Price at which all rooms were sold:
    - (Sum of Revenue Earned) / (Sum of Room Nights sold)
- Revenue per Available Room (RevPAR): The amount of money earned per available Room Night on average:
    - (Sum of Revenue Earned) / ((Count of Distinct Stay Nights) * (Nr of rooms in the hotel i.e. Availability)) = ADR * Occupancy % 
 

This Project presents a MySQL-based solution to informing some of stakeholders with the data available. Specifically, after a diagnostic analysis extracting a series of queries stored as views, the 1st Data Mart answers:
 - How any stay months' KPIs (Occupancy) 



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


<img width="401" alt="EDA_1_KPIs_by_Weekdays" src="https://user-images.githubusercontent.com/71438198/99775990-7c4fae80-2b10-11eb-9d81-7b9dfe7cd315.png">
