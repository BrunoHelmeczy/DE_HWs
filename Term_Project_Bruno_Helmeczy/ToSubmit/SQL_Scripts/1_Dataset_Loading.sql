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
