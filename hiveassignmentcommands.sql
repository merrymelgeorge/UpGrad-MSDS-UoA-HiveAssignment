wget https://hiveassignmentdatabde.s3.amazonaws.com/Parking_Violations_Issued_-_Fiscal_Year_2017.csv;

hive

create database if not exists assignment;
use assignment;

create external table if not exists parking_violations (summons_number bigint, plate_id varchar(255), registration_state varchar(255), plate_type varchar(255), issue_date varchar(255), violation_code bigint, vehicle_body_type varchar(255), vehicle_make varchar(255), issuing_agency varchar(255), street_code1 bigint, street_code2 bigint, street_code3 bigint, vehicle_expiration_date bigint, violation_location varchar(255), violation_precinct bigint, issuer_precinct bigint, issuer_code bigint, issuer_command varchar(255), issuer_squad varchar(255), violation_time varchar(255), time_first_observed varchar(255), violation_county varchar(255), violation_in_front_of_or_opposite varchar(255), house_number varchar(255), street_name varchar(255), intersecting_street varchar(255), date_first_observed bigint, law_section bigint, sub_division varchar(255), violation_legal_code varchar(255), days_parking_in_effect varchar(255), from_hours_in_effect varchar(255), to_hours_in_effect varchar(255), vehicle_color varchar(255), unregistered_vehicle varchar(255), vehicle_year bigint, meter_number varchar(255), feet_from_curb bigint, violation_post_code varchar(255), violation_description varchar(255), no_standing_or_stopping_violation varchar(255), hydrant_violation varchar(255), double_parking_violation varchar(255)) row format delimited fields terminated by ',' lines terminated by '\n' stored as textfile tblproperties("skip.header.line.count"="1");

load data local inpath '/home/hadoop/Parking_Violations_Issued_-_Fiscal_Year_2017.csv' into table parking_violations;

create table if not exists parking_violations_2017 as select * from parking_violations where substr(issue_date,7,4) = '2017';


SELECT COUNT(summons_number) FROM parking_violations_2017;
SELECT COUNT(DISTINCT summons_number) FROM parking_violations_2017;

SELECT COUNT(DISTINCT registration_state) FROM parking_violations_2017;
SELECT COUNT(DISTINCT registration_state) FROM parking_violations_2017 WHERE registration_state IS NOT NULL;

SELECT registration_state, COUNT(registration_state) FROM parking_violations_2017 GROUP BY registration_state;

SELECT COUNT(summons_number) FROM parking_violations_2017 WHERE street_code1 IS NULL OR street_code2 IS NULL OR street_code3 IS NULL OR street_code1=0 OR street_code2=0 OR street_code3=0;

CREATE TABLE IF NOT EXISTS parking_violations_2017_part2 AS SELECT A.summons_number, A.violation_code, A.violation_description, A.violation_time, A.violation_time_formatted, from_unixtime(unix_timestamp(concat(A.issue_date,' ',A.violation_time_formatted), 'MM/dd/yyyy hh:mm')) as violation_timestamp_formatted, CASE WHEN HOUR(from_unixtime(unix_timestamp(concat(A.issue_date,' ',A.violation_time_formatted), 'MM/dd/yyyy hh:mm'))) BETWEEN 6 AND 09 THEN 'Morning Slot - 06:00AM to 10:00AM' WHEN HOUR(from_unixtime(unix_timestamp(concat(A.issue_date,' ',A.violation_time_formatted), 'MM/dd/yyyy hh:mm'))) BETWEEN 10 AND 13 THEN 'Mid Day Slot - 10:00AM to 02:00PM' WHEN HOUR(from_unixtime(unix_timestamp(concat(A.issue_date,' ',A.violation_time_formatted), 'MM/dd/yyyy hh:mm'))) BETWEEN 14 AND 17 THEN 'Afternoon Slot - 02:00PM to 06:00PM' WHEN HOUR(from_unixtime(unix_timestamp(concat(A.issue_date,' ',A.violation_time_formatted), 'MM/dd/yyyy hh:mm'))) BETWEEN 18 AND 21 THEN 'Evening Slot - 06:00PM to 10:00PM' WHEN HOUR(from_unixtime(unix_timestamp(concat(A.issue_date,' ',A.violation_time_formatted), 'MM/dd/yyyy hh:mm'))) BETWEEN 22 AND 23 THEN 'Night Slot - 10:00PM to 02:00AM' WHEN HOUR(from_unixtime(unix_timestamp(concat(A.issue_date,' ',A.violation_time_formatted), 'MM/dd/yyyy hh:mm'))) BETWEEN 0 AND 1 THEN 'Night Slot - 10:00PM to 02:00AM' WHEN HOUR(from_unixtime(unix_timestamp(concat(A.issue_date,' ',A.violation_time_formatted), 'MM/dd/yyyy hh:mm'))) BETWEEN 2 AND 5 THEN 'Early Morning Slot - 02:00AM to 06:00AM' END as violation_time_bucket, A.issue_date, CASE WHEN MONTH(from_unixtime(unix_timestamp(concat(A.issue_date,' ',A.violation_time_formatted), 'MM/dd/yyyy hh:mm'))) IN (1,2,12) THEN 'Winter' WHEN MONTH(from_unixtime(unix_timestamp(concat(A.issue_date,' ',A.violation_time_formatted), 'MM/dd/yyyy hh:mm'))) IN (3,4,5) THEN 'Spring' WHEN MONTH(from_unixtime(unix_timestamp(concat(A.issue_date,' ',A.violation_time_formatted), 'MM/dd/yyyy hh:mm'))) IN (6,7,8) THEN 'Summer' WHEN MONTH(from_unixtime(unix_timestamp(concat(A.issue_date,' ',A.violation_time_formatted), 'MM/dd/yyyy hh:mm'))) IN (9,10,11) THEN 'Fall' ELSE 'INVALID' END AS issue_date_season FROM (SELECT summons_number, violation_code, violation_description, violation_time, CASE WHEN LENGTH(violation_time)=5 AND SUBSTR(violation_time,5,1) NOT IN ('A', 'P') THEN 'INVALID' WHEN violation_time LIKE '%+%' THEN 'INVALID' WHEN violation_time LIKE '%.%' THEN 'INVALID' WHEN CAST(SUBSTRING(violation_time, 0, 2) AS INT)=0 THEN SUBSTRING(violation_time, 0, 2)|| ":" || SUBSTRING(violation_time, 3, 2) WHEN LENGTH(violation_time)=4 AND CAST(SUBSTRING(violation_time, 0, 2) AS INT)<13 THEN SUBSTRING(violation_time, 0, 2)|| ":" || SUBSTRING(violation_time, 3, 2) WHEN LENGTH(violation_time)=4 AND CAST(SUBSTRING(violation_time, 0, 2) AS INT)>12 THEN CAST((CAST(SUBSTRING(violation_time, 0, 2) AS INT)+ 12) AS STRING)|| ":" || SUBSTRING(violation_time, 3, 2) WHEN CAST(SUBSTRING(violation_time, 0, 2) AS INT)>12 AND CAST(SUBSTRING(violation_time, 0, 2) AS INT)<24 THEN SUBSTRING(violation_time, 0, 2)|| ":" || SUBSTRING(violation_time, 3, 2) WHEN CAST(SUBSTRING(violation_time, 0, 2) AS INT)>23 THEN 'INVALID' WHEN violation_time IS NULL THEN 'INVALID' WHEN violation_time = '' THEN 'INVALID' WHEN violation_time LIKE '%A' THEN SUBSTRING(violation_time, 0, 2)|| ":" || SUBSTRING(violation_time, 3, 2) WHEN violation_time LIKE '12%P' THEN SUBSTRING(violation_time, 0, 2)|| ":" || SUBSTRING(violation_time, 3, 2) ELSE CAST((CAST(SUBSTRING(violation_time, 0, 2) AS INT)+ 12) AS STRING)|| ":" || SUBSTRING(violation_time, 3, 2) END AS violation_time_formatted, issue_date FROM parking_violations_2017)A;

2.1
SELECT CASE WHEN A.hr IS NULL THEN 'INVALID' ELSE A.hr END AS hr, A.cnt FROM(SELECT HOUR(violation_timestamp_formatted) AS hr, COUNT(summons_number) AS cnt FROM parking_violations_2017_part2 GROUP BY HOUR(violation_timestamp_formatted))A ORDER BY CAST(hr AS INT);

2.1.1
SELECT CASE WHEN A.hr IS NULL THEN 'INVALID' ELSE A.hr END AS hr, A.cnt FROM(SELECT HOUR(violation_timestamp_formatted) AS hr, COUNT(summons_number) AS cnt FROM parking_violations_2017_part2 GROUP BY HOUR(violation_timestamp_formatted))A ORDER BY A.cnt DESC;

2.2
SELECT A.violation_time_bucket, A.violation_code, A.cnt FROM (SELECT violation_time_bucket, violation_code, COUNT(summons_number) as cnt, RANK() OVER(PARTITION BY violation_time_bucket ORDER BY COUNT(summons_number) DESC) AS rnk FROM parking_violations_2017_part2 GROUP BY violation_time_bucket, violation_code)A WHERE A.RNK<4 ORDER BY A.violation_time_bucket, A.cnt DESC;

2.3
SELECT B.violation_code, B.violation_time_bucket, B.scnt FROM(SELECT violation_time_bucket, violation_code, COUNT(summons_number) as scnt, RANK() OVER(PARTITION BY violation_code ORDER BY COUNT(summons_number) DESC) AS crnk FROM parking_violations_2017_part2 WHERE violation_code IN (SELECT A.violation_code FROM(SELECT violation_code, RANK() OVER(ORDER BY COUNT(summons_number) DESC) as rnk FROM parking_violations_2017_part2 GROUP BY violation_code)A WHERE A.rnk<4) GROUP BY violation_time_bucket, violation_code)B WHERE B.crnk = 1 ORDER BY violation_code;

2.4.1
SELECT issue_date_season, COUNT(summons_number) as cnt from parking_violations_2017_part2 WHERE issue_date_season<> 'INVALID' GROUP BY issue_date_season;


2.4.2
SELECT A.issue_date_season, A.violation_code, A.cnt FROM(SELECT issue_date_season, violation_code, COUNT(summons_number) as cnt, RANK() OVER(PARTITION BY issue_date_season ORDER BY COUNT(summons_number) DESC) as rnk FROM parking_violations_2017_part2 WHERE issue_date_season <> 'INVALID' GROUP BY issue_date_season, violation_code)A WHERE A.rnk<4;
