 USE portfolio_1;

-- 1. Remove Duplicates
-- 2 standardize the data
-- 3 Missing Values
-- 4 Remove any columns

-- --------------------------------- CREATING COPY OF DATA -------------------------------------------------------

CREATE TABLE check1
LIKE edtechdata;

SELECT *
FROM check1;

INSERT INTO check1
SELECT *
FROM edtechdata;

SELECT *
FROM check1;
-- ----------------------------- 1. Handling Duplicates ----------------------------------------------------------------

-- 1.1 Checking overall duplicates

-- 1.1.1. Checking duplicates using row num

SELECT *,
ROW_NUMBER()OVER(PARTITION BY client_id, user_id, first_trial_appointment_date, first_payment_date, os, tutor, average_score, homework_done, paywall_paid, school_name, desktop_enter, add_homework_done, call_date, first_visit_date, region, is_big_city) AS Rownum
FROM check1; 

WITH duplicate_CTE AS
(
SELECT *,
ROW_NUMBER()OVER(PARTITION BY client_id, user_id, first_trial_appointment_date, first_payment_date, os, tutor, average_score, homework_done, paywall_paid, school_name, desktop_enter, add_homework_done, call_date, first_visit_date, region, is_big_city) AS Rownum
FROM check1 
)

SELECT *
FROM duplicate_CTE
WHERE Rownum > 1;

-- 1.1.2. Making a new table that has rownum in it 

CREATE TABLE `check2` (
  `client_id` text,
  `user_id` text,
  `first_trial_appointment_date` text,
  `first_payment_date` text,
  `os` text,
  `tutor` text,
  `job` text,
  `task_class` double DEFAULT NULL,
  `average_score` double DEFAULT NULL,
  `homework_done` int DEFAULT NULL,
  `paywall_paid` text,
  `school_name` text,
  `desktop_enter` text,
  `add_homework_done` int DEFAULT NULL,
  `call_date` text,
  `first_visit_date` text,
  `region` text,
  `is_big_city` text,
  `row_num` int
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

INSERT INTO check2
SELECT *, 
ROW_NUMBER()OVER(PARTITION BY client_id, user_id, first_trial_appointment_date, first_payment_date, os, tutor, average_score, homework_done, paywall_paid, school_name, desktop_enter, add_homework_done, call_date, first_visit_date, region, is_big_city)
FROM check1;

SELECT *
FROM check2;

-- 1.1.3. deleting overall duplicates

# no. of overall duplicates

SELECT COUNT(*)
FROM check2
WHERE row_num > 1; 

# there are 73 overall duplicates

SET SQL_SAFE_UPDATES = 0;

DELETE 
FROM check2
WHERE row_num > 1;

# deleted 73 rows of overall duplicates

-- 1.1.4. deleting rows with duplicated userids
# the rule is that latest call_date shoule be kept

# first lets check how many user_ids are duplicated

WITH user_id_duplicate_CTE
AS
(
SELECT *,
ROW_NUMBER()OVER(PARTITION BY User_id) AS dupe
FROM check2
)

SELECT COUNT(*)
FROM user_id_duplicate_CTE
WHERE dupe > 1;


# creating check 3
CREATE TABLE `check3` (
  `client_id` text,
  `user_id` text,
  `first_trial_appointment_date` text,
  `first_payment_date` text,
  `os` text,
  `tutor` text,
  `job` text,
  `task_class` double DEFAULT NULL,
  `average_score` double DEFAULT NULL,
  `homework_done` int DEFAULT NULL,
  `paywall_paid` text,
  `school_name` text,
  `desktop_enter` text,
  `add_homework_done` int DEFAULT NULL,
  `call_date` text,
  `first_visit_date` text,
  `region` text,
  `is_big_city` text,
  `row_num` int DEFAULT NULL,
  `dupes`  int
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

INSERT INTO check3
SELECT *, ROW_NUMBER()OVER(PARTITION BY User_id)
FROM check2;

SELECT *
FROM check3;



-- 1.1.4.1. standardizing the call_date

# first lets check if any date is not in format using REGEXP
# our general date format is yyyy-mm-dd hh:mm:ss
# in regex it is {0-9}[4]-[0-9][2]-{0-9}[2] {0-9}[2]:{0-9}[2]
# we will use ^ to indicate anything that starts with this 

SELECT call_date
FROM check3
WHERE call_date NOT REGEXP'^[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}';

# lets set all these inconsistent values to null

UPDATE check3
SET call_date = NULL
WHERE call_date NOT REGEXP'^[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}';

# converting all other to date format

UPDATE check3
SET call_date = STR_TO_DATE(call_date, '%Y-%m-%d %H:%i:%s');

ALTER TABLE check3
MODIFY COLUMN call_date DATETIME;

DESCRIBE check3;

-- 1.1.4.2. Deleting duplicated user ids based on call date now. only to keep userids with max date


WITH CTE AS
(
SELECT *,
ROW_NUMBER()OVER(PARTITION BY user_id ORDER BY call_date DESC) AS rn
FROM check3
)

SELECT User_id, call_date, rn
FROM CTE;

# lets delete rows with rn > 1 as they are duplicate user ids with lower call date

CREATE TABLE `check4` (
  `client_id` text,
  `user_id` text,
  `first_trial_appointment_date` text,
  `first_payment_date` text,
  `os` text,
  `tutor` text,
  `job` text,
  `task_class` double DEFAULT NULL,
  `average_score` double DEFAULT NULL,
  `homework_done` int DEFAULT NULL,
  `paywall_paid` text,
  `school_name` text,
  `desktop_enter` text,
  `add_homework_done` int DEFAULT NULL,
  `call_date` datetime DEFAULT NULL,
  `first_visit_date` text,
  `region` text,
  `is_big_city` text,
  `row_num` int DEFAULT NULL,
  `dupes` int DEFAULT NULL,
  `rn` int DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;



INSERT INTO check4
SELECT *, ROW_NUMBER()OVER(PARTITION BY user_id ORDER BY call_date DESC) 
FROM check3;


DELETE 
FROM check4
WHERE rn > 1;

# cleaned duplicates of user id

WITH CTE AS
(
SELECT *,
ROW_NUMBER()OVER(PARTITION BY user_id ORDER BY call_date DESC) AS rn1
FROM check4
)
# checking for the duplicates
SELECT User_id, call_date, rn1
FROM CTE
WHERE rn > 1;

-- -------------------- 2. Standardizing the data ---------------------------------------------------------------

-- 2.1. converting all date time columsn to datetime format

SELECT *
FROM check4;

SELECT first_trial_appointment_date,
STR_TO_DATE(first_trial_appointment_date, '%Y-%m-%d %H:%i:%s')
FROM check4;

UPDATE check4
SET first_trial_appointment_date = NULL
WHERE first_trial_appointment_date NOT REGEXP'^[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}'
OR first_trial_appointment_date = '';


UPDATE check4
SET first_trial_appointment_date = STR_TO_DATE(first_trial_appointment_date, '%Y-%m-%d %H:%i:%s')
WHERE first_trial_appointment_date IS NOT NULL;

ALTER TABLE check4
MODIFY COLUMN first_trial_appointment_date DATETIME;


-- 2.2 first payment date
 SELECT first_payment_date
 FROM check4
 WHERE  first_payment_date IS NOT NULL
 AND first_payment_date != '';
 
 UPDATE check4
 SET first_payment_date = NULL
 WHERE first_payment_date = '';
 
 SELECT first_payment_date
 FROM check4
 WHERE  first_payment_date IS NOT NULL;
 
 SELECT first_payment_date, 
 STR_TO_DATE(first_payment_date, '%Y-%m-%d %H:%i:%s')
 FROM check4
 WHERE  first_payment_date IS NOT NULL;
 
 UPDATE check4
 SET first_payment_date = STR_TO_DATE(first_payment_date, '%Y-%m-%d %H:%i:%s')
 WHERE  first_payment_date IS NOT NULL;
 
 ALTER TABLE check4
MODIFY COLUMN first_payment_date DATETIME;

-- 2.3. first_visit_date
 SELECT first_visit_date
 FROM check4
 WHERE  first_visit_date IS NOT NULL
 AND first_visit_date != '';
 
 UPDATE check4
 SET first_visit_date = NULL
 WHERE first_visit_date = '';
 
 SELECT first_visit_date
 FROM check4
 WHERE  first_visit_date IS NOT NULL;
 
 SELECT first_visit_date, 
 STR_TO_DATE(first_visit_date, '%Y-%m-%d %H:%i:%s')
 FROM check4
 WHERE  first_visit_date IS NOT NULL;
 
 UPDATE check4
 SET first_visit_date = STR_TO_DATE(first_visit_date, '%Y-%m-%d %H:%i:%s')
 WHERE  first_visit_date IS NOT NULL;
 
 ALTER TABLE check4
MODIFY COLUMN first_visit_date DATETIME;

-- ------------------------- 3. standardizing categorical fields --------------------------------------------------

-- 3.1. os

SELECT DISTINCT(os)
FROM check4;

#Checking counts of each section
SELECT os, COUNT(*)
FROM check4
GROUP BY os;


# making missing values as other
UPDATE check4
SET os = 'Other'
WHERE os = '';

SELECT os, COUNT(*)
FROM check4
GROUP BY os;

-- 3.2. tutor
# no. of distinct values
SELECT DISTINCT(tutor)
FROM check4;

# no. of values per category
SELECT tutor, COUNT(tutor)
FROM check4
GROUP BY tutor;

# standardizing the columns I am doing now as ongoing, earlier as former, No as unassigned and missing also as unassingned

UPDATE check4
SET tutor = 'Unassigned'
WHERE tutor = '';

UPDATE check4
SET tutor = 'Unassigned'
WHERE tutor = 'No';

UPDATE check4
SET tutor = 'Former'
WHERE tutor = 'Earlier';

UPDATE check4
SET tutor = 'Ongoing'
WHERE tutor = 'I am doing now';

SELECT tutor, COUNT(tutor)
FROM check4
GROUP BY tutor;


-- 3.3 job

SELECT job, COUNT(job)
FROM check4
GROUP BY job;

# Replacing missing values with unknown
UPDATE check4
SET job = 'Unknonwn'
WHERE job = '';

SELECT job, COUNT(job)
FROM check4
GROUP BY job;

-- 3.4. paywall_paid
SELECT paywall_paid, COUNT(paywall_paid)
FROM check4
GROUP BY paywall_paid;

# all good

-- 3.5 school_name

SELECT school_name, COUNT(school_name)
FROM check4
GROUP BY school_name;

# this is all not required data, lets drop this column

ALTER TABLE check4
DROP COLUMN school_name;

DESCRIBE check4;

-- 3.6. desktop_enter
SELECT desktop_enter, COUNT(desktop_enter)
FROM check4
GROUP BY desktop_enter;

#Lets et this GEROJa... to NULL
# first chekc this entry
SELECT *
FROM check4
WHERE desktop_enter = ' GEROJa SOTsIALISTIChESKOGO TRUDA V.S. GRIZODUBOVOJ"""';

UPDATE check4
SET desktop_enter = 'False'
WHERE desktop_enter = ' GEROJa SOTsIALISTIChESKOGO TRUDA V.S. GRIZODUBOVOJ"""';

SELECT desktop_enter, COUNT(desktop_enter)
FROM check4
GROUP BY desktop_enter;

-- 3.7. region
SELECT region, COUNT(*)
FROM check4
GROUP BY region;

# this is a redundant column and a lot is missing too so lets delete it

ALTER TABLE check4
DROP COLUMN region;

DESCRIBE check4;

-- 3.8. is_big_city
SELECT is_big_city, COUNT(*)
FROM check4
GROUP BY is_big_city;

# one is missing, make it false

UPDATE check4
SET is_big_city = 'False'
WHERE is_big_city = '';

SELECT is_big_city, COUNT(*)
FROM check4
GROUP BY is_big_city;


-- ----------------------------------- 4. standardizing numerical Fields ------------------------------------------

-- 4.1. task_class

SELECT task_class, COUNT(*)
FROM check4
GROUP BY task_class
ORDER BY task_class;

SELECT task_class, COUNT(*)
FROM check4
WHERE task_class = '';

DESCRIBE check4;

SELECT MIN(task_class)
FROM check4;

SELECT MAX(task_class)
FROM check4;

#The task class is all filled. no invalid values . no null values. The task class values varies from 1 to 11.

-- 4.2. average_score

# checking null values
SELECT COUNT(*)
FROM check4
WHERE average_score IS NULL;

SELECT COUNT(*)
FROM check4
WHERE average_score = '';

# there are 702 missing values in average score. They should be filled by mean values

SELECT MIN(average_score) AS Min_average_score,
MAX(average_score) AS Max_average_score,
AVG(average_score) AS Mean_average_score
FROM check4;

SELECT AVG(average_score) INTO @a
FROM Check4;

SELECT @a;


UPDATE check4
SET average_score = @a
WHERE average_score IS NULL OR average_score = '';


-- 4.3. homework_done
SELECT homework_done, COUNT(*)
FROM check4
GROUP BY homework_done
ORDER BY homework_done;

SELECT homework_done, COUNT(*)
FROM check4
WHERE homework_done = '';
# homeworkdone is all ok

-- 4.4 add_homework_done


SELECT add_homework_done, COUNT(*)
FROM check4
GROUP BY add_homework_done
ORDER BY add_homework_done;

SELECT add_homework_done, COUNT(*)
FROM check4
WHERE add_homework_done = '';

# add_homework_done is also ok

-- ------------------------------- check4 is the clean table -----------------------------------------------------