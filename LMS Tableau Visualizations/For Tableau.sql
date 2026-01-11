
SELECT *
FROM check4;


CREATE TABLE check5
LIKE check4;

INSERT INTO check5
SELECT*
FROM check4;


SELECT *
FROM check5;





-- ------------------------------ Exploratory Data Analysis ---------------------------------------------------
# 1. os vs user count
SELECT os, COUNT(*)
FROM check5
GROUP BY os
ORDER BY 2;

# paid vs not Paid

SELECT paywall_paid, COUNT(*)
FROM check5
GROUP BY paywall_paid;

SELECT paywall_paid, COUNT(*) * 100/ (SELECT COUNT(*) FROM check5) AS Percentage
FROM check5
GROUP BY paywall_paid;

# job_category_distribution

SELECT job, COUNT(*)
FROM check5
GROUP BY job;

SELECT job, COUNT(*) * 100/ (SELECT COUNT(*) FROM check5 WHERE job != 'Unknonwn' ) AS percentage
FROM check5
GROUP BY job
HAVING job  != 'Unknonwn' 
ORDER BY 2;

# coversion Funnel
SELECT COUNT(first_visit_date) AS funnel1, 
COUNT(first_trial_appointment_date) AS funnel2, 
COUNT(first_payment_date) AS funnel3 
FROM check5;


# average score distribution

WITH CTE AS
(
SELECT 
    average_score,
    CASE 
        WHEN average_score < 20 THEN '0–19'
        WHEN average_score < 40 THEN '20–39'
        WHEN average_score < 60 THEN '40–59'
        WHEN average_score < 80 THEN '60–79'
        ELSE '80–100'
    END AS score_bin
FROM check5
)
SELECT score_bin, COUNT(*)
FROM CTE
GROUP BY score_bin
ORDER BY 1;


# Big_city

SELECT is_big_city, COUNT(*)
FROM check5
GROUP BY is_big_city;

SELECT is_big_city, COUNT(*) * 100/ ( SELECT COUNT(*) FROM check5)
FROM check5
GROUP BY is_big_city;


# effect of tutor on avg scores

SELECT tutor, AVG(average_score)
FROM check5
GROUP BY tutor;

# count of tutor on add homework done

SELECT tutor, COUNT(*)
FROM check5
GROUP BY tutor;

