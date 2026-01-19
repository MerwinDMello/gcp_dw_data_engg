-- First day of the Week
SELECT DATE_TRUNC(CURRENT_DATE(), WEEK(MONDAY)) 

-- First and Last day of the previous month 
SELECT DATE_TRUNC(DATE_SUB(CURRENT_DATE('US/Central'), INTERVAL 1 MONTH),month) as First_Day_Prev_Month, 
LAST_DAY(DATE_SUB(CURRENT_DATE('US/Central'), INTERVAL 1 MONTH)) as Last_Day_Prev_Month

-- Generate Date Array
SELECT EXTRACT(YEAR FROM day) as Year, EXTRACT(WEEK FROM day) + 1 as Week_No, day as Day
FROM UNNEST(
GENERATE_DATE_ARRAY(DATE('2024-01-01'), CURRENT_DATE(), INTERVAL 1 WEEK)
) AS day