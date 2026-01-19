SELECT 'PBMPA100-20' || ',' || 
coalesce(cast(trim(sum(sum1) )as varchar (20)),'0')|| ',' || 
coalesce(cast(trim(sum(sum2 ))as varchar (20)),'0') || ',' || 
coalesce(cast(trim(sum(sum3) )as varchar (20)),'0') || ',' as Source_string from 
(SELECT	SUM (Bad_Debt_Writeoff_Amt) as sum1,SUM(Non_Secn_Self_Pay_AR_Amt) as sum2,SUM(Non_Secn_Unins_Disc_Amt) as sum3 
FROM	edwpbs.Rpt_AR_ADA_Detail x WHERE  Month_Id =  CAST (  ( ADD_MONTHS(CURRENT_DATE, -1 ) (FORMAT 'YYYYMM') )  AS CHAR(6)) ) Src