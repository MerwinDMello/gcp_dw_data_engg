SELECT 
'PBMAR095' || ',' || cast(zeroifnull(count(*)) as varchar(20)) || ','   AS SOURCE_STRING
FROM Edwpbs_Staging.Pass_Current_PF PC where rptg_Period = cast ( ( add_months(current_Date,-1) (format 'YYYYMM')) as Char(6)) 
and DW_Last_Update_Date_Time in 
(select max (DW_Last_Update_Date_Time) from Edwpbs_Staging.Pass_Current_PF)