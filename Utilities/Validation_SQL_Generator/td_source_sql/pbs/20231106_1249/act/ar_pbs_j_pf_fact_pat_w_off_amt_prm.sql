Select 'PBMAR900-30' || ',' || 
coalesce(Cast(Sum(Write_Off_Amt) As VARCHAR(20)),'0') ||',' as Source_String
From edwpbs.Fact_RCOM_AR_Patient_Level
where Date_Sid = Cast(cast((add_months(current_date, -1) (format 'yyyymm')) as char(6)) as integer)