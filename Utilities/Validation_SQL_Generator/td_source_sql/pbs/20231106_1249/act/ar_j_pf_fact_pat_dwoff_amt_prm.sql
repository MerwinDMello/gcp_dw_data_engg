Locking table edwpbs.Fact_RCOM_AR_patient_Level for access
Select 'PBMAR380' || ',' || coalesce(Cast(Sum(Payor_Denial_Amt) As VARCHAR(20)) , '0') || ',' || 
coalesce(cast(sum(Payor_Denial_Cnt)  as varchar(20)) , '0') || ','AS SOURCE_STRING
From edwpbs.Fact_RCOM_AR_patient_Level
where Date_Sid = Cast(cast((add_months(current_date, -1) (format 'yyyymm')) as char(6)) as integer)