Locking table edwpbs.Fact_RCOM_AR_Patient_Level for access
Select 'PBMAR900-20' || ',' || coalesce(Cast(Sum(Late_Charge_Credit_Amt) As VARCHAR(20)), '0') || ',' ||
coalesce(Cast(Sum(Late_Charge_Debit_Amt) As VARCHAR(20)) , '0') || ',' AS SOURCE_STRING
From edwpbs.Fact_RCOM_AR_Patient_Level
where Date_Sid = Cast(cast((add_months(current_date, -1) (format 'yyyymm')) as char(6)) as integer)