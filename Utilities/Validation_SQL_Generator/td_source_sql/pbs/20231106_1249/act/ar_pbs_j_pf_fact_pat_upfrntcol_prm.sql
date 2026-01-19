Locking table edwpbs.Fact_RCOM_AR_Patient_Level for access
Select 'PBMAR900-010'||','||
COALESCE(Cast(Sum(Payor_Up_Front_Collection_Amt) As VARCHAR(20)),'0')||',' AS SOURCE_STRING
From edwpbs.Fact_RCOM_AR_Patient_Level
where Date_Sid = Cast(cast((add_months(current_date, -1) (format 'yyyymm')) as char(6)) as integer)