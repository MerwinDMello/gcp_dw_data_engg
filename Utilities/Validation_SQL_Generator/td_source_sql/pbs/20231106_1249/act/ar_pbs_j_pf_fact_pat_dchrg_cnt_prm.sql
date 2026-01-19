Locking table edwpbs.Fact_RCOM_AR_Patient_Level for access
select 'PBMAR900-60' ||','|| trim(zeroifnull(sum(Billed_Patient_cnt))) ||','|| 
trim(sum(cast(discharge_to_billing_day_cnt as decimal(20,2)))) ||',' as source_string
from edwpbs.Fact_RCOM_AR_Patient_Level 
where date_sid = Cast(cast((add_months(Current_Date, -1) (format 'YYYYMM')) as char(6)) as integer)