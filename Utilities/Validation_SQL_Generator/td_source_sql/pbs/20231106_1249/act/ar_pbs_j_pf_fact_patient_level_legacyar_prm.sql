Locking table  edwpbs.fact_rcom_ar_patient_level for access
SELECT 
'PBMAR100' || ',' ||
cast(zeroifnull(SUM(ARP.Payor_Payment_Amt)) as varchar(20))  || ',' ||
cast(zeroifnull(SUM(ARP.AR_Patient_Amt)) as varchar(20))  || ',' ||
cast(zeroifnull(SUM(ARP.AR_Insurance_Amt)) as varchar(20))  || ',' ||
cast(zeroifnull(SUM(ARP.Gross_Charge_Amt)) as varchar(20))  || ',' ||
cast(zeroifnull(SUM(ARP.Payor_Contractual_Amt)) as varchar(20))  || ',' AS SOURCE_STRING
FROM
 edwpbs.fact_rcom_ar_patient_level ARP
Where 
ARP.Date_Sid = cast((add_months(current_date, -1)(Format 'yyyymm')) as char(6))
AND ARP.Patient_SID <>0 and Source_Sid=9
AND ARP.Coid in (select co_id from edwpbs_staging.pass_current group by 1)
