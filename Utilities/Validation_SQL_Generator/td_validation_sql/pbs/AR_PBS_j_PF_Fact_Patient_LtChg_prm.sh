
#  @@START DMEXPRESS_EXPORTED_VARIABLES

export AC_EXP_TOLERANCE_PERCENT=0
export AC_EXP_TOLERANCE_AMT=0
export JOBNAME='PBMAR900-20'
export AC_ACT_SQL_STATEMENT="Locking table edwpbs.Fact_RCOM_AR_Patient_Level for access
Select 'PBMAR900-20' || ',' || coalesce(Cast(Sum(Late_Charge_Credit_Amt) As VARCHAR(20)), '0') || ',' ||
coalesce(Cast(Sum(Late_Charge_Debit_Amt) As VARCHAR(20)) , '0') || ',' AS SOURCE_STRING
From edwpbs.Fact_RCOM_AR_Patient_Level
where Date_Sid = Cast(cast((add_months(current_date, -1) (format 'yyyymm')) as char(6)) as integer)"

export AC_EXP_SQL_STATEMENT="locking table EDWPF.Patient_Suspended_Charge for access
locking table Edwfs.Fact_Facility for access
locking table edwpbs.fact_rcom_ar_patient_level  for access
locking table EDWPF.Patient_Suspended_Charge for access
locking table Edwfs.Fact_Facility for access

Select 'PBMAR900-20' || ',' || EXP1.Late_Charge_Credit_Amt || ','  ||   EXP1.Late_Charge_Debit_Amt || ',' AS SOURCE_STRING from (
SELECT 
  	Add_Months(CAST((CAST(((PSC.Charge_Posted_Date) (FORMAT 'YYYY-MM')) AS CHAR(7)) ||'-01') AS DATE), 1) - 6 AS date1, cast(Sum(Case When PSC.Charge_Amt < 0 Then PSC.Charge_Amt 
             Else 0.00
   End) as varchar(20)) As Late_Charge_Credit_Amt,  
cast(Sum(Case When PSC.Charge_Amt >= 0 Then PSC.Charge_Amt
             Else 0.00
   End) as varchar(20)) As Late_Charge_Debit_Amt  

FROM

EDWPF_Base_Views.Patient_Suspended_Charge  PSC
--- EDWPF.Patient_Suspended_Charge PSC   old expected query

---Join Edwfs.Fact_Facility FF   -  old expected   query modified by Mahesh 
Join Edwfs_Base_Views.Fact_Facility FF

 on FF.COID = PSC.COID
 and FF.Company_Code = PSC.Company_Code

LEFT OUTER JOIN edwpbs_Base_Views.fact_rcom_ar_patient_level fp
ON fp.patient_sid = PSC.pat_acct_num
AND fp.company_code = PSC.company_code
AND fp.coid = PSC.coid
AND CAST(CAST((PSC.Charge_Posted_Date (FORMAT 'yyyymm')) AS CHAR(6))  AS INTEGER)  = fp.date_sid
AND fp.iplan_insurance_order_num = 0

WHERE  cast((PSC.Charge_Posted_Date (format 'yyyymm')) as char(6)) =  cast((add_months(current_date, -1) (format 'yyyymm')) as char(6))
and  PSC.Source_System_Code = 'P'
AND (PSC.Company_Code = 'H' OR substr(Trim(FF.Company_Code_Operations),1,1) = 'Y')
group by 1
) EXP1"

#  @@END DMEXPRESS_EXPORTED_VARIABLES
#  End of Exported variables section
#  


