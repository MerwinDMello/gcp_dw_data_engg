#  @@START DMEXPRESS_EXPORTED_VARIABLES

export AC_EXP_TOLERANCE_PERCENT=0
export AC_EXP_TOLERANCE_AMT=0
export JOBNAME=''PBMAR900-30''

export AC_ACT_SQL_STATEMENT="Select 'PBMAR900-30' || ',' || 
coalesce(Cast(Sum(Write_Off_Amt) As VARCHAR(20)),'0') ||',' as Source_String
From edwpbs.Fact_RCOM_AR_Patient_Level
where Date_Sid = Cast(cast((add_months(current_date, -1) (format 'yyyymm')) as char(6)) as integer)"


export AC_EXP_SQL_STATEMENT="SELECT 'PBMAR900-30' || ',' || 
        coalesce(CAST(SUM(ART.AR_Transaction_Amt) AS VARCHAR(20)),'0') ||',' as Source_string
FROM
         Edwpf.AR_Transaction ART

JOIN Edwfs.Fact_Facility FF
 ON FF.COID = ART.COID
 AND FF.Company_Code = ART.Company_Code
 
 LEFT OUTER JOIN edwpbs.fact_rcom_ar_patient_level fp
ON fp.patient_sid = ART.pat_acct_num
AND fp.company_code = ART.company_code
AND fp.coid = ART.coid
AND CAST(CAST((ART.AR_TRANSACTION_EFFECTIVE_DATE (FORMAT 'yyyymm')) AS CHAR(6))  AS INTEGER)  = fp.date_sid
AND fp.iplan_insurance_order_num = 0

WHERE ART.Transaction_Type_Code = '3'
AND ART.Source_System_Code = 'P'
AND (ART.Company_Code = 'H' OR substr(TRIM(FF.Company_Code_Operations),1,1) = 'Y')
AND ART.AR_TRANSACTION_ENTER_DATE BETWEEN                                                
ADD_MONTHS(CAST((CAST((ADD_MONTHS(CURRENT_DATE, - 0) (FORMAT 'YYYY-MM')) AS CHAR(7)) ||'-01') AS DATE) , -1) 
AND    (CAST((CAST((ADD_MONTHS(CURRENT_DATE, - 0) (FORMAT 'YYYY-MM')) AS CHAR(7)) ||'-01') AS DATE) +3)

AND ART.AR_TRANSACTION_EFFECTIVE_DATE BETWEEN                                           
ADD_MONTHS(CAST((CAST((ADD_MONTHS(CURRENT_DATE, - 0) (FORMAT 'YYYY-MM')) AS CHAR(7)) ||'-01') AS DATE) , -1) 
AND  (CAST((CAST((ADD_MONTHS(CURRENT_DATE, - 0) (FORMAT 'YYYY-MM')) AS CHAR(7)) ||'-01') AS DATE) -1)
"

#  @@END DMEXPRESS_EXPORTED_VARIABLES
#  End of Exported variables section
#  
