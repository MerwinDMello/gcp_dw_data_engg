LOCKING TABLE Edwpf.AR_Transaction FOR ACCESS
LOCKING TABLE Edwfs.Fact_Facility FOR ACCESS
LOCKING TABLE edwpf.Admission FOR ACCESS
LOCKING TABLE edwpf.Admission_Discharge FOR ACCESS
LOCKING TABLE edwpf.Registration_Account_Status FOR ACCESS
select 'PBMAR900-010'||','||
COALESCE(SUM1,'0')||',' AS SOURCE_STRING FROM
(SELECT 
	 ADD_MONTHS(CAST((CAST(((ART.AR_TRANSACTION_EFFECTIVE_DATE) (FORMAT 'YYYY-MM')) AS CHAR(7)) ||'-01') AS DATE), 1) - 1 EFFECTIVE_DATE, 
       CAST(SUM(ART.AR_Transaction_Amt) AS VARCHAR(22)) AS SUM1
             
FROM	EDWPF.AR_Transaction ART
JOIN	Edwfs.Fact_Facility FF
ON		FF.COID = ART.COID AND FF.Company_Code = ART.Company_Code
JOIN 	EDWPF.Registration_Account_Status STS
ON  	ART.Patient_DW_ID = STS.Patient_DW_ID
LEFT OUTER JOIN EDWPF.Admission_Discharge AD
ON STS.Patient_DW_ID = AD.Patient_DW_ID
LEFT OUTER JOIN edwpbs.Fact_RCOM_AR_Patient_Level FP
ON	 	ART.pat_acct_num = FP.Patient_Sid
AND ART.company_code = FP.Company_Code
AND ART.COID = FP.COID 
AND CAST(CAST((ART.AR_TRANSACTION_EFFECTIVE_DATE (FORMAT 'yyyymm')) AS CHAR(6))  AS INTEGER)  = FP.Date_Sid
AND FP.Iplan_Insurance_order_num = 0
LEFT OUTER JOIN Edwpf.Admission ADM
ON ADM.Patient_DW_ID = ART.Patient_DW_ID
LEFT OUTER JOIN (                                                        
       SELECT     payor_dw_id, payor_id, eff_from_date, eff_to_date                    
       FROM     edwpbs_base_views.rcom_payor_dimension_eom                                  
       ) rpdeb (payor_dw_id, payor_id, eff_from_date, eff_to_date)                 
       ON     ART.payor_dw_id = rpdeb.payor_dw_id                         
       AND     CAST(ADM.Admission_Date AS DATE)  BETWEEN rpdeb.eff_from_date                  
       AND     rpdeb.eff_to_date                                                
LEFT OUTER JOIN (                                                        
       SELECT     payor_dw_id, payor_id, eff_from_date                                 
       FROM     edwpbs_base_views.rcom_payor_dimension_eom                                  
       ) rpde (payor_dw_id, payor_id, eff_from_date)                               
       ON     ART.payor_dw_id = rpde.payor_dw_id                          
       AND     rpde.eff_from_date IS NULL
WHERE  STS.Account_Status_Date = 
	(SELECT MAX(STSM.Account_Status_Date)
         FROM EDWPF.Registration_Account_Status STSM 
         WHERE  STSM.Patient_DW_ID = STS.Patient_DW_ID
         AND STSM.Account_Status_Date <= (CAST((CAST((ADD_MONTHS(CURRENT_DATE, - 0) (FORMAT 'YYYY-MM')) AS CHAR(7)) ||'-01') AS DATE) +3) 
	)
AND ART.Transaction_Type_Code = '1'
AND ART.Iplan_ID = 0
AND (ART.Company_Code = 'H' OR substr(TRIM(FF.Company_Code_Operations),1,1) = 'Y')
AND (ART.AR_TRANSACTION_ENTER_DATE BETWEEN
	   ADD_MONTHS(CAST((CAST((ADD_MONTHS(CURRENT_DATE, -0) (FORMAT 'YYYY-MM')) AS CHAR(7)) ||'-01') AS DATE) , -1) 
	   AND  (CAST((CAST((ADD_MONTHS(CURRENT_DATE, -0) (FORMAT 'YYYY-MM')) AS CHAR(7)) ||'-01') AS DATE) +3) )
AND 
      (ART.AR_TRANSACTION_EFFECTIVE_DATE BETWEEN                                           
       ADD_MONTHS(CAST((CAST((ADD_MONTHS(CURRENT_DATE, - 0) (FORMAT 'YYYY-MM')) AS CHAR(7)) ||'-01') AS DATE) , -1) 
	  AND  (CAST((CAST((ADD_MONTHS(CURRENT_DATE, -0) (FORMAT 'YYYY-MM')) AS CHAR(7)) ||'-01') AS DATE) -1)
	 ) AND STS.Account_Status_Code IN ('AR','UB','CA')
AND (AD.Discharge_Date IS NULL OR (ART.AR_Transaction_Effective_Date <= AD.Discharge_Date + 2))
GROUP BY 1)k