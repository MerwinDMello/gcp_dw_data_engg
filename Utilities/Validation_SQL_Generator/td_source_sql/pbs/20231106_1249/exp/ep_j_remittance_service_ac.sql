SELECT 'J_Remittance_Service' || ',' || CAST(COUNT(*) AS VARCHAR(20)) || ',' as Source_String from
(
SELECT 
rs.Service_GUID AS SERVICE_GUID,
rs.Claim_GUID AS CLAIM_GUID,
rs.Audit_Date AS AUDIT_DATE,
rs.Delete_Ind  ,
rs.Delete_Date  ,
coalesce(rc.COID,'') AS COID,
coalesce(rc.COMPANY_CODE,'') AS COMPANY_CODE,
rs.Charge_Amt AS CHARGE_AMT,
rs.Payment_Amt AS Payment_Amt,
rs.Coinsurance_Amt AS Coinsurance_Amt,
rs.Deductible_Amt AS DEDUCTIBLE_AMT,
rs.Adjudicated_HCPCS_Code AS ADJUDICATED_HCPCS_CODE,
rs.Submitted_HCPCS_Code AS SUBMITTED_HCPCS_CODE,
rs.Procedure_6_Desc_7 AS SUBMITTED_HCPCS_CODE_DESC,
rs.REVENUE_CODE AS Payor_Sent_Revenue_Code,
rs.Adjudicated_HIPPS_Code AS ADJUDICATED_HIPPS_CODE,
rs.SUBMITTED_HIPPS_CODE AS SUBMITTED_HIPPS_CODE,
rs.APC_CODE AS APC_CODE,
rs.APC_AMT AS APC_AMT,
rs.Quantity AS ADJUDICATED_SERVICE_QTY,
rs.Original_Quantity_Cnt AS SUBMITTED_SERVICE_QTY,
rs.HCA_Category AS SERVICE_CATEGORY_CODE,
rs.Date_Time_Qualifier1 AS DATE_TIME_QUALIFIER_CODE_1,
To_Date(CASE  
                                                WHEN Length(SERVICE_DATE1)>0 THEN 
                                                                CASE  
                                                                                WHEN Length(StrTok(SERVICE_DATE1, '/', 1)) = 1 AND Length(StrTok(SERVICE_DATE1, '/', 2)) = 1 THEN StrTok(SERVICE_DATE1,'/', 3)||'/0'||StrTok(SERVICE_DATE1, '/', 1)||'/0'||StrTok(SERVICE_DATE1,'/', 2)
                                                                                WHEN Length(StrTok(SERVICE_DATE1, '/', 1)) = 1 THEN StrTok(SERVICE_DATE1,'/', 3)||'/0'||StrTok(SERVICE_DATE1, '/', 1)||'/'||StrTok(SERVICE_DATE1,'/', 2)
                                                                                WHEN Length(StrTok(SERVICE_DATE1, '/', 2)) = 1 THEN StrTok(SERVICE_DATE1,'/', 3)||'/'||StrTok(SERVICE_DATE1, '/', 1)||'/0'||StrTok(SERVICE_DATE1,'/', 2)
                                                                                ELSE StrTok(SERVICE_DATE1,'/', 3)||'/'||StrTok(SERVICE_DATE1, '/', 1)||'/'||StrTok(SERVICE_DATE1,'/', 2)
                                                                END 
                                                ELSE NULL
                               END, 'YYYY/MM/DD') AS  SERVICE_DATE_1,
--SERVICE_DATE1 as SERVICE_DATE_1,
Date_Time_Qualifier2 AS DATE_TIME_QUALIFIER_CODE_2,
To_Date(CASE  
                                                WHEN Length(SERVICE_DATE2)>0 THEN 
                                                                CASE  
                                                                                WHEN Length(StrTok(SERVICE_DATE2, '/', 1)) = 1 AND Length(StrTok(SERVICE_DATE2, '/', 2)) = 1 THEN StrTok(SERVICE_DATE2,'/', 3)||'/0'||StrTok(SERVICE_DATE2, '/', 1)||'/0'||StrTok(SERVICE_DATE2,'/', 2)
                                                                                WHEN Length(StrTok(SERVICE_DATE2, '/', 1)) = 1 THEN StrTok(SERVICE_DATE2,'/', 3)||'/0'||StrTok(SERVICE_DATE2, '/', 1)||'/'||StrTok(SERVICE_DATE2,'/', 2)
                                                                                WHEN Length(StrTok(SERVICE_DATE2, '/', 2)) = 1 THEN StrTok(SERVICE_DATE2,'/', 3)||'/'||StrTok(SERVICE_DATE2, '/', 1)||'/0'||StrTok(SERVICE_DATE2,'/', 2)
                                                                               ELSE StrTok(SERVICE_DATE2,'/', 3)||'/'||StrTok(SERVICE_DATE2, '/', 1)||'/'||StrTok(SERVICE_DATE2,'/', 2)
                                                                END 
                                                ELSE NULL
                                END, 'YYYY/MM/DD') AS  SERVICE_DATE_2,
--SERVICE_DATE2 as SERVICE_DATE_2,
Current_Timestamp(0) AS DW_LAST_UPDATE_DATE_TIME,
'E' AS SOURCE_SYSTEM_CODE
FROM edwpbs_staging.remittance_service rs
LEFT JOIN edwpbs.remittance_claim  rc
ON rc.claim_guid =rs.claim_guid
 
WHERE rs.DW_Last_Update_Date_Time =(SELECT Max(Cast(DW_Last_Update_Date_Time AS DATE)) FROM EDWPBS_staging.Remittance_Service)
) a
