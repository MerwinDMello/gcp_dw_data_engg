SELECT 'J_EP_Ref_Remittance_Payee' || ',' || CAST(COUNT(*) AS VARCHAR(20)) || ',' as Source_String from
(
SELECT 
(SELECT Coalesce(Max(Remittance_Payee_SID),0)  FROM EDWPBS.REF_REMITTANCE_PAYEE) +
Row_Number() Over (ORDER BY 
Provider_NPI,
PROVIDER_TAX_ID,
Payee_Name ,                
Payee_Identification_Qualifier_Code,
Payee_City_Name ,          
Payee_State_Code ,         
Payee_Postal_Zone_Code ,
Provider_Tax_Id_Lookup_Code
)AS Remittance_Payee_SID,
 /*
CASE WHEN Length(Cast(STG.PROVIDER_TAX_ID AS VARCHAR(100))) = 10 THEN STG.PROVIDER_TAX_ID ELSE NULL end AS Provider_NPI,
  CASE WHEN Length(Cast(STG.PROVIDER_TAX_ID AS VARCHAR(100) )) = 9 THEN STG.PROVIDER_TAX_ID 
  WHEN Length(Cast(STG.PROVIDER_TAX_ID AS VARCHAR(100) )) = 8 THEN '0'||Cast(STG.PROVIDER_TAX_ID AS VARCHAR(100))
 */
 
  CASE WHEN Length(Trim(STG.PROVIDER_TAX_ID )) = 10 THEN Trim(STG.PROVIDER_TAX_ID) ELSE NULL end AS Provider_NPI,
  CASE WHEN Length(Trim(STG.PROVIDER_TAX_ID )) = 9 THEN Trim(STG.PROVIDER_TAX_ID )
  WHEN Length(Trim(STG.PROVIDER_TAX_ID)) = 8 THEN '0'||Trim(STG.PROVIDER_TAX_ID)
  
  
  ELSE
  NULL end AS Provider_Tax_Id,              
STG.Payee_Name AS Payee_Name,                    
STG.Payee_Identification_Qual_Code AS Payee_Identification_Qualifier_Code,
STG.Payee_City_Name AS Payee_City_Name,          
STG.Payee_State_Code AS Payee_State_Code ,            
STG.Payee_Postal_Zone_Code AS Payee_Postal_Zone_Code,   
'E' AS Source_System_Code ,    
Current_Timestamp(0) AS DW_Last_Update_Date_Time,
STG.Provider_Tax_Id_Lookup_Code AS Provider_Tax_Id_Lookup_Code
FROM EDWPBS_staging.remittance_payment STG where DW_Last_Update_Date_Time =(Select Max(Cast(DW_Last_Update_Date_Time AS DATE))  from EDWPBS_staging.remittance_payment)
 
AND (Payee_Name ,                
Payee_Identification_Qualifier_Code,
Payee_City_Name ,          
Payee_State_Code ,         
Payee_Postal_Zone_Code,Provider_Tax_Id_Lookup_Code
,CASE WHEN Length(Trim(PROVIDER_TAX_ID )) = 10 THEN  Trim(PROVIDER_TAX_ID) ELSE Coalesce(NULL,'') end 
,CASE WHEN Length(Trim(PROVIDER_TAX_ID )) = 9 THEN Trim(PROVIDER_TAX_ID )
WHEN Length(Trim(PROVIDER_TAX_ID)) = 8 THEN '0'||Trim(PROVIDER_TAX_ID)
 ELSE Coalesce(NULL,'') end
) NOT IN 
( SELECT DISTINCT Payee_Name ,                
Payee_Identification_Qualifier_Code,
Payee_City_Name ,          
Payee_State_Code ,         
Payee_Postal_Zone_Code,Provider_Tax_Id_Lookup_Code
,Coalesce(Provider_NPI,''),Coalesce(PROVIDER_TAX_ID,'') 
FROM EDWPBS.REF_REMITTANCE_PAYEE a)
GROUP BY 
Provider_NPI,
Provider_Tax_Id,               
Payee_Name ,                
Payee_Identification_Qualifier_Code,
Payee_City_Name ,          
Payee_State_Code ,         
Payee_Postal_Zone_Code ,
Provider_Tax_Id_Lookup_Code
)a
