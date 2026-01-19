SELECT 'J_EP_Ref_Remittance_Additional_Payee' || ',' || CAST(COUNT(*) AS VARCHAR(20)) || ',' as Source_String from
(
SEL       
(select coalesce(max(Remittance_Additional_Payee_SID),0)  from EDWPBS.REF_REMITTANCE_ADDITIONAL_PAYEE ) +
row_number() over (order by 
Additional_Payee_Id_Qualifier_Code,
Additional_Payee_Id
)as Remittance_Additional_Payee_SID ,-- SID
Additional_Payee_Id_Qualifier_Code,
Additional_Payee_Id,
'E' AS Source_System_Code ,           
Current_Timestamp(0) AS DW_Last_Update_Date_Time 
FROM
(
SEL
Reference_Id_Qualifier1_Code AS Additional_Payee_Id_Qualifier_Code,
Additional_Payee1_Id AS Additional_Payee_Id
FROM edwpbs_staging.remittance_payment
WHERE Coalesce(Reference_Id_Qualifier1_Code,'') IS NOT IN ('') OR Coalesce(Additional_Payee1_Id ,'') IS NOT IN ('') 
UNION
SEL
Reference_Id_Qualifier2_Code AS Additional_Payee_Id_Qualifier_Code ,
Additional_Payee2_Id AS Additional_Payee_Id
FROM edwpbs_staging.remittance_payment
WHERE Coalesce(Reference_Id_Qualifier2_Code,'') IS NOT IN ('') OR Coalesce(Additional_Payee2_Id ,'') IS NOT IN ('') 
UNION
SEL
Reference_Id_Qualifier3_Code AS Additional_Payee_Id_Qualifier_Code,
Additional_Payee3_Id AS Additional_Payee_Id
FROM edwpbs_staging.remittance_payment
WHERE Coalesce(Reference_Id_Qualifier3_Code,'') IS NOT IN ('') OR Coalesce(Additional_Payee3_Id ,'') IS NOT IN ('') 
UNION
SEL
Reference_Id_Qualifier4_Code AS Additional_Payee_Id_Qualifier_Code,
Additional_Payee4_Id AS Additional_Payee_Id
FROM edwpbs_staging.remittance_payment
WHERE Coalesce(Reference_Id_Qualifier4_Code,'') IS NOT IN ('') OR Coalesce(Additional_Payee4_Id ,'') IS NOT IN ('') 
 
) F
)A