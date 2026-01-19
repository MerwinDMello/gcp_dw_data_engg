SELECT 'J_EP_Ref_Remittance_Corrected_Priority_Payor' || ',' || CAST(COUNT(*) + 1 AS VARCHAR(20)) || ',' as Source_String from
(
SEL       
(SELECT Coalesce(Max(Corrected_Priority_Payor_SID),0)  FROM EDWPBS.Ref_Remittance_Corrected_Priority_Payor ) +
Row_Number() Over (ORDER BY 
Corrected_Priority_Payor_Qualifier_Code,
Corrected_Priority_Payor_Id,
Corrected_Priority_Payor_Name
)AS Corrected_Priority_Payor_SID ,-- SID
Corrected_Priority_Payor_Qualifier_Code,
Corrected_Priority_Payor_Id,
Corrected_Priority_Payor_Name,
'E' AS Source_System_Code ,           
Current_Timestamp(0) AS DW_Last_Update_Date_Time 
FROM
(
SEL
 Corretd_Prior_Payor_Qulifr_Cod AS Corrected_Priority_Payor_Qualifier_Code,
Corrected_Priority_Payor_Num AS Corrected_Priority_Payor_Id ,
NM103_Corretd_Priority_Payr_Nm AS Corrected_Priority_Payor_Name
FROM  EDWPBS_Staging.remittance_claim
where dw_last_update_date_time = (select max(cast(dw_last_update_date_time as date)) from EDWPBS_STAGING.remittance_claim)
and Coalesce(Corretd_Prior_Payor_Qulifr_Cod,'') IS NOT IN ('')  OR Coalesce(Corrected_Priority_Payor_Num,'') IS NOT IN ('')   OR Coalesce(NM103_Corretd_Priority_Payr_Nm,'') IS NOT IN ('')  
GROUP BY 1,2,3
) F 
WHERE 
(Corrected_Priority_Payor_Qualifier_Code,Corrected_Priority_Payor_Id,Corrected_Priority_Payor_Name) NOT IN 
( SEL Corrected_Priority_Payor_Qualifier_Code,Corrected_Priority_Payor_Id,Corrected_Priority_Payor_Name FROM EDWPBS.Ref_Remittance_Corrected_Priority_Payor )
)a ;
