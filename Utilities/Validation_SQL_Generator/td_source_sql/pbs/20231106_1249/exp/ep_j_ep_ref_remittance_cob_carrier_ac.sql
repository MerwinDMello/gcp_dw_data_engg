SELECT 'J_EP_Ref_Remittance_COB_Carrier' || ',' || CAST(COUNT(*)+1 AS VARCHAR(20)) || ',' as Source_String from
(
SEL       
(SELECT Coalesce(Max(COB_Carrier_SID),0)  FROM EDWPBS.Ref_Remittance_COB_Carrier ) +
Row_Number() Over (ORDER BY 
COB_Qualifier_Code,
COB_Carrier_Num,
COB_Carrier_Name
)AS COB_Carrier_SID ,-- SID
COB_Qualifier_Code,
COB_Carrier_Num,
COB_Carrier_Name,
'E' AS Source_System_Code ,           
Current_Timestamp(0) AS DW_Last_Update_Date_Time 
FROM
(
SEL
Crossover_Carrier_Qualifr_Code AS COB_Qualifier_Code ,
Cordintn_of_Benefit_Carrier_Nm AS COB_Carrier_Num,
Coordintn_Of_Beneft_Carrier_Nm AS COB_Carrier_Name
FROM  EDWPBS_Staging.remittance_claim
where dw_last_update_date_time = (select max(cast(dw_last_update_date_time as date)) from EDWPBS_STAGING.remittance_claim)
and Coalesce(Crossover_Carrier_Qualifr_Code,'') IS NOT IN ('')  OR Coalesce(Coordintn_Of_Beneft_Carrier_Nm,'') IS NOT IN ('')   OR Coalesce(Cordintn_of_Benefit_Carrier_Nm,'') IS NOT IN ('')  
GROUP BY 1,2,3
) F 
WHERE 
(COB_Qualifier_Code,COB_Carrier_Num,COB_Carrier_Name) NOT IN 
( SEL COB_Qualifier_Code,COB_Carrier_Num,COB_Carrier_Name FROM EDWPBS.Ref_Remittance_COB_Carrier  )
) a ;
