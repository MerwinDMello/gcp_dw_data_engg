SELECT 'J_EP_Ref_Remittance_Rendering_Provider' || ',' || CAST(COUNT(*) +1 AS VARCHAR(20)) || ',' as Source_String from
(
SEL       
(SELECT Coalesce(Max(Remittance_Rendering_Provider_SID),0)  FROM EDWPBS.Ref_Remittance_Rendering_Provider ) +
Row_Number() Over (ORDER BY 
Serv_Provider_Enty_Type_Qualifier_Code,
Rendering_Provider_Last_Org_Name,
Rendering_Provider_First_Name,
Rendering_Provider_Middle_Name,
Rendering_Provider_Name_Suffix,
Serv_Provider_Id_Qualifier_Code,
Rendering_Provider_Id
)AS Remittance_Rendering_Provider_SID ,-- SID
Serv_Provider_Enty_Type_Qualifier_Code,
Rendering_Provider_Last_Org_Name,
Rendering_Provider_First_Name,
Rendering_Provider_Middle_Name,
Rendering_Provider_Name_Suffix,
Serv_Provider_Id_Qualifier_Code,
Rendering_Provider_Id,
'E' AS Source_System_Code ,           
Current_Timestamp(0) AS DW_Last_Update_Date_Time 
FROM
(
SEL
Srvce_Prvdr_Enty_Typ_Qulfr_Cod AS Serv_Provider_Enty_Type_Qualifier_Code ,
Rendering_Provider_Last_Org_Nm AS Rendering_Provider_Last_Org_Name ,
Rendering_Provider_First_Name AS Rendering_Provider_First_Name ,
Rendering_Provider_Middle_Name AS Rendering_Provider_Middle_Name,
Rendering_Provider_Name_Suffix AS Rendering_Provider_Name_Suffix ,
Srvce_Prvdr_Idntfctn_Qulfr_Cod AS Serv_Provider_Id_Qualifier_Code,
Rendering_Provider_Id AS Rendering_Provider_Id
FROM  EDWPBS_Staging.remittance_claim
where dw_last_update_date_time = (select max(cast(dw_last_update_date_time as date)) from EDWPBS_STAGING.remittance_claim)
and Coalesce(Srvce_Prvdr_Enty_Typ_Qulfr_Cod,'') IS NOT IN ('')  OR Coalesce(Rendering_Provider_Last_Org_Nm,'') IS NOT IN ('') 
OR Coalesce(Rendering_Provider_First_Name,'') IS NOT IN ('')  OR Coalesce(Rendering_Provider_Middle_Name,'') IS NOT IN ('')  
OR Coalesce(Rendering_Provider_Name_Suffix,'') IS NOT IN ('')  OR Coalesce(Srvce_Prvdr_Idntfctn_Qulfr_Cod,'') IS NOT IN ('')  
OR Coalesce(Rendering_Provider_Id,'') IS NOT IN ('')  
GROUP BY 1,2,3,4,5,6,7
) F 
WHERE 
(coalesce(Serv_Provider_Enty_Type_Qualifier_Code,''),
coalesce(Rendering_Provider_Last_Org_Name,''),
coalesce(Rendering_Provider_First_Name,''),
coalesce(Rendering_Provider_Middle_Name,''),
coalesce(Rendering_Provider_Name_Suffix,''),
coalesce(Serv_Provider_Id_Qualifier_Code,''),
coalesce(Rendering_Provider_Id,'')) NOT IN 
( SEL 
coalesce(Serv_Provider_Enty_Type_Qualifier_Code,''),
coalesce(Rendering_Provider_Last_Org_Name,''),
coalesce(Rendering_Provider_First_Name,''),
coalesce(Rendering_Provider_Middle_Name,''),
coalesce(Rendering_Provider_Name_Suffix,''),
coalesce(Serv_Provider_Id_Qualifier_Code,''),
coalesce(Rendering_Provider_Id,'') FROM EDWPBS.Ref_Remittance_Rendering_Provider  )
) a;
