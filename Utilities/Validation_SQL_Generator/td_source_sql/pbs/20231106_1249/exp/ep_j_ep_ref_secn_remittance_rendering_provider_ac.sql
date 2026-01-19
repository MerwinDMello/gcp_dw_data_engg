SELECT 'J_EP_Ref_Secn_Remittance_Rendering_Provider' || ',' || CAST(COUNT(*) AS VARCHAR(20)) || ',' as Source_String from
(
SEL       
(SELECT Coalesce(Max(Remittance_Secn_Rendering_Provider_SID),0)  FROM EDWPBS.Ref_Secn_Remittance_Rendering_Provider ) +
Row_Number() Over (ORDER BY 
Secn_Rendering_Provider_Id_Qlfr_Code,
Secn_Rendering_Provider_Id
)AS Remittance_Secn_Rendering_Provider_SID ,-- SID
Secn_Rendering_Provider_Id_Qlfr_Code,
Secn_Rendering_Provider_Id,
'E' AS Source_System_Code ,           
Current_Timestamp(0) AS DW_Last_Update_Date_Time 
FROM
(
SEL
Rndrng_Prvdr_Ref_Idn_Qul_1_Cod AS Secn_Rendering_Provider_Id_Qlfr_Code,
Rendering_Provder_Secndary_Id1 AS Secn_Rendering_Provider_Id
FROM  EDWPBS_Staging.remittance_claim
WHERE Coalesce(Rndrng_Prvdr_Ref_Idn_Qul_1_Cod,'') IS NOT IN ('') OR Coalesce(Rendering_Provder_Secndary_Id1 ,'') IS NOT IN ('') 
UNION
SEL
Rndrng_Prvdr_Ref_Idn_Qul_2_Cod AS Secn_Rendering_Provider_Id_Qlfr_Code,
Rendering_Provder_Secndary_Id2 AS Secn_Rendering_Provider_Id
FROM  EDWPBS_Staging.remittance_claim
WHERE Coalesce(Rndrng_Prvdr_Ref_Idn_Qul_2_Cod,'') IS NOT IN ('') OR Coalesce(Rendering_Provder_Secndary_Id2 ,'') IS NOT IN ('') 
 
UNION
SEL
Rndrng_Prvdr_Ref_Idn_Qul_3_Cod AS Secn_Rendering_Provider_Id_Qlfr_Code,
Rendering_Provder_Secndary_Id3 AS Secn_Rendering_Provider_Id
FROM  EDWPBS_Staging.remittance_claim
WHERE Coalesce(Rndrng_Prvdr_Ref_Idn_Qul_3_Cod,'') IS NOT IN ('') OR Coalesce(Rendering_Provder_Secndary_Id3 ,'') IS NOT IN ('') 
 
UNION
SEL
Rndrng_Prvdr_Ref_Idn_Qul_4_Cod AS Secn_Rendering_Provider_Id_Qlfr_Code,
Rendering_Provder_Secndary_Id4 AS Secn_Rendering_Provider_Id
FROM  EDWPBS_Staging.remittance_claim
WHERE Coalesce(Rndrng_Prvdr_Ref_Idn_Qul_4_Cod,'') IS NOT IN ('') OR Coalesce(Rendering_Provder_Secndary_Id4 ,'') IS NOT IN ('') 
 
UNION
SEL
Rndrng_Prvdr_Ref_Idn_Qul_5_Cod AS Secn_Rendering_Provider_Id_Qlfr_Code,
Rendering_Provder_Secndary_Id5 AS Secn_Rendering_Provider_Id
FROM  EDWPBS_Staging.remittance_claim
WHERE Coalesce(Rndrng_Prvdr_Ref_Idn_Qul_5_Cod,'') IS NOT IN ('') OR Coalesce(Rendering_Provder_Secndary_Id5 ,'') IS NOT IN ('') 
 
UNION
SEL
Rndrng_Prvdr_Ref_Idn_Qul_6_Cod AS Secn_Rendering_Provider_Id_Qlfr_Code,
Rendering_Provder_Secndary_Id6 AS Secn_Rendering_Provider_Id
FROM  EDWPBS_Staging.remittance_claim
WHERE Coalesce(Rndrng_Prvdr_Ref_Idn_Qul_6_Cod,'') IS NOT IN ('') OR Coalesce(Rendering_Provder_Secndary_Id6 ,'') IS NOT IN ('') 
 
UNION
SEL
Rndrng_Prvdr_Ref_Idn_Qul_7_Cod AS Secn_Rendering_Provider_Id_Qlfr_Code,
Rendering_Provder_Secndary_Id7 AS Secn_Rendering_Provider_Id
FROM  EDWPBS_Staging.remittance_claim
WHERE Coalesce(Rndrng_Prvdr_Ref_Idn_Qul_7_Cod,'') IS NOT IN ('') OR Coalesce(Rendering_Provder_Secndary_Id7 ,'') IS NOT IN ('') 
 
UNION
SEL
Rndrng_Prvdr_Ref_Idn_Qul_8_Cod AS Secn_Rendering_Provider_Id_Qlfr_Code,
Rendering_Provder_Secndary_Id8 AS Secn_Rendering_Provider_Id
FROM  EDWPBS_Staging.remittance_claim
WHERE Coalesce(Rndrng_Prvdr_Ref_Idn_Qul_8_Cod,'') IS NOT IN ('') OR Coalesce(Rendering_Provder_Secndary_Id8 ,'') IS NOT IN ('') 
 
UNION
SEL
Rndrng_Prvdr_Ref_Idn_Qul_9_Cod AS Secn_Rendering_Provider_Id_Qlfr_Code,
Rendering_Provder_Secndary_Id9 AS Secn_Rendering_Provider_Id
FROM  EDWPBS_Staging.remittance_claim
WHERE Coalesce(Rndrng_Prvdr_Ref_Idn_Qul_9_Cod,'') IS NOT IN ('') OR Coalesce(Rendering_Provder_Secndary_Id9 ,'') IS NOT IN ('') 
 
UNION
SEL
Rndrng_Prvdr_Rf_Idn_Qul_10_Cod AS Secn_Rendering_Provider_Id_Qlfr_Code,
Rendering_Provdr_Secndary_Id10 AS Secn_Rendering_Provider_Id
FROM  EDWPBS_Staging.remittance_claim
WHERE Coalesce(Rndrng_Prvdr_Rf_Idn_Qul_10_Cod,'') IS NOT IN ('') OR Coalesce(Rendering_Provdr_Secndary_Id10 ,'') IS NOT IN ('') 
 ) F)A ;
