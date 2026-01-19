SELECT 'Ref_Secn_Remittance_Rendering_Provider_service' || ',' || CAST(COUNT(*) AS VARCHAR(20)) || ',' as Source_String from
( 
SEL
rendrng_provdr_ref_Idn_Qual1 AS Secn_Rendering_Provider_Id_Qlfr_Code,
Rendering_Provider_Identifier1 AS Secn_Rendering_Provider_Id
FROM  EDWPBS_Staging.remittance_service
WHERE Coalesce(rendrng_provdr_ref_Idn_Qual1,'') IS NOT IN ('') OR Coalesce(Rendering_Provider_Identifier1 ,'') IS NOT IN ('') 
UNION
SEL
rendrng_provdr_ref_Idn_Qual2 AS Secn_Rendering_Provider_Id_Qlfr_Code,
Rendering_Provider_Identifier2 AS Secn_Rendering_Provider_Id
FROM  EDWPBS_Staging.remittance_service
WHERE Coalesce(rendrng_provdr_ref_Idn_Qual2,'') IS NOT IN ('') OR Coalesce(Rendering_Provider_Identifier2 ,'') IS NOT IN ('') 
UNION
SEL
rendrng_provdr_ref_Idn_Qual3 AS Secn_Rendering_Provider_Id_Qlfr_Code,
Rendering_Provider_Identifier3 AS Secn_Rendering_Provider_Id
FROM  EDWPBS_Staging.remittance_service
WHERE Coalesce(rendrng_provdr_ref_Idn_Qual3,'') IS NOT IN ('') OR Coalesce(Rendering_Provider_Identifier3 ,'') IS NOT IN ('') 
UNION
SEL
rendrng_provdr_ref_Idn_Qual4 AS Secn_Rendering_Provider_Id_Qlfr_Code,
Rendering_Provider_Identifier4 AS Secn_Rendering_Provider_Id
FROM  EDWPBS_Staging.remittance_service
WHERE Coalesce(rendrng_provdr_ref_Idn_Qual4,'') IS NOT IN ('') OR Coalesce(Rendering_Provider_Identifier4 ,'') IS NOT IN ('') 
 
UNION
SEL
rendrng_provdr_ref_Idn_Qual5 AS Secn_Rendering_Provider_Id_Qlfr_Code,
Rendering_Provider_Identifier5 AS Secn_Rendering_Provider_Id
FROM  EDWPBS_Staging.remittance_service
WHERE Coalesce(rendrng_provdr_ref_Idn_Qual5,'') IS NOT IN ('') OR Coalesce(Rendering_Provider_Identifier5 ,'') IS NOT IN ('') 
UNION
SEL
rendrng_provdr_ref_Idn_Qual6 AS Secn_Rendering_Provider_Id_Qlfr_Code,
Rendering_Provider_Identifier6 AS Secn_Rendering_Provider_Id
FROM  EDWPBS_Staging.remittance_service
WHERE Coalesce(rendrng_provdr_ref_Idn_Qual6,'') IS NOT IN ('') OR Coalesce(Rendering_Provider_Identifier6 ,'') IS NOT IN ('') 
UNION
 SEL
rendrng_provdr_ref_Idn_Qual7 AS Secn_Rendering_Provider_Id_Qlfr_Code,
Rendering_Provider_Identifier7 AS Secn_Rendering_Provider_Id
FROM  EDWPBS_Staging.remittance_service
WHERE Coalesce(rendrng_provdr_ref_Idn_Qual7,'') IS NOT IN ('') OR Coalesce(Rendering_Provider_Identifier7 ,'') IS NOT IN ('') 
UNION
SEL
rendrng_provdr_ref_Idn_Qual8 AS Secn_Rendering_Provider_Id_Qlfr_Code,
Rendering_Provider_Identifier8 AS Secn_Rendering_Provider_Id
FROM  EDWPBS_Staging.remittance_service
WHERE Coalesce(rendrng_provdr_ref_Idn_Qual8,'') IS NOT IN ('') OR Coalesce(Rendering_Provider_Identifier8 ,'') IS NOT IN ('') 
 
UNION
SEL
rendrng_provdr_ref_Idn_Qual9 AS Secn_Rendering_Provider_Id_Qlfr_Code,
Rendering_Provider_Identifier9 AS Secn_Rendering_Provider_Id
FROM  EDWPBS_Staging.remittance_service
WHERE Coalesce(rendrng_provdr_ref_Idn_Qual9,'') IS NOT IN ('') OR Coalesce(Rendering_Provider_Identifier9 ,'') IS NOT IN ('') 
UNION
SEL
rendrng_provdr_ref_Idn_Qual10 AS Secn_Rendering_Provider_Id_Qlfr_Code,
Rendering_Providr_Identifier10 AS Secn_Rendering_Provider_Id
FROM  EDWPBS_Staging.remittance_service
WHERE Coalesce(rendrng_provdr_ref_Idn_Qual10,'') IS NOT IN ('') OR Coalesce(Rendering_Providr_Identifier10 ,'') IS NOT IN ('') 
 ) F 
where (coalesce(Secn_Rendering_Provider_Id_Qlfr_Code,''),coalesce(Secn_Rendering_Provider_Id,''))
 not in 
 (select coalesce(Secn_Rendering_Provider_Id_Qlfr_Code,''),coalesce(Secn_Rendering_Provider_Id,'')
 from edwpbs.Ref_Secn_Remittance_Rendering_Provider)
