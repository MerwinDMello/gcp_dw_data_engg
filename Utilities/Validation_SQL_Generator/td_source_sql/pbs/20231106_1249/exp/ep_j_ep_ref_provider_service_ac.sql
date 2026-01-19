SELECT 'J_EP_Ref_Provider_Service' || ',' || CAST(COUNT(*) AS VARCHAR(20)) || ',' as Source_String from
( 
SELECT  ref_idn_qualifier1 AS PROVIDER_SERV_ID_QLFR_CODE , Provider_Identifier1 AS PROVIDER_SERV_ID ,'E' AS SOURCE_SYSTEM_CODE, current_timestamp(0) AS DW_LAST_UPDATE_DATE_TIME
FROM EDWPBS_STAGING.Remittance_Service
where  COALESCE(ref_idn_qualifier1,'') <> '' or coalesce(Provider_Identifier1,'')<> '' 
UNION 
SELECT  ref_idn_qualifier2 AS PROVIDER_SERV_ID_QLFR_CODE , Provider_Identifier2 AS PROVIDER_SERV_ID ,'E' AS SOURCE_SYSTEM_CODE, current_timestamp(0) AS DW_LAST_UPDATE_DATE_TIME
FROM EDWPBS_STAGING.Remittance_Service
where  COALESCE(ref_idn_qualifier2,'') <> '' or coalesce(Provider_Identifier2,'')<> ''
UNION 
SELECT  ref_idn_qualifier3 AS PROVIDER_SERV_ID_QLFR_CODE , Provider_Identifier3 AS PROVIDER_SERV_ID ,'E' AS SOURCE_SYSTEM_CODE, current_timestamp(0) AS DW_LAST_UPDATE_DATE_TIME
FROM EDWPBS_STAGING.Remittance_Service
where  COALESCE(ref_idn_qualifier3,'') <> '' or coalesce(Provider_Identifier3,'')<> ''
UNION 
SELECT  ref_idn_qualifier4 AS PROVIDER_SERV_ID_QLFR_CODE , Provider_Identifier4 AS PROVIDER_SERV_ID ,'E' AS SOURCE_SYSTEM_CODE, current_timestamp(0) AS DW_LAST_UPDATE_DATE_TIME
FROM EDWPBS_STAGING.Remittance_Service
where  COALESCE(ref_idn_qualifier4,'') <> '' or coalesce(Provider_Identifier4,'')<> ''
UNION 
SELECT  ref_idn_qualifier5 AS PROVIDER_SERV_ID_QLFR_CODE , Provider_Identifier5 AS PROVIDER_SERV_ID ,'E' AS SOURCE_SYSTEM_CODE, current_timestamp(0) AS DW_LAST_UPDATE_DATE_TIME
FROM EDWPBS_STAGING.Remittance_Service
where  COALESCE(ref_idn_qualifier5,'') <> '' or coalesce(Provider_Identifier5,'')<> ''
UNION 
SELECT  ref_idn_qualifier6 AS PROVIDER_SERV_ID_QLFR_CODE , Provider_Identifier6 AS PROVIDER_SERV_ID ,'E' AS SOURCE_SYSTEM_CODE, current_timestamp(0) AS DW_LAST_UPDATE_DATE_TIME
FROM EDWPBS_STAGING.Remittance_Service
where  COALESCE(ref_idn_qualifier6,'') <> '' or coalesce(Provider_Identifier6,'')<> ''
UNION 
SELECT  ref_idn_qualifier7 AS PROVIDER_SERV_ID_QLFR_CODE , Provider_Identifier7 AS PROVIDER_SERV_ID ,'E' AS SOURCE_SYSTEM_CODE, current_timestamp(0) AS DW_LAST_UPDATE_DATE_TIME
FROM EDWPBS_STAGING.Remittance_Service
where  COALESCE(ref_idn_qualifier7,'') <> '' or coalesce(Provider_Identifier7,'')<> ''
UNION 
SELECT  ref_idn_qualifier8 AS PROVIDER_SERV_ID_QLFR_CODE , Provider_Identifier8 AS PROVIDER_SERV_ID ,'E' AS SOURCE_SYSTEM_CODE, current_timestamp(0) AS DW_LAST_UPDATE_DATE_TIME
FROM EDWPBS_STAGING.Remittance_Service
where  COALESCE(ref_idn_qualifier8,'') <> '' or coalesce(Provider_Identifier8,'')<> ''
) a
