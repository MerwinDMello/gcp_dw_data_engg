SELECT 'J_EP_Junc_Remittance_Provider_Serv' || ',' || CAST(COUNT(*) AS VARCHAR(20)) || ',' as Source_String from
( 
sel 
SERVICE_GUID,
PROVIDER_SERV_ID_LINE_NUM,
coalesce(REMITTANCE_PROVIDER_SERV_SID,99999) as REMITTANCE_PROVIDER_SERV_SID,
'E' as SOURCE_SYSTEM_CODE,
Current_timestamp(0) as DW_LAST_UPDATE_DATE_TIME
from 
(
SEL
SERVICE_GUID,
CLAIM_GUID,
1 AS PROVIDER_SERV_ID_LINE_NUM,
ref_idn_qualifier1 as PROVIDER_SERV_ID_QLFR_CODE,
Provider_Identifier1 as PROVIDER_SERV_ID
FROM  EDWPBS_Staging.remittance_service
WHERE DELETE_IND ='N' AND ( Coalesce(ref_idn_qualifier1,'') IS NOT IN ('') OR Coalesce(Provider_Identifier1,'') IS NOT IN ('') )
UNION ALL
SEL
SERVICE_GUID,
CLAIM_GUID,
2 AS PROVIDER_SERV_ID_LINE_NUM,
ref_idn_qualifier2 as PROVIDER_SERV_ID_QLFR_CODE,
Provider_Identifier2 as PROVIDER_SERV_ID
FROM  EDWPBS_Staging.remittance_service
WHERE DELETE_IND ='N' AND ( Coalesce(ref_idn_qualifier2,'') IS NOT IN ('') OR Coalesce(Provider_Identifier2,'') IS NOT IN ('') )
UNION ALL
SEL
SERVICE_GUID,
CLAIM_GUID,
3 AS PROVIDER_SERV_ID_LINE_NUM,
ref_idn_qualifier3 as PROVIDER_SERV_ID_QLFR_CODE,
Provider_Identifier3 as PROVIDER_SERV_ID
FROM  EDWPBS_Staging.remittance_service
WHERE DELETE_IND ='N' AND ( Coalesce(ref_idn_qualifier3,'') IS NOT IN ('') OR Coalesce(Provider_Identifier3,'') IS NOT IN ('') )
UNION ALL
SEL
SERVICE_GUID,
CLAIM_GUID,
4 AS PROVIDER_SERV_ID_LINE_NUM,
ref_idn_qualifier4 as PROVIDER_SERV_ID_QLFR_CODE,
Provider_Identifier4 as PROVIDER_SERV_ID
FROM  EDWPBS_Staging.remittance_service
WHERE DELETE_IND ='N' AND ( Coalesce(ref_idn_qualifier4,'') IS NOT IN ('') OR Coalesce(Provider_Identifier4,'') IS NOT IN ('') )
UNION ALL
SEL
SERVICE_GUID,
CLAIM_GUID,
5 AS PROVIDER_SERV_ID_LINE_NUM,
ref_idn_qualifier5 as PROVIDER_SERV_ID_QLFR_CODE,
Provider_Identifier5 as PROVIDER_SERV_ID
FROM  EDWPBS_Staging.remittance_service
WHERE DELETE_IND ='N' AND ( Coalesce(ref_idn_qualifier5,'') IS NOT IN ('') OR Coalesce(Provider_Identifier5,'') IS NOT IN ('') )
UNION ALL
SEL
SERVICE_GUID,
CLAIM_GUID,
6 AS PROVIDER_SERV_ID_LINE_NUM,
ref_idn_qualifier6 as PROVIDER_SERV_ID_QLFR_CODE,
Provider_Identifier6 as PROVIDER_SERV_ID
FROM  EDWPBS_Staging.remittance_service
WHERE DELETE_IND ='N' AND ( Coalesce(ref_idn_qualifier6,'') IS NOT IN ('') OR Coalesce(Provider_Identifier6,'') IS NOT IN ('') )
UNION ALL
SEL
SERVICE_GUID,
CLAIM_GUID,
7 AS PROVIDER_SERV_ID_LINE_NUM,
ref_idn_qualifier7 as PROVIDER_SERV_ID_QLFR_CODE,
Provider_Identifier7 as PROVIDER_SERV_ID
FROM  EDWPBS_Staging.remittance_service
WHERE DELETE_IND ='N' AND ( Coalesce(ref_idn_qualifier7,'') IS NOT IN ('') OR Coalesce(Provider_Identifier7,'') IS NOT IN ('') )
UNION ALL
SEL
SERVICE_GUID,
CLAIM_GUID,
8 AS PROVIDER_SERV_ID_LINE_NUM,
ref_idn_qualifier8 as PROVIDER_SERV_ID_QLFR_CODE,
Provider_Identifier8 as PROVIDER_SERV_ID
FROM  EDWPBS_Staging.remittance_service
WHERE DELETE_IND ='N' AND ( Coalesce(ref_idn_qualifier8,'') IS NOT IN ('') OR Coalesce(Provider_Identifier8,'') IS NOT IN ('') )
)F
 LEFT OUTER  JOIN EDWPBS_base_views.ref_provider_service RPS ON 
 RPS.PROVIDER_SERV_ID_QLFR_CODE =F.PROVIDER_SERV_ID_QLFR_CODE
and  RPS.PROVIDER_SERV_ID=F.PROVIDER_SERV_ID
) a
