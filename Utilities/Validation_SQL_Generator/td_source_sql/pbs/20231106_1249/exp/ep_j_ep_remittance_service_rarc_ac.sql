SELECT 'J_EP_Remittance_Service_RARC' || ',' || CAST(COUNT(*) AS VARCHAR(20)) || ',' as Source_String from
(
SELECT
Service_GUID,
RARC_Qualifier_Code,
RARC_Code,
Audit_Date,
Delete_Ind,
Delete_Date,
Current_Timestamp(0) AS DW_Last_Update_Date_Time,
'E' AS Source_System_Code
from  EDWPBS_STAGING.Remittance_Service_RARC
where Audit_Date = (select max(cast(Audit_Date as date)) from  EDWPBS_STAGING.Remittance_Service_RARC)
GROUP BY 1,2,3,4,5,6,7,8
  )A
