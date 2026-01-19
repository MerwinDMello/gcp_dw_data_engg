SELECT 'J_EP_Remittance_Claim_CARC' || ',' || CAST(COUNT(*) AS VARCHAR(20)) || ',' as Source_String from
(
SELECT
CCC.Claim_GUID,
CCC.Adj_Group_Code,
CCC.CARC_Code,
CCC.Audit_Date,
CCC.Delete_Ind,
CCC.Delete_Date,
C.Coid AS Coid,
C.Company_Code AS Company_Code,
SUM(CCC.Adj_Amt) AS Adj_Amt,
SUM(CCC.Adj_Qty) AS Adj_Qty,
CCC.Adj_Category,
CCC.CC_Adj_Group_Code,
Current_Timestamp(0) AS DW_Last_Update_Date_Time,
'E' AS Source_System_Code
from  EDWPBS_STAGING.Remittance_Claim_CARC CCC
LEFT JOIN Edwpbs_Base_Views.Remittance_Claim C ON C.Claim_GUID = CCC.Claim_GUID
where CCC.Audit_Date = (select max(cast(Audit_Date as date)) from  EDWPBS_STAGING.Remittance_Claim_CARC)
GROUP BY 1,2,3,4,5,6,7,8,11,12,13,14
  )A
