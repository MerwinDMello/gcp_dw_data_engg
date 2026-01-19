SELECT '$JOBNAME'||','||CAST(Count(*) AS VARCHAR (20))||','
AS SOURCE_STRING FROM 
(
SELECT
ROW_NUMBER() over (order by  Source_Type) + (SELECT COALESCE(MAX(WCTP_Source_Type_SID),0)  FROM EDWCI.Ref_MHB_WCTP_Source_Type) as WCTP_Source_Type_SID,
Source_Type as WCTP_Source_Type_Desc,
'B' AS Source_System_Code,
Current_Timestamp(0) as DW_Last_Update_Date_Time
FROM 
(SELECT Distinct 
Source_Type
from EDWCI_Staging.vwWCTPInboundMessages
where Source_Type NOT IN 
(SELECT WCTP_Source_Type_Desc FROM EDWCI.Ref_MHB_WCTP_Source_Type) 
) A) A
