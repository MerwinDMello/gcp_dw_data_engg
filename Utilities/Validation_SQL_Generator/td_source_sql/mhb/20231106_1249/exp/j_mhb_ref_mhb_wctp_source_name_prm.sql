SELECT '$JOBNAME'||','||CAST(Count(*) AS VARCHAR (20))||','
AS SOURCE_STRING FROM 
(
SELECT
ROW_NUMBER() over (order by  Source_Name) + (SELECT COALESCE(MAX(WCTP_Source_Name_SID),0)  FROM EDWCI.Ref_MHB_WCTP_Source_Name) as WCTP_Source_Name_SID,
Source_Name as WCTP_Source_Name,
'B' AS Source_System_Code,
Current_Timestamp(0) as DW_Last_Update_Date_Time
FROM 
(SELECT Distinct 
Source_Name
from EDWCI_Staging.vwWCTPInboundMessages
where Source_Name NOT IN 
(SELECT WCTP_Source_Name FROM EDWCI.Ref_MHB_WCTP_Source_Name) 
) A) WRK
