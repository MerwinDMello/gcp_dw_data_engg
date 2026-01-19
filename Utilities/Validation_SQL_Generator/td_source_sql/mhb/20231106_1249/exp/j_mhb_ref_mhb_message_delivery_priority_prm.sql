SELECT '$JOBNAME'||','||CAST(Count(*) AS VARCHAR (20))||','
AS SOURCE_STRING FROM 
(
SELECT
ROW_NUMBER() over (order by  Message_Delivery_Priority) + (SELECT COALESCE(MAX(Message_Delivery_Priority_SID),0)  FROM EDWCI.Ref_MHB_Message_Delivery_Priority) as Message_Delivery_Priority_SID,
Message_Delivery_Priority as Message_Delivery_Priority_Desc,
'B' AS Source_System_Code,
Current_Timestamp(0) as DW_Last_Update_Date_Time
FROM 
(SELECT Distinct 
Message_Delivery_Priority
from EDWCI_Staging.vwWCTPInboundMessages
where Message_Delivery_Priority NOT IN 
(SELECT Message_Delivery_Priority_Desc FROM EDWCI.Ref_MHB_Message_Delivery_Priority) 
) A) WRK
