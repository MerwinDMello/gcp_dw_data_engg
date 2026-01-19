SELECT '$JOBNAME'||','||CAST(Count(*) AS VARCHAR (20))||','
AS SOURCE_STRING FROM 
(SELECT Distinct 
DestinationName
from EDWCI_Staging.vwWCTPOutboundMessages
where DestinationName NOT IN 
(SELECT WCTP_Destination_Name FROM EDWCI.Ref_MHB_WCTP_Destination_Name) 
) A