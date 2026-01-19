SELECT 'J_MHB_Ref_MHB_Alert_Type'||','||CAST(Count(*) AS VARCHAR (20))||','
AS SOURCE_STRING FROM
(
select *from
(
sel *
from 
(sel distinct Alert_Title 
 from EDWCI_Staging.vwPatientAlertTracker)X
 where X.Alert_Title not in (sel Alert_Type_Desc from Edwci.Ref_MHB_Alert_Type  )
 )X
) Q