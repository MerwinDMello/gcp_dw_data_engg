SELECT 'J_MHB_Ref_MHB_Action_Type'||','||CAST(Count(*) AS VARCHAR (20))||','
AS SOURCE_STRING FROM
(
select 
*
from 
(sel  Action as Action_Type_Desc
 from edwci_staging.vwLabOrderAudits
 group by 1
 UNION 
 Sel
 'Photo Viewed' as Action_Type_Desc
from edwci_staging.vwLabOrderAudits
 UNION
 Sel
  'Photo Saved' as Action_Type_Desc
from edwci_staging.vwLabOrderAudits 
) Y where Action_Type_Desc not in  (sel  Action_Type_Desc from Edwci.Ref_MHB_Action_Type )
) Q