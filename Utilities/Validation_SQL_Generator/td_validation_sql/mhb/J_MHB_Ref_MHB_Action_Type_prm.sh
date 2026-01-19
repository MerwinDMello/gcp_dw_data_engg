 #  @@START DMEXPRESS_EXPORTED_VARIABLES

export AC_EXP_TOLERANCE_PERCENT=0
export AC_EXP_TOLERANCE_AMT=0
export JOBNAME='J_MHB_Ref_MHB_Action_Type'


export AC_ACT_SQL_STATEMENT="SELECT 'J_MHB_Ref_MHB_Action_Type'||','||CAST(Count(*) AS VARCHAR (20))||','

AS SOURCE_STRING FROM
(
sel * from Edwci.Ref_MHB_Action_Type where dw_last_update_date_time(date)=current_date
 ) Q"

export AC_EXP_SQL_STATEMENT="SELECT 'J_MHB_Ref_MHB_Action_Type'||','||CAST(Count(*) AS VARCHAR (20))||','
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
) Q"


#  @@END DMEXPRESS_EXPORTED_VARIABLES
#  End of Exported variables section
#  