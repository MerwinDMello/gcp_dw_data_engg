#  @@START DMEXPRESS_EXPORTED_VARIABLES

export AC_EXP_TOLERANCE_PERCENT=0
export AC_EXP_TOLERANCE_AMT=0
export JOBNAME='J_MHB_Ref_MHB_Alert_Type'


export AC_ACT_SQL_STATEMENT="SELECT 'J_MHB_Ref_MHB_Alert_Type'||','||CAST(Count(*) AS VARCHAR (20))||','

AS SOURCE_STRING FROM
(
sel * from Edwci.Ref_MHB_Alert_Type where dw_last_update_date_time(date)=current_date
 ) Q"

export AC_EXP_SQL_STATEMENT="SELECT 'J_MHB_Ref_MHB_Alert_Type'||','||CAST(Count(*) AS VARCHAR (20))||','
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
) Q"


#  @@END DMEXPRESS_EXPORTED_VARIABLES
#  End of Exported variables section
#     