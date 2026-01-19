 #  @@START DMEXPRESS_EXPORTED_VARIABLES

export AC_EXP_TOLERANCE_PERCENT=0
export AC_EXP_TOLERANCE_AMT=0
export JOBNAME='J_MHB_Ref_MHB_WCTP_Destination_Name'








export AC_EXP_SQL_STATEMENT="SELECT '$JOBNAME'||','||CAST(Count(*) AS VARCHAR (20))||','
AS SOURCE_STRING FROM 
(SELECT Distinct 
DestinationName
from EDWCI_Staging.vwWCTPOutboundMessages
where DestinationName NOT IN 
(SELECT WCTP_Destination_Name FROM EDWCI.Ref_MHB_WCTP_Destination_Name) 
) A"

export AC_ACT_SQL_STATEMENT="SELECT '$JOBNAME'||','||CAST(Count(*) AS VARCHAR (20))||','

AS SOURCE_STRING FROM
(
sel * from EDWCI.Ref_MHB_WCTP_Destination_Name where dw_last_update_date_time(date)=current_date
 ) Q"



#  @@END DMEXPRESS_EXPORTED_VARIABLES
#  End of Exported variables section
#  





