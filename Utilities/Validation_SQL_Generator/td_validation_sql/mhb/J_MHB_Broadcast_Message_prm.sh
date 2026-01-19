#  @@START DMEXPRESS_EXPORTED_VARIABLES

export AC_EXP_TOLERANCE_PERCENT=0
export AC_EXP_TOLERANCE_AMT=0
export JOBNAME='J_MHB_Broadcast_Message'


export AC_ACT_SQL_STATEMENT="SELECT '$JOBNAME'||','||CAST(Count(*) AS VARCHAR (20))||','

AS SOURCE_STRING FROM
(
sel * from EDWCI.MHB_Broadcast_Message  where dw_last_update_date_time(date)=current_date
 ) Q"

export AC_EXP_SQL_STATEMENT="SELECT '$JOBNAME'||','||CAST(Count(*) AS VARCHAR (20))||','
AS SOURCE_STRING FROM EDWCI_Staging.MHB_Broadcast_Message_WRK"


#  @@END DMEXPRESS_EXPORTED_VARIABLES
#  End of Exported variables section
#   