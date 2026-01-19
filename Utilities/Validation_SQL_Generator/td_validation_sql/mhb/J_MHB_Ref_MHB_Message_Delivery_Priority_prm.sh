#  @@START DMEXPRESS_EXPORTED_VARIABLES

export AC_EXP_TOLERANCE_PERCENT=0
export AC_EXP_TOLERANCE_AMT=0
export JOBNAME='J_MHB_Ref_MHB_Message_Delivery_Priority'

export AC_EXP_SQL_STATEMENT="SELECT '$JOBNAME'||','||CAST(Count(*) AS VARCHAR (20))||','
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
"


export AC_ACT_SQL_STATEMENT="SELECT '$JOBNAME'||','||CAST(Count(*) AS VARCHAR (20))||','

AS SOURCE_STRING FROM
(
sel * from EDWCI.Ref_MHB_Message_Delivery_Priority  where dw_last_update_date_time(date)=current_date
 ) Q"


#  @@END DMEXPRESS_EXPORTED_VARIABLES
#  End of Exported variables section
#   