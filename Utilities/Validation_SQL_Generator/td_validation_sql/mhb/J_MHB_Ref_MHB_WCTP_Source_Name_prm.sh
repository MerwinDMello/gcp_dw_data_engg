#  @@START DMEXPRESS_EXPORTED_VARIABLES

export AC_EXP_TOLERANCE_PERCENT=0
export AC_EXP_TOLERANCE_AMT=0
export JOBNAME='J_MHB_Ref_MHB_WCTP_Source_Name'

export AC_EXP_SQL_STATEMENT="SELECT '$JOBNAME'||','||CAST(Count(*) AS VARCHAR (20))||','
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
"


export AC_ACT_SQL_STATEMENT="SELECT '$JOBNAME'||','||CAST(Count(*) AS VARCHAR (20))||','

AS SOURCE_STRING FROM
(
sel * from EDWCI.Ref_MHB_WCTP_Source_Name  where dw_last_update_date_time(date)=current_date
 ) Q"


#  @@END DMEXPRESS_EXPORTED_VARIABLES
#  End of Exported variables section
#   