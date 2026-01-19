#  @@START DMEXPRESS_EXPORTED_VARIABLES

export AC_EXP_TOLERANCE_PERCENT=0
export AC_EXP_TOLERANCE_AMT=0
export JOBNAME='J_MHB_Lab_Order_Audit_Detail'


export AC_ACT_SQL_STATEMENT="SELECT 'J_MHB_Lab_Order_Audit_Detail'||','||CAST(Count(*) AS VARCHAR (20))||','

AS SOURCE_STRING FROM
(
sel * from Edwci.MHB_LAB_ORDER_AUDIT_DETAIL  where dw_last_update_date_time(date)=current_date
 ) Q"

export AC_EXP_SQL_STATEMENT="SELECT 'J_MHB_Lab_Order_Audit_Detail'||','||CAST(Count(*) AS VARCHAR (20))||','
AS SOURCE_STRING FROM
(
SELECT 
*
FROM
EDWCI_STAGING.MHB_LAB_ORDER_AUDIT_DETAIL_WRK
 
) Q"


#  @@END DMEXPRESS_EXPORTED_VARIABLES
#  End of Exported variables section
#    