#  @@START DMEXPRESS_EXPORTED_VARIABLES

export AC_EXP_TOLERANCE_PERCENT=0
export AC_EXP_TOLERANCE_AMT=0
export JOBNAME='J_MHB_Ref_MHB_User_Role'


export AC_ACT_SQL_STATEMENT="SELECT 'J_MHB_Ref_MHB_User_Role'||','||CAST(Count(*) AS VARCHAR (20))||','

AS SOURCE_STRING FROM
(
sel * from Edwci.Ref_MHB_User_Role where dw_last_update_date_time(date)=current_date
 ) Q"

export AC_EXP_SQL_STATEMENT="SELECT 'J_MHB_Ref_MHB_User_Role'||','||CAST(Count(*) AS VARCHAR (20))||','
AS SOURCE_STRING FROM
(
select * from 
(sel distinct MHB_User_Role_Desc
 from EDWCI_Staging.User_Role_Stg)X
 where X.MHB_User_Role_Desc not in (sel MHB_User_Role_Desc from Edwci.Ref_MHB_User_Role  )
) Q"


#  @@END DMEXPRESS_EXPORTED_VARIABLES
#  End of Exported variables section
#   