#  @@START DMEXPRESS_EXPORTED_VARIABLES

export AC_EXP_TOLERANCE_PERCENT=0
export AC_EXP_TOLERANCE_AMT=0
export Job_Name='J_IM_MT_CDM_USER_COID'
export JOBNAME='J_IM_MT_CDM_USER_COID'

export AC_EXP_SQL_STATEMENT="select 'J_IM_MT_CDM_USER_COID' ||','||cast(zeroifnull(count(*)) as varchar(20)) ||',' as source_string from 
(
SELECT  distinct 
ROLE_PLYR_SK as ROLE_PLYR_SK
,VLD_TO_TS as VLD_TO_TS
,COMPANY_CODE as COMPANY_CODE
,COID as COID
,CURRENT_TIMESTAMP(0) as time_stamp
FROM EDWIM_BASE_VIEWS.ENCNT_TO_ROLE
WHERE VLD_TO_TS = '9999-12-31 00:00:00'
 AND NOT(RL_TYPE_REF_CD LIKE 'INSUR%')
 AND  NOT(RL_TYPE_REF_CD LIKE  'FACIL%')
 AND  NOT(RL_TYPE_REF_CD LIKE  'PATI%')
        
)A;"



export AC_ACT_SQL_STATEMENT="select 'J_IM_MT_CDM_USER_COID' ||','||cast(zeroifnull(A.counts) as varchar(20)) ||',' as source_string from 
(
SELECT
	COUNT(*) as counts
 FROM Edwim_Staging.MT_CDM_User_Coid        
           
           )A;"






#  @@END DMEXPRESS_EXPORTED_VARIABLES
#  End of Exported variables section
#



