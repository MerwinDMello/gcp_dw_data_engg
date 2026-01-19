
#  @@START DMEXPRESS_EXPORTED_VARIABLES

export AC_EXP_TOLERANCE_PERCENT=0
export AC_EXP_TOLERANCE_AMT=0
export Job_Name='J_IM_MT_CDM_USER_MNEM'
export JOBNAME='J_IM_MT_CDM_USER_MNEM'

export AC_EXP_SQL_STATEMENT="select 'J_IM_MT_CDM_USER_MNEM' ||','||cast(zeroifnull(count(*)) as varchar(20)) ||',' as source_string from 
(
SELECT 
ROLE_PLYR_SK as ROLE_PLYR_SK
,VLD_TO_TS as VLD_TO_TS
,UPPER(ID_TXT) AS User_Mnemonic
,CURRENT_TIMESTAMP(0) as time_stamp
FROM
EDWIM_BASE_VIEWS.PRCTNR_ROLE_IDFN
WHERE REGISTN_TYPE_REF_CD = 'USER_MNE'
AND VLD_TO_TS = '9999-12-31 00:00:00'
QUALIFY ROW_NUMBER() OVER (PARTITION BY ROLE_PLYR_SK ORDER BY VLD_FR_TS DESC )  = 1
        
)A;"



export AC_ACT_SQL_STATEMENT="select 'J_IM_MT_CDM_USER_MNEM' ||','||cast(zeroifnull(A.counts) as varchar(20)) ||',' as source_string from 
(
SELECT
	COUNT(*) as counts
 FROM Edwim_Staging.MT_CDM_User_Mnem        
           
           )A;"






#  @@END DMEXPRESS_EXPORTED_VARIABLES
#  End of Exported variables section
#



