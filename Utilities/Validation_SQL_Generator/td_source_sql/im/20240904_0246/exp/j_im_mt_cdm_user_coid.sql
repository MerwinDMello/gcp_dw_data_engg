select 'J_IM_MT_CDM_USER_COID' ||','||cast(zeroifnull(count(*)) as varchar(20)) ||',' as source_string from 
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
        
)A;