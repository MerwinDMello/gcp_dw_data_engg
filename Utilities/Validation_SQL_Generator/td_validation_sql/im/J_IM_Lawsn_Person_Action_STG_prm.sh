export Job_Name='J_IM_Lawsn_Person_Action_STG'
export JOBNAME='J_IM_Lawsn_Person_Action_STG'

export AC_EXP_SQL_STATEMENT="select 'J_IM_Lawsn_Person_Action_STG'||','||cast(count(*) as varchar(20))||',' AS SOURCE_STRING

from
(
SELECT 
CMPY,
EMPL,                
ACTN_CD
FROM
(
SELECT 
CMPY,
EMPL,                
EFF_DT,
ACTN_CD,
ROW_NUMBER() OVER(PARTITION BY CMPY, EMPL  ORDER BY EFF_DT DESC) as rnk

FROM ORAFS.VW_ESAF_PERSONNEL_ACTION
)
where rnk=1
       )" 

export AC_ACT_SQL_STATEMENT="select 'J_IM_Lawsn_Person_Action_STG'||','||cast(count(*) as varchar(20))||',' AS SOURCE_STRING from EDWIM_STAGING.Lawsn_Person_Action;"


