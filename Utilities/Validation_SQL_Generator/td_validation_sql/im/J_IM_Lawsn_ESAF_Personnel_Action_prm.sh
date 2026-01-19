export Job_Name='J_IM_Lawsn_ESAF_Personnel_Action'
export JOBNAME='J_IM_Lawsn_ESAF_Personnel_Action'


export AC_EXP_SQL_STATEMENT="select 'J_IM_Lawsn_ESAF_Personnel_Action'||','||cast(count(*) as varchar(20))||',' AS SOURCE_STRING

from
(
SELECT
ACTN_CD,
CMPY,
EFF_DT,
EMPL
FROM
(
SELECT DISTINCT
T1.ACTN_CD,
T1.CMPY,
CAST(T1.EFF_DT AS DATE) as EFF_DT,
T1.EMPL
FROM ORAFS.VW_ESAF_PERSONNEL_ACTION T1
GROUP BY 
ACTN_CD,
CMPY,
EFF_DT,
EMPL

UNION

SELECT DISTINCT
T2.ACTN_CD,
T2.CMPY,
CAST(T2.EFF_DT  AS DATE) as EFF_DT,
T2.EMPL
FROM ORACORP.VW_ESAF_PERSONNEL_ACTION T2
)
)A" 

export AC_ACT_SQL_STATEMENT="select 'J_IM_Lawsn_ESAF_Personnel_Action'||','||cast(count(*) as varchar(20))||',' AS SOURCE_STRING from EDWIM_Staging.Lawsn_eSAF_Personnel_Action ;"


