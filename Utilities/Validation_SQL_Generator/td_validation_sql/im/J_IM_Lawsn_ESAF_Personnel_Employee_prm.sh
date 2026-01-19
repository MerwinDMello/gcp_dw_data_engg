export Job_Name='J_IM_Lawsn_ESAF_Personnel_Employee'
export JOBNAME='J_IM_Lawsn_ESAF_Personnel_Employee'


export AC_EXP_SQL_STATEMENT="select 'J_IM_Lawsn_ESAF_Personnel_Employee'||','||cast(count(*) as varchar(20))||',' AS SOURCE_STRING

from
(
SELECT
CMPY,
EMPL,
CMPY_NUM,
DOB,
HOME_PHN_NUM,
SPLMT_PHN_NUM
FROM
(
SELECT 
T1.CMPY,
T1.EMPL,
T1.CMPY_NUM,
CAST(T1.DOB  AS DATE) as DOB,
T1.HOME_PHN_NUM,
T1.SPLMT_PHN_NUM
FROM ORAFS.VW_ESAF_PERSONNEL_EMPLOYEE T1

UNION

SELECT 
T2.CMPY,
T2.EMPL,
T2.CMPY_NUM,
CAST(T2.DOB  AS DATE) as DOB,
T2.HOME_PHN_NUM,
T2.SPLMT_PHN_NUM
FROM ORACORP.VW_ESAF_PERSONNEL_EMPLOYEE T2
)S
)A" 

export AC_ACT_SQL_STATEMENT="select 'J_IM_Lawsn_ESAF_Personnel_Employee'||','||cast(count(*) as varchar(20))||',' AS SOURCE_STRING from EDWIM_Staging.Lawsn_eSAF_Personnel_Employee ;"


