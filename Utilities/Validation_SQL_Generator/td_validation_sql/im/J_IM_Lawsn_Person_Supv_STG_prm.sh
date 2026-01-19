export Job_Name='J_IM_Lawsn_Person_Supv_STG'
export JOBNAME='J_IM_Lawsn_Person_Supv_STG'

export AC_EXP_SQL_STATEMENT="select 'J_IM_Lawsn_Person_Supv_STG'||','||cast(count(*) as varchar(20))||',' AS SOURCE_STRING
FROM
(
SELECT 
CMPY,
EMPL,
Supervisor_User_Id
 from
(
SELECT 
t1.CMPY,
t1.EMPL,
--t2.EMPL AS Supervisor_Empl_Num,
t3.CMPY_Num AS Supervisor_User_Id,
--t2.SUPV_CD,
--t2.EFF_DT,
ROW_NUMBER() OVER (PARTITION BY t1.CMPY, t1.EMPL ORDER BY t2.EFF_DT DESC) as rnk
                                        
FROM ORAFS.VW_ESAF_EMPLOYEE t1

INNER JOIN ORAFS.VW_ESAF_SUPERVISOR t2
ON t1.CMPY = t2.CMPY
AND t1.SUPV = t2.SUPV_CD

INNER JOIN ORAFS.VW_ESAF_PERSONNEL_EMPLOYEE t3
ON t2.CMPY = t3.CMPY
AND t2.EMPL = t3.EMPL
       
WHERE t2.ACTV_FLG = 'A'
)
where rnk=1	
       )" 

export AC_ACT_SQL_STATEMENT="select 'J_IM_Lawsn_Person_Supv_STG'||','||cast(count(*) as varchar(20))||',' AS SOURCE_STRING from EDWIM_STAGING.Lawsn_Person_Supv;"


