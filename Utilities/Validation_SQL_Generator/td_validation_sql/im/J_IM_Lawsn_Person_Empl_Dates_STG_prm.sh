export Job_Name='J_IM_Lawsn_Person_Empl_Dates_STG'
export JOBNAME='J_IM_Lawsn_Person_Empl_Dates_STG'

export AC_EXP_SQL_STATEMENT="select 'J_IM_Lawsn_Person_Empl_Dates_STG'||','||cast(count(*) as varchar(20))||',' AS SOURCE_STRING

from
(

SELECT DISTINCT
t1.CMPY,
t1.EMPL,
CAST(t2.HIRE_DATE AS DATE) AS Hired_Date,
CAST(t3.POSN_DATE AS DATE) AS Posn_Start_Date,
CAST(t1.TERMN_DATE AS DATE) AS Termn_Date
FROM  
        (
                SELECT
                CMPY,
                EMPL,
                MAX(TERMN_DT) AS TERMN_DATE
                FROM  ORAFS.VW_ESAF_EMPLOYEE
                GROUP BY CMPY, EMPL
        ) t1
INNER JOIN 
        (
                SELECT
                CMPY,
                EMPL,
                MIN(HIRE_DT) AS HIRE_DATE
                FROM  ORAFS.VW_ESAF_EMPLOYEE
                GROUP BY CMPY, EMPL
        ) t2
ON t1.EMPL = t2.EMPL 
AND t1.CMPY = t2.CMPY
INNER JOIN 
        (
                SELECT
                CMPY,
                EMPL,
                MAX(HIRE_DT) AS POSN_DATE
                FROM  ORAFS.VW_ESAF_EMPLOYEE
                GROUP BY CMPY,EMPL
        ) t3
ON t1.EMPL = t3.EMPL   
AND t1.CMPY = t3.CMPY
   
       )" 

export AC_ACT_SQL_STATEMENT="select 'J_IM_Lawsn_Person_Empl_Dates_STG'||','||cast(count(*) as varchar(20))||',' AS SOURCE_STRING from EDWIM_STAGING.Lawsn_Person_Empl_Dates;"


