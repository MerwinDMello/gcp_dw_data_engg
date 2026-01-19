export JOBNAME='J_REF_FACILITY'
export AC_EXP_SQL_STATEMENT="SELECT	'J_REF_FACILITY'||','|| cast(count(*) as varchar(20))||',	' as SOURCE_STRING 
FROM
(
SELECT 
ROW_NUMBER() over (order by trim(Type_Stg.Facility_Name)) + (SEL COALESCE(MAX(Facility_Id), 0) AS ID1 FROM  EDWCR.REF_FACILITY) AS  Facility_Id,
trim(Type_Stg.Facility_Name) as Facility_Name,
Type_Stg.Source_System_Code as Source_System_Code,
Current_timestamp(0) as DW_Last_Update_Date_Time
FROM (
SELECT 
Facility_Name,
Source_System_Code
FROM EDWCR_Staging.Ref_Facility_Stg
where Facility_Name not in (sel trim(Facility_Name) from EDWCR.REF_FACILITY where Facility_Name IS NOT NULL )
)Type_Stg
WHERE Facility_Name IS NOT NULL and trim(Facility_Name) <> ''
) A"


export AC_ACT_SQL_STATEMENT="select 'J_REF_FACILITY'||','|| cast(count(*) as varchar(20))||',' as SOURCE_STRING 
FROM EdwCR.REF_FACILITY WHERE 
DW_Last_Update_Date_Time >= (SELECT MAX(Job_Start_Date_Time) as Job_Start_Date_Time FROM EDWCR_DMX_AC.ETL_JOB_RUN where Job_Name = 'J_REF_FACILITY');"
