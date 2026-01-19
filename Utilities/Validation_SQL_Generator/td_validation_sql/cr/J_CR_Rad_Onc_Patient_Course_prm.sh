export JOBNAME='J_CR_Rad_Onc_Patient_Course'

export AC_EXP_SQL_STATEMENT="SELECT '$JOBNAME'||','|| cast(count(*) as varchar(20))||',' as SOURCE_STRING 
from $NCR_STG_SCHEMA.STG_DimCourse dp
inner join $NCR_TGT_SCHEMA.Ref_Rad_Onc_Site rs 
on rs.Source_Site_Id=dp.DimSiteID
LEFT JOIN $NCR_TGT_SCHEMA.Rad_Onc_Patient ra 
on dp.DimPatientID = ra.Source_Patient_Id 
and rs.Site_Sk=ra.Site_SK "

export AC_ACT_SQL_STATEMENT="select '$JOBNAME'||','|| cast(count(*) as varchar(20))||',' as SOURCE_STRING 
FROM $NCR_TGT_SCHEMA.Rad_Onc_Patient_Course
where 
DW_Last_Update_Date_Time >= (SELECT MAX(Job_Start_Date_Time) as Job_Start_Date_Time FROM ${NCR_AC_VIEW}.ETL_JOB_RUN where Job_Name = '$JOBNAME')
"


