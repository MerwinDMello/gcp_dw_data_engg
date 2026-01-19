export JOBNAME='J_CN_REG_CANCER_PATIENT_TUMOR_STAGING'

export AC_EXP_SQL_STATEMENT="Select 'J_CN_REG_CANCER_PATIENT_TUMOR_STAGING'||','|| cast(count(*) as varchar(20))||',' as SOURCE_STRING from
(                                           
sel 
ROW_NUMBER() OVER( ORDER BY Cancer_Patient_Tumor_Driver_SK asc ,Cancer_Patient_Driver_SK asc , Cancer_Tumor_Driver_SK asc , Cancer_Stage_Class_Method_Code,Cancer_Stage_Type_Code asc) AS Cancer_Patient_Tumor_Staging_SK,
      Cancer_Patient_Tumor_Driver_SK ,
      Cancer_Patient_Driver_SK ,
      Cancer_Tumor_Driver_SK ,
      Coid ,
      Company_Code ,
      Best_CS_Summary_Desc ,
      Best_CS_TNM_Desc ,
      Tumor_Size_Num_Text ,
      Cancer_Stage_Code ,
      Cancer_Stage_Class_Method_Code ,
      Cancer_Staging_Type_Code AS Cancer_Stage_Type_Code,
      Cancer_Stage_Result_Text ,
      AJCC_Stage_Desc ,
      Tumor_Size_Summary_Desc ,
      Source_System_Code ,
CURRENT_TIMESTAMP(0) as DW_Last_Update_Date_Time
from
(
Select
distinct
CPTD.Cancer_Patient_Tumor_Driver_SK
,CPTD.Cancer_Patient_Driver_SK
,CPTD.Cancer_Tumor_Driver_SK
,CPTD.Coid
,CPTD.Company_Code
,CR.Best_CS_Summary_Desc
,CR.Best_CS_TNM_Desc
,CR.Tumor_Size_Num_Text
,CNPS.Cancer_Stage_Code
,coalesce(CR.Cancer_Stage_Classification_Method_Code,CNPS.Cancer_Stage_Class_Method_Code) as Cancer_Stage_Class_Method_Code
,coalesce(CR.Cancer_Stage_Type_Code,CNPS.Cancer_Staging_Type_Code) as Cancer_Staging_Type_Code
,coalesce(CR.Cancer_Stage_Result_Text,CNPS.Cancer_Staging_Result_Code) as Cancer_Stage_Result_Text
,CR.AJCC_Stage_Desc
,CR.Tumor_Size_Summary_Desc
,CPTD.Source_System_Code
,CPTD.DW_Last_Update_Date_Time
from EDWCR_BASE_VIEWS.Cancer_Patient_Tumor_Driver CPTD
Left Outer  Join

( Select
CPT.CR_Patient_Id
,CPT.TUMOR_PRIMARY_SITE_ID
,t26.Lookup_Desc as Best_CS_Summary_Desc
,t46.Lookup_Desc as Best_CS_TNM_Desc
,CPT.Tumor_Size_Num_Text 
,CPS.Cancer_Stage_Classification_Method_Code
,CPS.Cancer_Stage_Type_Code
,CPS.Cancer_Stage_Result_Text
,RAS.AJCC_Stage_Desc
,t43.Lookup_Desc as Tumor_Size_Summary_Desc
From EDWCR_BASE_VIEWS.CR_PATIENT_TUMOR CPT
Left Outer join
edwcr_base_views.CR_Patient_Staging CPS
on CPT.Tumor_Id = CPS.Tumor_Id
Left Outer Join Ref_AJCC_Stage RAS
on CPS.AJCC_Stage_Id = RAS.AJCC_Stage_Id
left outer join edwcr_base_views.ref_lookup_code t26
on CPT.Best_CS_Summary_Id = t26.Master_Lookup_Sid
and t26.Lookup_Id =26
left outer join edwcr_base_views.ref_lookup_code t46
on CPT.Best_CS_TNM_Id = t46.Master_Lookup_Sid
and t46.Lookup_Id =46
left outer join edwcr_base_views.ref_lookup_code t43
on CPT.Tumor_Size_Summary_Id = t43.Master_Lookup_Sid
and t43.Lookup_Id =43
where CPS.Cancer_Stage_Classification_Method_Code='C' and CPS.Cancer_Stage_Type_Code='T'
) CR
ON CPTD.CR_PATIENT_ID=CR.CR_PATIENT_ID
AND CPTD.CR_TUMOR_PRIMARY_SITE_ID=CR.TUMOR_PRIMARY_SITE_ID
Left Outer Join edwcr_base_views.cn_patient_staging CNPS
on CPTD.CN_PATIENT_ID=CNPS.Nav_PATIENT_ID
and CPTD.CN_Tumor_Type_Id=CNPS.Tumor_Type_Id
and CNPS.Cancer_Stage_Class_Method_Code='C' and CNPS.Cancer_Staging_Type_Code='T'
where CR.CR_Patient_Id is not null or CNPS.Nav_PATIENT_ID is not null
Union ALL
Select
distinct
CPTD.Cancer_Patient_Tumor_Driver_SK
,CPTD.Cancer_Patient_Driver_SK
,CPTD.Cancer_Tumor_Driver_SK
,CPTD.Coid
,CPTD.Company_Code
,CR.Best_CS_Summary_Desc
,CR.Best_CS_TNM_Desc
,CR.Tumor_Size_Num_Text
,CNPS.Cancer_Stage_Code
,coalesce(CR.Cancer_Stage_Classification_Method_Code,CNPS.Cancer_Stage_Class_Method_Code) as Cancer_Stage_Class_Method_Code
,coalesce(CR.Cancer_Stage_Type_Code,CNPS.Cancer_Staging_Type_Code) as Cancer_Staging_Type_Code
,coalesce(CR.Cancer_Stage_Result_Text,CNPS.Cancer_Staging_Result_Code) as Cancer_Stage_Result_Text
,CR.AJCC_Stage_Desc
,CR.Tumor_Size_Summary_Desc
,CPTD.Source_System_Code
,CPTD.DW_Last_Update_Date_Time
from EDWCR_BASE_VIEWS.Cancer_Patient_Tumor_Driver CPTD
Left Outer  Join

( Select
CPT.CR_Patient_Id
,CPT.TUMOR_PRIMARY_SITE_ID
,t26.Lookup_Desc as Best_CS_Summary_Desc
,t46.Lookup_Desc as Best_CS_TNM_Desc
,CPT.Tumor_Size_Num_Text 
,CPS.Cancer_Stage_Classification_Method_Code
,CPS.Cancer_Stage_Type_Code
,CPS.Cancer_Stage_Result_Text
,RAS.AJCC_Stage_Desc
,t43.Lookup_Desc as Tumor_Size_Summary_Desc
From EDWCR_BASE_VIEWS.CR_PATIENT_TUMOR CPT
Left Outer join
edwcr_base_views.CR_Patient_Staging CPS
on CPT.Tumor_Id = CPS.Tumor_Id
Left Outer Join Ref_AJCC_Stage RAS
on CPS.AJCC_Stage_Id = RAS.AJCC_Stage_Id
left outer join edwcr_base_views.ref_lookup_code t26
on CPT.Best_CS_Summary_Id = t26.Master_Lookup_Sid
and t26.Lookup_Id =26
left outer join edwcr_base_views.ref_lookup_code t46
on CPT.Best_CS_TNM_Id = t46.Master_Lookup_Sid
and t46.Lookup_Id =46
left outer join edwcr_base_views.ref_lookup_code t43
on CPT.Tumor_Size_Summary_Id = t43.Master_Lookup_Sid
and t43.Lookup_Id =43
where CPS.Cancer_Stage_Classification_Method_Code='C' and CPS.Cancer_Stage_Type_Code='N'
) CR
ON CPTD.CR_PATIENT_ID=CR.CR_PATIENT_ID
AND CPTD.CR_TUMOR_PRIMARY_SITE_ID=CR.TUMOR_PRIMARY_SITE_ID
Left Outer Join edwcr_base_views.cn_patient_staging CNPS
on CPTD.CN_PATIENT_ID=CNPS.Nav_PATIENT_ID
and CPTD.CN_Tumor_Type_Id=CNPS.Tumor_Type_Id
and CNPS.Cancer_Stage_Class_Method_Code='C' and CNPS.Cancer_Staging_Type_Code='N'
where CR.CR_Patient_Id is not null or CNPS.Nav_PATIENT_ID is not null
union all
Select
distinct
CPTD.Cancer_Patient_Tumor_Driver_SK
,CPTD.Cancer_Patient_Driver_SK
,CPTD.Cancer_Tumor_Driver_SK
,CPTD.Coid
,CPTD.Company_Code
,CR.Best_CS_Summary_Desc
,CR.Best_CS_TNM_Desc
,CR.Tumor_Size_Num_Text
,CNPS.Cancer_Stage_Code
,coalesce(CR.Cancer_Stage_Classification_Method_Code,CNPS.Cancer_Stage_Class_Method_Code) as Cancer_Stage_Class_Method_Code
,coalesce(CR.Cancer_Stage_Type_Code,CNPS.Cancer_Staging_Type_Code) as Cancer_Staging_Type_Code
,coalesce(CR.Cancer_Stage_Result_Text,CNPS.Cancer_Staging_Result_Code) as Cancer_Stage_Result_Text
,CR.AJCC_Stage_Desc
,CR.Tumor_Size_Summary_Desc
,CPTD.Source_System_Code
,CPTD.DW_Last_Update_Date_Time
from EDWCR_BASE_VIEWS.Cancer_Patient_Tumor_Driver CPTD
Left Outer  Join

( Select
CPT.CR_Patient_Id
,CPT.TUMOR_PRIMARY_SITE_ID
,t26.Lookup_Desc as Best_CS_Summary_Desc
,t46.Lookup_Desc as Best_CS_TNM_Desc
,CPT.Tumor_Size_Num_Text 
,CPS.Cancer_Stage_Classification_Method_Code
,CPS.Cancer_Stage_Type_Code
,CPS.Cancer_Stage_Result_Text
,RAS.AJCC_Stage_Desc
,t43.Lookup_Desc as Tumor_Size_Summary_Desc
From EDWCR_BASE_VIEWS.CR_PATIENT_TUMOR CPT
Left Outer join
edwcr_base_views.CR_Patient_Staging CPS
on CPT.Tumor_Id = CPS.Tumor_Id
Left Outer Join Ref_AJCC_Stage RAS
on CPS.AJCC_Stage_Id = RAS.AJCC_Stage_Id
left outer join edwcr_base_views.ref_lookup_code t26
on CPT.Best_CS_Summary_Id = t26.Master_Lookup_Sid
and t26.Lookup_Id =26
left outer join edwcr_base_views.ref_lookup_code t46
on CPT.Best_CS_TNM_Id = t46.Master_Lookup_Sid
and t46.Lookup_Id =46
left outer join edwcr_base_views.ref_lookup_code t43
on CPT.Tumor_Size_Summary_Id = t43.Master_Lookup_Sid
and t43.Lookup_Id =43
where CPS.Cancer_Stage_Classification_Method_Code='C' and CPS.Cancer_Stage_Type_Code='M'
) CR
ON CPTD.CR_PATIENT_ID=CR.CR_PATIENT_ID
AND CPTD.CR_TUMOR_PRIMARY_SITE_ID=CR.TUMOR_PRIMARY_SITE_ID
Left Outer Join edwcr_base_views.cn_patient_staging CNPS
on CPTD.CN_PATIENT_ID=CNPS.Nav_PATIENT_ID
and CPTD.CN_Tumor_Type_Id=CNPS.Tumor_Type_Id
and CNPS.Cancer_Stage_Class_Method_Code='C' and CNPS.Cancer_Staging_Type_Code='M'
where CR.CR_Patient_Id is not null or CNPS.Nav_PATIENT_ID is not null
Union all
Select
distinct
CPTD.Cancer_Patient_Tumor_Driver_SK
,CPTD.Cancer_Patient_Driver_SK
,CPTD.Cancer_Tumor_Driver_SK
,CPTD.Coid
,CPTD.Company_Code
,CR.Best_CS_Summary_Desc
,CR.Best_CS_TNM_Desc
,CR.Tumor_Size_Num_Text
,CNPS.Cancer_Stage_Code
,coalesce(CR.Cancer_Stage_Classification_Method_Code,CNPS.Cancer_Stage_Class_Method_Code) as Cancer_Stage_Class_Method_Code
,coalesce(CR.Cancer_Stage_Type_Code,CNPS.Cancer_Staging_Type_Code) as Cancer_Staging_Type_Code
,coalesce(CR.Cancer_Stage_Result_Text,CNPS.Cancer_Staging_Result_Code) as Cancer_Stage_Result_Text
,CR.AJCC_Stage_Desc
,CR.Tumor_Size_Summary_Desc
,CPTD.Source_System_Code
,CPTD.DW_Last_Update_Date_Time
from EDWCR_BASE_VIEWS.Cancer_Patient_Tumor_Driver CPTD
Left Outer  Join

( Select
CPT.CR_Patient_Id
,CPT.TUMOR_PRIMARY_SITE_ID
,t26.Lookup_Desc as Best_CS_Summary_Desc
,t46.Lookup_Desc as Best_CS_TNM_Desc
,CPT.Tumor_Size_Num_Text 
,CPS.Cancer_Stage_Classification_Method_Code
,CPS.Cancer_Stage_Type_Code
,CPS.Cancer_Stage_Result_Text
,RAS.AJCC_Stage_Desc
,t43.Lookup_Desc as Tumor_Size_Summary_Desc
From EDWCR_BASE_VIEWS.CR_PATIENT_TUMOR CPT
Left Outer join
edwcr_base_views.CR_Patient_Staging CPS
on CPT.Tumor_Id = CPS.Tumor_Id
Left Outer Join Ref_AJCC_Stage RAS
on CPS.AJCC_Stage_Id = RAS.AJCC_Stage_Id
left outer join edwcr_base_views.ref_lookup_code t26
on CPT.Best_CS_Summary_Id = t26.Master_Lookup_Sid
and t26.Lookup_Id =26
left outer join edwcr_base_views.ref_lookup_code t46
on CPT.Best_CS_TNM_Id = t46.Master_Lookup_Sid
and t46.Lookup_Id =46
left outer join edwcr_base_views.ref_lookup_code t43
on CPT.Tumor_Size_Summary_Id = t43.Master_Lookup_Sid
and t43.Lookup_Id =43
where CPS.Cancer_Stage_Classification_Method_Code='C' and CPS.Cancer_Stage_Type_Code is null
) CR
ON CPTD.CR_PATIENT_ID=CR.CR_PATIENT_ID
AND CPTD.CR_TUMOR_PRIMARY_SITE_ID=CR.TUMOR_PRIMARY_SITE_ID
Left Outer Join edwcr_base_views.cn_patient_staging CNPS
on CPTD.CN_PATIENT_ID=CNPS.Nav_PATIENT_ID
and CPTD.CN_Tumor_Type_Id=CNPS.Tumor_Type_Id
and CNPS.Cancer_Stage_Class_Method_Code='C' and CNPS.Cancer_Staging_Type_Code is null
where CR.CR_Patient_Id is not null or CNPS.Nav_PATIENT_ID is not null
union all
Select
distinct
CPTD.Cancer_Patient_Tumor_Driver_SK
,CPTD.Cancer_Patient_Driver_SK
,CPTD.Cancer_Tumor_Driver_SK
,CPTD.Coid
,CPTD.Company_Code
,CR.Best_CS_Summary_Desc
,CR.Best_CS_TNM_Desc
,CR.Tumor_Size_Num_Text
,CNPS.Cancer_Stage_Code
,coalesce(CR.Cancer_Stage_Classification_Method_Code,CNPS.Cancer_Stage_Class_Method_Code) as Cancer_Stage_Class_Method_Code
,coalesce(CR.Cancer_Stage_Type_Code,CNPS.Cancer_Staging_Type_Code) as Cancer_Staging_Type_Code
,coalesce(CR.Cancer_Stage_Result_Text,CNPS.Cancer_Staging_Result_Code) as Cancer_Stage_Result_Text
,CR.AJCC_Stage_Desc
,CR.Tumor_Size_Summary_Desc
,CPTD.Source_System_Code
,CPTD.DW_Last_Update_Date_Time
from EDWCR_BASE_VIEWS.Cancer_Patient_Tumor_Driver CPTD
Left Outer  Join

( Select
CPT.CR_Patient_Id
,CPT.TUMOR_PRIMARY_SITE_ID
,t26.Lookup_Desc as Best_CS_Summary_Desc
,t46.Lookup_Desc as Best_CS_TNM_Desc
,CPT.Tumor_Size_Num_Text 
,CPS.Cancer_Stage_Classification_Method_Code
,CPS.Cancer_Stage_Type_Code
,CPS.Cancer_Stage_Result_Text
,RAS.AJCC_Stage_Desc
,t43.Lookup_Desc as Tumor_Size_Summary_Desc
From EDWCR_BASE_VIEWS.CR_PATIENT_TUMOR CPT
Left Outer join
edwcr_base_views.CR_Patient_Staging CPS
on CPT.Tumor_Id = CPS.Tumor_Id
Left Outer Join Ref_AJCC_Stage RAS
on CPS.AJCC_Stage_Id = RAS.AJCC_Stage_Id
left outer join edwcr_base_views.ref_lookup_code t26
on CPT.Best_CS_Summary_Id = t26.Master_Lookup_Sid
and t26.Lookup_Id =26
left outer join edwcr_base_views.ref_lookup_code t46
on CPT.Best_CS_TNM_Id = t46.Master_Lookup_Sid
and t46.Lookup_Id =46
left outer join edwcr_base_views.ref_lookup_code t43
on CPT.Tumor_Size_Summary_Id = t43.Master_Lookup_Sid
and t43.Lookup_Id =43
where  CPS.Cancer_Stage_Classification_Method_Code='P'  and CPS.Cancer_Stage_Type_Code='T'
) CR
ON CPTD.CR_PATIENT_ID=CR.CR_PATIENT_ID
AND CPTD.CR_TUMOR_PRIMARY_SITE_ID=CR.TUMOR_PRIMARY_SITE_ID
Left Outer Join edwcr_base_views.cn_patient_staging CNPS
on CPTD.CN_PATIENT_ID=CNPS.Nav_PATIENT_ID
and CPTD.CN_Tumor_Type_Id=CNPS.Tumor_Type_Id
and CNPS.Cancer_Stage_Class_Method_Code='P' and CNPS.Cancer_Staging_Type_Code='T'
where CR.CR_Patient_Id is not null or CNPS.Nav_PATIENT_ID is not null
union all
Select
distinct
CPTD.Cancer_Patient_Tumor_Driver_SK
,CPTD.Cancer_Patient_Driver_SK
,CPTD.Cancer_Tumor_Driver_SK
,CPTD.Coid
,CPTD.Company_Code
,CR.Best_CS_Summary_Desc
,CR.Best_CS_TNM_Desc
,CR.Tumor_Size_Num_Text
,CNPS.Cancer_Stage_Code
,coalesce(CR.Cancer_Stage_Classification_Method_Code,CNPS.Cancer_Stage_Class_Method_Code) as Cancer_Stage_Class_Method_Code
,coalesce(CR.Cancer_Stage_Type_Code,CNPS.Cancer_Staging_Type_Code) as Cancer_Staging_Type_Code
,coalesce(CR.Cancer_Stage_Result_Text,CNPS.Cancer_Staging_Result_Code) as Cancer_Stage_Result_Text
,CR.AJCC_Stage_Desc
,CR.Tumor_Size_Summary_Desc
,CPTD.Source_System_Code
,CPTD.DW_Last_Update_Date_Time
from EDWCR_BASE_VIEWS.Cancer_Patient_Tumor_Driver CPTD
Left Outer  Join

( Select
CPT.CR_Patient_Id
,CPT.TUMOR_PRIMARY_SITE_ID
,t26.Lookup_Desc as Best_CS_Summary_Desc
,t46.Lookup_Desc as Best_CS_TNM_Desc
,CPT.Tumor_Size_Num_Text 
,CPS.Cancer_Stage_Classification_Method_Code
,CPS.Cancer_Stage_Type_Code
,CPS.Cancer_Stage_Result_Text
,RAS.AJCC_Stage_Desc
,t43.Lookup_Desc as Tumor_Size_Summary_Desc
From EDWCR_BASE_VIEWS.CR_PATIENT_TUMOR CPT
Left Outer join
edwcr_base_views.CR_Patient_Staging CPS
on CPT.Tumor_Id = CPS.Tumor_Id
Left Outer Join Ref_AJCC_Stage RAS
on CPS.AJCC_Stage_Id = RAS.AJCC_Stage_Id
left outer join edwcr_base_views.ref_lookup_code t26
on CPT.Best_CS_Summary_Id = t26.Master_Lookup_Sid
and t26.Lookup_Id =26
left outer join edwcr_base_views.ref_lookup_code t46
on CPT.Best_CS_TNM_Id = t46.Master_Lookup_Sid
and t46.Lookup_Id =46
left outer join edwcr_base_views.ref_lookup_code t43
on CPT.Tumor_Size_Summary_Id = t43.Master_Lookup_Sid
and t43.Lookup_Id =43
where  CPS.Cancer_Stage_Classification_Method_Code='P'  and CPS.Cancer_Stage_Type_Code='N'
) CR
ON CPTD.CR_PATIENT_ID=CR.CR_PATIENT_ID
AND CPTD.CR_TUMOR_PRIMARY_SITE_ID=CR.TUMOR_PRIMARY_SITE_ID
Left Outer Join edwcr_base_views.cn_patient_staging CNPS
on CPTD.CN_PATIENT_ID=CNPS.Nav_PATIENT_ID
and CPTD.CN_Tumor_Type_Id=CNPS.Tumor_Type_Id
and CNPS.Cancer_Stage_Class_Method_Code='P' and CNPS.Cancer_Staging_Type_Code='N'
where CR.CR_Patient_Id is not null or CNPS.Nav_PATIENT_ID is not null
union all
Select
distinct
CPTD.Cancer_Patient_Tumor_Driver_SK
,CPTD.Cancer_Patient_Driver_SK
,CPTD.Cancer_Tumor_Driver_SK
,CPTD.Coid
,CPTD.Company_Code
,CR.Best_CS_Summary_Desc
,CR.Best_CS_TNM_Desc
,CR.Tumor_Size_Num_Text
,CNPS.Cancer_Stage_Code
,coalesce(CR.Cancer_Stage_Classification_Method_Code,CNPS.Cancer_Stage_Class_Method_Code) as Cancer_Stage_Class_Method_Code
,coalesce(CR.Cancer_Stage_Type_Code,CNPS.Cancer_Staging_Type_Code) as Cancer_Staging_Type_Code
,coalesce(CR.Cancer_Stage_Result_Text,CNPS.Cancer_Staging_Result_Code) as Cancer_Stage_Result_Text
,CR.AJCC_Stage_Desc
,CR.Tumor_Size_Summary_Desc
,CPTD.Source_System_Code
,CPTD.DW_Last_Update_Date_Time
from EDWCR_BASE_VIEWS.Cancer_Patient_Tumor_Driver CPTD
Left Outer  Join

( Select
CPT.CR_Patient_Id
,CPT.TUMOR_PRIMARY_SITE_ID
,t26.Lookup_Desc as Best_CS_Summary_Desc
,t46.Lookup_Desc as Best_CS_TNM_Desc
,CPT.Tumor_Size_Num_Text 
,CPS.Cancer_Stage_Classification_Method_Code
,CPS.Cancer_Stage_Type_Code
,CPS.Cancer_Stage_Result_Text
,RAS.AJCC_Stage_Desc
,t43.Lookup_Desc as Tumor_Size_Summary_Desc
From EDWCR_BASE_VIEWS.CR_PATIENT_TUMOR CPT
Left Outer join
edwcr_base_views.CR_Patient_Staging CPS
on CPT.Tumor_Id = CPS.Tumor_Id
Left Outer Join Ref_AJCC_Stage RAS
on CPS.AJCC_Stage_Id = RAS.AJCC_Stage_Id
left outer join edwcr_base_views.ref_lookup_code t26
on CPT.Best_CS_Summary_Id = t26.Master_Lookup_Sid
and t26.Lookup_Id =26
left outer join edwcr_base_views.ref_lookup_code t46
on CPT.Best_CS_TNM_Id = t46.Master_Lookup_Sid
and t46.Lookup_Id =46
left outer join edwcr_base_views.ref_lookup_code t43
on CPT.Tumor_Size_Summary_Id = t43.Master_Lookup_Sid
and t43.Lookup_Id =43
where  CPS.Cancer_Stage_Classification_Method_Code='P'  and CPS.Cancer_Stage_Type_Code='M'
) CR
ON CPTD.CR_PATIENT_ID=CR.CR_PATIENT_ID
AND CPTD.CR_TUMOR_PRIMARY_SITE_ID=CR.TUMOR_PRIMARY_SITE_ID
Left Outer Join edwcr_base_views.cn_patient_staging CNPS
on CPTD.CN_PATIENT_ID=CNPS.Nav_PATIENT_ID
and CPTD.CN_Tumor_Type_Id=CNPS.Tumor_Type_Id
and CNPS.Cancer_Stage_Class_Method_Code='P' and CNPS.Cancer_Staging_Type_Code='M'
where CR.CR_Patient_Id is not null or CNPS.Nav_PATIENT_ID is not null
union all
Select
distinct
CPTD.Cancer_Patient_Tumor_Driver_SK
,CPTD.Cancer_Patient_Driver_SK
,CPTD.Cancer_Tumor_Driver_SK
,CPTD.Coid
,CPTD.Company_Code
,CR.Best_CS_Summary_Desc
,CR.Best_CS_TNM_Desc
,CR.Tumor_Size_Num_Text
,CNPS.Cancer_Stage_Code
,coalesce(CR.Cancer_Stage_Classification_Method_Code,CNPS.Cancer_Stage_Class_Method_Code) as Cancer_Stage_Class_Method_Code
,coalesce(CR.Cancer_Stage_Type_Code,CNPS.Cancer_Staging_Type_Code) as Cancer_Staging_Type_Code
,coalesce(CR.Cancer_Stage_Result_Text,CNPS.Cancer_Staging_Result_Code) as Cancer_Stage_Result_Text
,CR.AJCC_Stage_Desc
,CR.Tumor_Size_Summary_Desc
,CPTD.Source_System_Code
,CPTD.DW_Last_Update_Date_Time
from EDWCR_BASE_VIEWS.Cancer_Patient_Tumor_Driver CPTD
Left Outer  Join

( Select
CPT.CR_Patient_Id
,CPT.TUMOR_PRIMARY_SITE_ID
,t26.Lookup_Desc as Best_CS_Summary_Desc
,t46.Lookup_Desc as Best_CS_TNM_Desc
,CPT.Tumor_Size_Num_Text 
,CPS.Cancer_Stage_Classification_Method_Code
,CPS.Cancer_Stage_Type_Code
,CPS.Cancer_Stage_Result_Text
,RAS.AJCC_Stage_Desc
,t43.Lookup_Desc as Tumor_Size_Summary_Desc
From EDWCR_BASE_VIEWS.CR_PATIENT_TUMOR CPT
Left Outer join
edwcr_base_views.CR_Patient_Staging CPS
on CPT.Tumor_Id = CPS.Tumor_Id
Left Outer Join Ref_AJCC_Stage RAS
on CPS.AJCC_Stage_Id = RAS.AJCC_Stage_Id
left outer join edwcr_base_views.ref_lookup_code t26
on CPT.Best_CS_Summary_Id = t26.Master_Lookup_Sid
and t26.Lookup_Id =26
left outer join edwcr_base_views.ref_lookup_code t46
on CPT.Best_CS_TNM_Id = t46.Master_Lookup_Sid
and t46.Lookup_Id =46
left outer join edwcr_base_views.ref_lookup_code t43
on CPT.Tumor_Size_Summary_Id = t43.Master_Lookup_Sid
and t43.Lookup_Id =43
where  CPS.Cancer_Stage_Classification_Method_Code='P'  and CPS.Cancer_Stage_Type_Code is null
) CR
ON CPTD.CR_PATIENT_ID=CR.CR_PATIENT_ID
AND CPTD.CR_TUMOR_PRIMARY_SITE_ID=CR.TUMOR_PRIMARY_SITE_ID
Left Outer Join edwcr_base_views.cn_patient_staging CNPS
on CPTD.CN_PATIENT_ID=CNPS.Nav_PATIENT_ID
and CPTD.CN_Tumor_Type_Id=CNPS.Tumor_Type_Id
and CNPS.Cancer_Stage_Class_Method_Code='P' and CNPS.Cancer_Staging_Type_Code is null
where CR.CR_Patient_Id is not null or CNPS.Nav_PATIENT_ID is not null
)a
)A"


export AC_ACT_SQL_STATEMENT="Select 'J_CN_REG_CANCER_PATIENT_TUMOR_STAGING'||','|| cast(count(*) as varchar(20))||',' as SOURCE_STRING from ${NCR_TGT_SCHEMA}.CANCER_PATIENT_TUMOR_STAGING"
