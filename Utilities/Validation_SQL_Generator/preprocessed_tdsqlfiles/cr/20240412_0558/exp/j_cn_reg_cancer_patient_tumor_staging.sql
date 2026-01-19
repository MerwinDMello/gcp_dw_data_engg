SELECT CONCAT(count(*)) AS SOURCE_STRING
FROM (sel ROW_NUMBER() OVER(
 ORDER BY Cancer_Patient_Tumor_Driver_SK ASC ,Cancer_Patient_Driver_SK ASC , Cancer_Tumor_Driver_SK ASC , Cancer_Stage_Class_Method_Code, Cancer_Stage_Type_Code ASC) AS Cancer_Patient_Tumor_Staging_SK,
 Cancer_Patient_Tumor_Driver_SK,
 Cancer_Patient_Driver_SK,
 Cancer_Tumor_Driver_SK,
 Coid,
 Company_Code,
 Best_CS_Summary_Desc,
 Best_CS_TNM_Desc,
 Tumor_Size_Num_Text,
 Cancer_Stage_Code,
 Cancer_Stage_Class_Method_Code,
 Cancer_Staging_Type_Code AS Cancer_Stage_Type_Code,
 Cancer_Stage_Result_Text,
 AJCC_Stage_Desc,
 Tumor_Size_Summary_Desc,
 Source_System_Code,
 CURRENT_TIMESTAMP(0) AS DW_Last_Update_Date_Time
 FROM
 (SELECT DISTINCT CPTD.Cancer_Patient_Tumor_Driver_SK ,
 CPTD.Cancer_Patient_Driver_SK ,
 CPTD.Cancer_Tumor_Driver_SK ,
 CPTD.Coid ,
 CPTD.Company_Code ,
 CR.Best_CS_Summary_Desc ,
 CR.Best_CS_TNM_Desc ,
 CR.Tumor_Size_Num_Text ,
 CNPS.Cancer_Stage_Code ,
 coalesce(CR.Cancer_Stage_Classification_Method_Code, CNPS.Cancer_Stage_Class_Method_Code) AS Cancer_Stage_Class_Method_Code ,
 coalesce(CR.Cancer_Stage_Type_Code, CNPS.Cancer_Staging_Type_Code) AS Cancer_Staging_Type_Code ,
 coalesce(CR.Cancer_Stage_Result_Text, CNPS.Cancer_Staging_Result_Code) AS Cancer_Stage_Result_Text ,
 CR.AJCC_Stage_Desc ,
 CR.Tumor_Size_Summary_Desc ,
 CPTD.Source_System_Code ,
 CPTD.DW_Last_Update_Date_Time
 FROM EDWCR_BASE_VIEWS.Cancer_Patient_Tumor_Driver CPTD
 LEFT OUTER JOIN
 (SELECT CPT.CR_Patient_Id ,
 CPT.TUMOR_PRIMARY_SITE_ID ,
 t26.Lookup_Desc AS Best_CS_Summary_Desc ,
 t46.Lookup_Desc AS Best_CS_TNM_Desc ,
 CPT.Tumor_Size_Num_Text ,
 CPS.Cancer_Stage_Classification_Method_Code ,
 CPS.Cancer_Stage_Type_Code ,
 CPS.Cancer_Stage_Result_Text ,
 RAS.AJCC_Stage_Desc ,
 t43.Lookup_Desc AS Tumor_Size_Summary_Desc
 FROM EDWCR_BASE_VIEWS.CR_PATIENT_TUMOR CPT
 LEFT OUTER JOIN edwcr_base_views.CR_Patient_Staging CPS ON CPT.Tumor_Id = CPS.Tumor_Id
 LEFT OUTER JOIN Ref_AJCC_Stage RAS ON CPS.AJCC_Stage_Id = RAS.AJCC_Stage_Id
 LEFT OUTER JOIN edwcr_base_views.ref_lookup_code t26 ON CPT.Best_CS_Summary_Id = t26.Master_Lookup_Sid
 AND t26.Lookup_Id =26
 LEFT OUTER JOIN edwcr_base_views.ref_lookup_code t46 ON CPT.Best_CS_TNM_Id = t46.Master_Lookup_Sid
 AND t46.Lookup_Id =46
 LEFT OUTER JOIN edwcr_base_views.ref_lookup_code t43 ON CPT.Tumor_Size_Summary_Id = t43.Master_Lookup_Sid
 AND t43.Lookup_Id =43
 WHERE CPS.Cancer_Stage_Classification_Method_Code='C'
 AND CPS.Cancer_Stage_Type_Code='T' ) CR ON CPTD.CR_PATIENT_ID=CR.CR_PATIENT_ID
 AND CPTD.CR_TUMOR_PRIMARY_SITE_ID=CR.TUMOR_PRIMARY_SITE_ID
 LEFT OUTER JOIN edwcr_base_views.cn_patient_staging CNPS ON CPTD.CN_PATIENT_ID=CNPS.Nav_PATIENT_ID
 AND CPTD.CN_Tumor_Type_Id=CNPS.Tumor_Type_Id
 AND CNPS.Cancer_Stage_Class_Method_Code='C'
 AND CNPS.Cancer_Staging_Type_Code='T'
 WHERE CR.CR_Patient_Id IS NOT NULL
 OR CNPS.Nav_PATIENT_ID IS NOT NULL
 UNION ALL SELECT DISTINCT CPTD.Cancer_Patient_Tumor_Driver_SK ,
 CPTD.Cancer_Patient_Driver_SK ,
 CPTD.Cancer_Tumor_Driver_SK ,
 CPTD.Coid ,
 CPTD.Company_Code ,
 CR.Best_CS_Summary_Desc ,
 CR.Best_CS_TNM_Desc ,
 CR.Tumor_Size_Num_Text ,
 CNPS.Cancer_Stage_Code ,
 coalesce(CR.Cancer_Stage_Classification_Method_Code, CNPS.Cancer_Stage_Class_Method_Code) AS Cancer_Stage_Class_Method_Code ,
 coalesce(CR.Cancer_Stage_Type_Code, CNPS.Cancer_Staging_Type_Code) AS Cancer_Staging_Type_Code ,
 coalesce(CR.Cancer_Stage_Result_Text, CNPS.Cancer_Staging_Result_Code) AS Cancer_Stage_Result_Text ,
 CR.AJCC_Stage_Desc ,
 CR.Tumor_Size_Summary_Desc ,
 CPTD.Source_System_Code ,
 CPTD.DW_Last_Update_Date_Time
 FROM EDWCR_BASE_VIEWS.Cancer_Patient_Tumor_Driver CPTD
 LEFT OUTER JOIN
 (SELECT CPT.CR_Patient_Id ,
 CPT.TUMOR_PRIMARY_SITE_ID ,
 t26.Lookup_Desc AS Best_CS_Summary_Desc ,
 t46.Lookup_Desc AS Best_CS_TNM_Desc ,
 CPT.Tumor_Size_Num_Text ,
 CPS.Cancer_Stage_Classification_Method_Code ,
 CPS.Cancer_Stage_Type_Code ,
 CPS.Cancer_Stage_Result_Text ,
 RAS.AJCC_Stage_Desc ,
 t43.Lookup_Desc AS Tumor_Size_Summary_Desc
 FROM EDWCR_BASE_VIEWS.CR_PATIENT_TUMOR CPT
 LEFT OUTER JOIN edwcr_base_views.CR_Patient_Staging CPS ON CPT.Tumor_Id = CPS.Tumor_Id
 LEFT OUTER JOIN Ref_AJCC_Stage RAS ON CPS.AJCC_Stage_Id = RAS.AJCC_Stage_Id
 LEFT OUTER JOIN edwcr_base_views.ref_lookup_code t26 ON CPT.Best_CS_Summary_Id = t26.Master_Lookup_Sid
 AND t26.Lookup_Id =26
 LEFT OUTER JOIN edwcr_base_views.ref_lookup_code t46 ON CPT.Best_CS_TNM_Id = t46.Master_Lookup_Sid
 AND t46.Lookup_Id =46
 LEFT OUTER JOIN edwcr_base_views.ref_lookup_code t43 ON CPT.Tumor_Size_Summary_Id = t43.Master_Lookup_Sid
 AND t43.Lookup_Id =43
 WHERE CPS.Cancer_Stage_Classification_Method_Code='C'
 AND CPS.Cancer_Stage_Type_Code='N' ) CR ON CPTD.CR_PATIENT_ID=CR.CR_PATIENT_ID
 AND CPTD.CR_TUMOR_PRIMARY_SITE_ID=CR.TUMOR_PRIMARY_SITE_ID
 LEFT OUTER JOIN edwcr_base_views.cn_patient_staging CNPS ON CPTD.CN_PATIENT_ID=CNPS.Nav_PATIENT_ID
 AND CPTD.CN_Tumor_Type_Id=CNPS.Tumor_Type_Id
 AND CNPS.Cancer_Stage_Class_Method_Code='C'
 AND CNPS.Cancer_Staging_Type_Code='N'
 WHERE CR.CR_Patient_Id IS NOT NULL
 OR CNPS.Nav_PATIENT_ID IS NOT NULL
 UNION ALL SELECT DISTINCT CPTD.Cancer_Patient_Tumor_Driver_SK ,
 CPTD.Cancer_Patient_Driver_SK ,
 CPTD.Cancer_Tumor_Driver_SK ,
 CPTD.Coid ,
 CPTD.Company_Code ,
 CR.Best_CS_Summary_Desc ,
 CR.Best_CS_TNM_Desc ,
 CR.Tumor_Size_Num_Text ,
 CNPS.Cancer_Stage_Code ,
 coalesce(CR.Cancer_Stage_Classification_Method_Code, CNPS.Cancer_Stage_Class_Method_Code) AS Cancer_Stage_Class_Method_Code ,
 coalesce(CR.Cancer_Stage_Type_Code, CNPS.Cancer_Staging_Type_Code) AS Cancer_Staging_Type_Code ,
 coalesce(CR.Cancer_Stage_Result_Text, CNPS.Cancer_Staging_Result_Code) AS Cancer_Stage_Result_Text ,
 CR.AJCC_Stage_Desc ,
 CR.Tumor_Size_Summary_Desc ,
 CPTD.Source_System_Code ,
 CPTD.DW_Last_Update_Date_Time
 FROM EDWCR_BASE_VIEWS.Cancer_Patient_Tumor_Driver CPTD
 LEFT OUTER JOIN
 (SELECT CPT.CR_Patient_Id ,
 CPT.TUMOR_PRIMARY_SITE_ID ,
 t26.Lookup_Desc AS Best_CS_Summary_Desc ,
 t46.Lookup_Desc AS Best_CS_TNM_Desc ,
 CPT.Tumor_Size_Num_Text ,
 CPS.Cancer_Stage_Classification_Method_Code ,
 CPS.Cancer_Stage_Type_Code ,
 CPS.Cancer_Stage_Result_Text ,
 RAS.AJCC_Stage_Desc ,
 t43.Lookup_Desc AS Tumor_Size_Summary_Desc
 FROM EDWCR_BASE_VIEWS.CR_PATIENT_TUMOR CPT
 LEFT OUTER JOIN edwcr_base_views.CR_Patient_Staging CPS ON CPT.Tumor_Id = CPS.Tumor_Id
 LEFT OUTER JOIN Ref_AJCC_Stage RAS ON CPS.AJCC_Stage_Id = RAS.AJCC_Stage_Id
 LEFT OUTER JOIN edwcr_base_views.ref_lookup_code t26 ON CPT.Best_CS_Summary_Id = t26.Master_Lookup_Sid
 AND t26.Lookup_Id =26
 LEFT OUTER JOIN edwcr_base_views.ref_lookup_code t46 ON CPT.Best_CS_TNM_Id = t46.Master_Lookup_Sid
 AND t46.Lookup_Id =46
 LEFT OUTER JOIN edwcr_base_views.ref_lookup_code t43 ON CPT.Tumor_Size_Summary_Id = t43.Master_Lookup_Sid
 AND t43.Lookup_Id =43
 WHERE CPS.Cancer_Stage_Classification_Method_Code='C'
 AND CPS.Cancer_Stage_Type_Code='M' ) CR ON CPTD.CR_PATIENT_ID=CR.CR_PATIENT_ID
 AND CPTD.CR_TUMOR_PRIMARY_SITE_ID=CR.TUMOR_PRIMARY_SITE_ID
 LEFT OUTER JOIN edwcr_base_views.cn_patient_staging CNPS ON CPTD.CN_PATIENT_ID=CNPS.Nav_PATIENT_ID
 AND CPTD.CN_Tumor_Type_Id=CNPS.Tumor_Type_Id
 AND CNPS.Cancer_Stage_Class_Method_Code='C'
 AND CNPS.Cancer_Staging_Type_Code='M'
 WHERE CR.CR_Patient_Id IS NOT NULL
 OR CNPS.Nav_PATIENT_ID IS NOT NULL
 UNION ALL SELECT DISTINCT CPTD.Cancer_Patient_Tumor_Driver_SK ,
 CPTD.Cancer_Patient_Driver_SK ,
 CPTD.Cancer_Tumor_Driver_SK ,
 CPTD.Coid ,
 CPTD.Company_Code ,
 CR.Best_CS_Summary_Desc ,
 CR.Best_CS_TNM_Desc ,
 CR.Tumor_Size_Num_Text ,
 CNPS.Cancer_Stage_Code ,
 coalesce(CR.Cancer_Stage_Classification_Method_Code, CNPS.Cancer_Stage_Class_Method_Code) AS Cancer_Stage_Class_Method_Code ,
 coalesce(CR.Cancer_Stage_Type_Code, CNPS.Cancer_Staging_Type_Code) AS Cancer_Staging_Type_Code ,
 coalesce(CR.Cancer_Stage_Result_Text, CNPS.Cancer_Staging_Result_Code) AS Cancer_Stage_Result_Text ,
 CR.AJCC_Stage_Desc ,
 CR.Tumor_Size_Summary_Desc ,
 CPTD.Source_System_Code ,
 CPTD.DW_Last_Update_Date_Time
 FROM EDWCR_BASE_VIEWS.Cancer_Patient_Tumor_Driver CPTD
 LEFT OUTER JOIN
 (SELECT CPT.CR_Patient_Id ,
 CPT.TUMOR_PRIMARY_SITE_ID ,
 t26.Lookup_Desc AS Best_CS_Summary_Desc ,
 t46.Lookup_Desc AS Best_CS_TNM_Desc ,
 CPT.Tumor_Size_Num_Text ,
 CPS.Cancer_Stage_Classification_Method_Code ,
 CPS.Cancer_Stage_Type_Code ,
 CPS.Cancer_Stage_Result_Text ,
 RAS.AJCC_Stage_Desc ,
 t43.Lookup_Desc AS Tumor_Size_Summary_Desc
 FROM EDWCR_BASE_VIEWS.CR_PATIENT_TUMOR CPT
 LEFT OUTER JOIN edwcr_base_views.CR_Patient_Staging CPS ON CPT.Tumor_Id = CPS.Tumor_Id
 LEFT OUTER JOIN Ref_AJCC_Stage RAS ON CPS.AJCC_Stage_Id = RAS.AJCC_Stage_Id
 LEFT OUTER JOIN edwcr_base_views.ref_lookup_code t26 ON CPT.Best_CS_Summary_Id = t26.Master_Lookup_Sid
 AND t26.Lookup_Id =26
 LEFT OUTER JOIN edwcr_base_views.ref_lookup_code t46 ON CPT.Best_CS_TNM_Id = t46.Master_Lookup_Sid
 AND t46.Lookup_Id =46
 LEFT OUTER JOIN edwcr_base_views.ref_lookup_code t43 ON CPT.Tumor_Size_Summary_Id = t43.Master_Lookup_Sid
 AND t43.Lookup_Id =43
 WHERE CPS.Cancer_Stage_Classification_Method_Code='C'
 AND CPS.Cancer_Stage_Type_Code IS NULL ) CR ON CPTD.CR_PATIENT_ID=CR.CR_PATIENT_ID
 AND CPTD.CR_TUMOR_PRIMARY_SITE_ID=CR.TUMOR_PRIMARY_SITE_ID
 LEFT OUTER JOIN edwcr_base_views.cn_patient_staging CNPS ON CPTD.CN_PATIENT_ID=CNPS.Nav_PATIENT_ID
 AND CPTD.CN_Tumor_Type_Id=CNPS.Tumor_Type_Id
 AND CNPS.Cancer_Stage_Class_Method_Code='C'
 AND CNPS.Cancer_Staging_Type_Code IS NULL
 WHERE CR.CR_Patient_Id IS NOT NULL
 OR CNPS.Nav_PATIENT_ID IS NOT NULL
 UNION ALL SELECT DISTINCT CPTD.Cancer_Patient_Tumor_Driver_SK ,
 CPTD.Cancer_Patient_Driver_SK ,
 CPTD.Cancer_Tumor_Driver_SK ,
 CPTD.Coid ,
 CPTD.Company_Code ,
 CR.Best_CS_Summary_Desc ,
 CR.Best_CS_TNM_Desc ,
 CR.Tumor_Size_Num_Text ,
 CNPS.Cancer_Stage_Code ,
 coalesce(CR.Cancer_Stage_Classification_Method_Code, CNPS.Cancer_Stage_Class_Method_Code) AS Cancer_Stage_Class_Method_Code ,
 coalesce(CR.Cancer_Stage_Type_Code, CNPS.Cancer_Staging_Type_Code) AS Cancer_Staging_Type_Code ,
 coalesce(CR.Cancer_Stage_Result_Text, CNPS.Cancer_Staging_Result_Code) AS Cancer_Stage_Result_Text ,
 CR.AJCC_Stage_Desc ,
 CR.Tumor_Size_Summary_Desc ,
 CPTD.Source_System_Code ,
 CPTD.DW_Last_Update_Date_Time
 FROM EDWCR_BASE_VIEWS.Cancer_Patient_Tumor_Driver CPTD
 LEFT OUTER JOIN
 (SELECT CPT.CR_Patient_Id ,
 CPT.TUMOR_PRIMARY_SITE_ID ,
 t26.Lookup_Desc AS Best_CS_Summary_Desc ,
 t46.Lookup_Desc AS Best_CS_TNM_Desc ,
 CPT.Tumor_Size_Num_Text ,
 CPS.Cancer_Stage_Classification_Method_Code ,
 CPS.Cancer_Stage_Type_Code ,
 CPS.Cancer_Stage_Result_Text ,
 RAS.AJCC_Stage_Desc ,
 t43.Lookup_Desc AS Tumor_Size_Summary_Desc
 FROM EDWCR_BASE_VIEWS.CR_PATIENT_TUMOR CPT
 LEFT OUTER JOIN edwcr_base_views.CR_Patient_Staging CPS ON CPT.Tumor_Id = CPS.Tumor_Id
 LEFT OUTER JOIN Ref_AJCC_Stage RAS ON CPS.AJCC_Stage_Id = RAS.AJCC_Stage_Id
 LEFT OUTER JOIN edwcr_base_views.ref_lookup_code t26 ON CPT.Best_CS_Summary_Id = t26.Master_Lookup_Sid
 AND t26.Lookup_Id =26
 LEFT OUTER JOIN edwcr_base_views.ref_lookup_code t46 ON CPT.Best_CS_TNM_Id = t46.Master_Lookup_Sid
 AND t46.Lookup_Id =46
 LEFT OUTER JOIN edwcr_base_views.ref_lookup_code t43 ON CPT.Tumor_Size_Summary_Id = t43.Master_Lookup_Sid
 AND t43.Lookup_Id =43
 WHERE CPS.Cancer_Stage_Classification_Method_Code='P'
 AND CPS.Cancer_Stage_Type_Code='T' ) CR ON CPTD.CR_PATIENT_ID=CR.CR_PATIENT_ID
 AND CPTD.CR_TUMOR_PRIMARY_SITE_ID=CR.TUMOR_PRIMARY_SITE_ID
 LEFT OUTER JOIN edwcr_base_views.cn_patient_staging CNPS ON CPTD.CN_PATIENT_ID=CNPS.Nav_PATIENT_ID
 AND CPTD.CN_Tumor_Type_Id=CNPS.Tumor_Type_Id
 AND CNPS.Cancer_Stage_Class_Method_Code='P'
 AND CNPS.Cancer_Staging_Type_Code='T'
 WHERE CR.CR_Patient_Id IS NOT NULL
 OR CNPS.Nav_PATIENT_ID IS NOT NULL
 UNION ALL SELECT DISTINCT CPTD.Cancer_Patient_Tumor_Driver_SK ,
 CPTD.Cancer_Patient_Driver_SK ,
 CPTD.Cancer_Tumor_Driver_SK ,
 CPTD.Coid ,
 CPTD.Company_Code ,
 CR.Best_CS_Summary_Desc ,
 CR.Best_CS_TNM_Desc ,
 CR.Tumor_Size_Num_Text ,
 CNPS.Cancer_Stage_Code ,
 coalesce(CR.Cancer_Stage_Classification_Method_Code, CNPS.Cancer_Stage_Class_Method_Code) AS Cancer_Stage_Class_Method_Code ,
 coalesce(CR.Cancer_Stage_Type_Code, CNPS.Cancer_Staging_Type_Code) AS Cancer_Staging_Type_Code ,
 coalesce(CR.Cancer_Stage_Result_Text, CNPS.Cancer_Staging_Result_Code) AS Cancer_Stage_Result_Text ,
 CR.AJCC_Stage_Desc ,
 CR.Tumor_Size_Summary_Desc ,
 CPTD.Source_System_Code ,
 CPTD.DW_Last_Update_Date_Time
 FROM EDWCR_BASE_VIEWS.Cancer_Patient_Tumor_Driver CPTD
 LEFT OUTER JOIN
 (SELECT CPT.CR_Patient_Id ,
 CPT.TUMOR_PRIMARY_SITE_ID ,
 t26.Lookup_Desc AS Best_CS_Summary_Desc ,
 t46.Lookup_Desc AS Best_CS_TNM_Desc ,
 CPT.Tumor_Size_Num_Text ,
 CPS.Cancer_Stage_Classification_Method_Code ,
 CPS.Cancer_Stage_Type_Code ,
 CPS.Cancer_Stage_Result_Text ,
 RAS.AJCC_Stage_Desc ,
 t43.Lookup_Desc AS Tumor_Size_Summary_Desc
 FROM EDWCR_BASE_VIEWS.CR_PATIENT_TUMOR CPT
 LEFT OUTER JOIN edwcr_base_views.CR_Patient_Staging CPS ON CPT.Tumor_Id = CPS.Tumor_Id
 LEFT OUTER JOIN Ref_AJCC_Stage RAS ON CPS.AJCC_Stage_Id = RAS.AJCC_Stage_Id
 LEFT OUTER JOIN edwcr_base_views.ref_lookup_code t26 ON CPT.Best_CS_Summary_Id = t26.Master_Lookup_Sid
 AND t26.Lookup_Id =26
 LEFT OUTER JOIN edwcr_base_views.ref_lookup_code t46 ON CPT.Best_CS_TNM_Id = t46.Master_Lookup_Sid
 AND t46.Lookup_Id =46
 LEFT OUTER JOIN edwcr_base_views.ref_lookup_code t43 ON CPT.Tumor_Size_Summary_Id = t43.Master_Lookup_Sid
 AND t43.Lookup_Id =43
 WHERE CPS.Cancer_Stage_Classification_Method_Code='P'
 AND CPS.Cancer_Stage_Type_Code='N' ) CR ON CPTD.CR_PATIENT_ID=CR.CR_PATIENT_ID
 AND CPTD.CR_TUMOR_PRIMARY_SITE_ID=CR.TUMOR_PRIMARY_SITE_ID
 LEFT OUTER JOIN edwcr_base_views.cn_patient_staging CNPS ON CPTD.CN_PATIENT_ID=CNPS.Nav_PATIENT_ID
 AND CPTD.CN_Tumor_Type_Id=CNPS.Tumor_Type_Id
 AND CNPS.Cancer_Stage_Class_Method_Code='P'
 AND CNPS.Cancer_Staging_Type_Code='N'
 WHERE CR.CR_Patient_Id IS NOT NULL
 OR CNPS.Nav_PATIENT_ID IS NOT NULL
 UNION ALL SELECT DISTINCT CPTD.Cancer_Patient_Tumor_Driver_SK ,
 CPTD.Cancer_Patient_Driver_SK ,
 CPTD.Cancer_Tumor_Driver_SK ,
 CPTD.Coid ,
 CPTD.Company_Code ,
 CR.Best_CS_Summary_Desc ,
 CR.Best_CS_TNM_Desc ,
 CR.Tumor_Size_Num_Text ,
 CNPS.Cancer_Stage_Code ,
 coalesce(CR.Cancer_Stage_Classification_Method_Code, CNPS.Cancer_Stage_Class_Method_Code) AS Cancer_Stage_Class_Method_Code ,
 coalesce(CR.Cancer_Stage_Type_Code, CNPS.Cancer_Staging_Type_Code) AS Cancer_Staging_Type_Code ,
 coalesce(CR.Cancer_Stage_Result_Text, CNPS.Cancer_Staging_Result_Code) AS Cancer_Stage_Result_Text ,
 CR.AJCC_Stage_Desc ,
 CR.Tumor_Size_Summary_Desc ,
 CPTD.Source_System_Code ,
 CPTD.DW_Last_Update_Date_Time
 FROM EDWCR_BASE_VIEWS.Cancer_Patient_Tumor_Driver CPTD
 LEFT OUTER JOIN
 (SELECT CPT.CR_Patient_Id ,
 CPT.TUMOR_PRIMARY_SITE_ID ,
 t26.Lookup_Desc AS Best_CS_Summary_Desc ,
 t46.Lookup_Desc AS Best_CS_TNM_Desc ,
 CPT.Tumor_Size_Num_Text ,
 CPS.Cancer_Stage_Classification_Method_Code ,
 CPS.Cancer_Stage_Type_Code ,
 CPS.Cancer_Stage_Result_Text ,
 RAS.AJCC_Stage_Desc ,
 t43.Lookup_Desc AS Tumor_Size_Summary_Desc
 FROM EDWCR_BASE_VIEWS.CR_PATIENT_TUMOR CPT
 LEFT OUTER JOIN edwcr_base_views.CR_Patient_Staging CPS ON CPT.Tumor_Id = CPS.Tumor_Id
 LEFT OUTER JOIN Ref_AJCC_Stage RAS ON CPS.AJCC_Stage_Id = RAS.AJCC_Stage_Id
 LEFT OUTER JOIN edwcr_base_views.ref_lookup_code t26 ON CPT.Best_CS_Summary_Id = t26.Master_Lookup_Sid
 AND t26.Lookup_Id =26
 LEFT OUTER JOIN edwcr_base_views.ref_lookup_code t46 ON CPT.Best_CS_TNM_Id = t46.Master_Lookup_Sid
 AND t46.Lookup_Id =46
 LEFT OUTER JOIN edwcr_base_views.ref_lookup_code t43 ON CPT.Tumor_Size_Summary_Id = t43.Master_Lookup_Sid
 AND t43.Lookup_Id =43
 WHERE CPS.Cancer_Stage_Classification_Method_Code='P'
 AND CPS.Cancer_Stage_Type_Code='M' ) CR ON CPTD.CR_PATIENT_ID=CR.CR_PATIENT_ID
 AND CPTD.CR_TUMOR_PRIMARY_SITE_ID=CR.TUMOR_PRIMARY_SITE_ID
 LEFT OUTER JOIN edwcr_base_views.cn_patient_staging CNPS ON CPTD.CN_PATIENT_ID=CNPS.Nav_PATIENT_ID
 AND CPTD.CN_Tumor_Type_Id=CNPS.Tumor_Type_Id
 AND CNPS.Cancer_Stage_Class_Method_Code='P'
 AND CNPS.Cancer_Staging_Type_Code='M'
 WHERE CR.CR_Patient_Id IS NOT NULL
 OR CNPS.Nav_PATIENT_ID IS NOT NULL
 UNION ALL SELECT DISTINCT CPTD.Cancer_Patient_Tumor_Driver_SK ,
 CPTD.Cancer_Patient_Driver_SK ,
 CPTD.Cancer_Tumor_Driver_SK ,
 CPTD.Coid ,
 CPTD.Company_Code ,
 CR.Best_CS_Summary_Desc ,
 CR.Best_CS_TNM_Desc ,
 CR.Tumor_Size_Num_Text ,
 CNPS.Cancer_Stage_Code ,
 coalesce(CR.Cancer_Stage_Classification_Method_Code, CNPS.Cancer_Stage_Class_Method_Code) AS Cancer_Stage_Class_Method_Code ,
 coalesce(CR.Cancer_Stage_Type_Code, CNPS.Cancer_Staging_Type_Code) AS Cancer_Staging_Type_Code ,
 coalesce(CR.Cancer_Stage_Result_Text, CNPS.Cancer_Staging_Result_Code) AS Cancer_Stage_Result_Text ,
 CR.AJCC_Stage_Desc ,
 CR.Tumor_Size_Summary_Desc ,
 CPTD.Source_System_Code ,
 CPTD.DW_Last_Update_Date_Time
 FROM EDWCR_BASE_VIEWS.Cancer_Patient_Tumor_Driver CPTD
 LEFT OUTER JOIN
 (SELECT CPT.CR_Patient_Id ,
 CPT.TUMOR_PRIMARY_SITE_ID ,
 t26.Lookup_Desc AS Best_CS_Summary_Desc ,
 t46.Lookup_Desc AS Best_CS_TNM_Desc ,
 CPT.Tumor_Size_Num_Text ,
 CPS.Cancer_Stage_Classification_Method_Code ,
 CPS.Cancer_Stage_Type_Code ,
 CPS.Cancer_Stage_Result_Text ,
 RAS.AJCC_Stage_Desc ,
 t43.Lookup_Desc AS Tumor_Size_Summary_Desc
 FROM EDWCR_BASE_VIEWS.CR_PATIENT_TUMOR CPT
 LEFT OUTER JOIN edwcr_base_views.CR_Patient_Staging CPS ON CPT.Tumor_Id = CPS.Tumor_Id
 LEFT OUTER JOIN Ref_AJCC_Stage RAS ON CPS.AJCC_Stage_Id = RAS.AJCC_Stage_Id
 LEFT OUTER JOIN edwcr_base_views.ref_lookup_code t26 ON CPT.Best_CS_Summary_Id = t26.Master_Lookup_Sid
 AND t26.Lookup_Id =26
 LEFT OUTER JOIN edwcr_base_views.ref_lookup_code t46 ON CPT.Best_CS_TNM_Id = t46.Master_Lookup_Sid
 AND t46.Lookup_Id =46
 LEFT OUTER JOIN edwcr_base_views.ref_lookup_code t43 ON CPT.Tumor_Size_Summary_Id = t43.Master_Lookup_Sid
 AND t43.Lookup_Id =43
 WHERE CPS.Cancer_Stage_Classification_Method_Code='P'
 AND CPS.Cancer_Stage_Type_Code IS NULL ) CR ON CPTD.CR_PATIENT_ID=CR.CR_PATIENT_ID
 AND CPTD.CR_TUMOR_PRIMARY_SITE_ID=CR.TUMOR_PRIMARY_SITE_ID
 LEFT OUTER JOIN edwcr_base_views.cn_patient_staging CNPS ON CPTD.CN_PATIENT_ID=CNPS.Nav_PATIENT_ID
 AND CPTD.CN_Tumor_Type_Id=CNPS.Tumor_Type_Id
 AND CNPS.Cancer_Stage_Class_Method_Code='P'
 AND CNPS.Cancer_Staging_Type_Code IS NULL
 WHERE CR.CR_Patient_Id IS NOT NULL
 OR CNPS.Nav_PATIENT_ID IS NOT NULL )a)A