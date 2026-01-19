#  @@START DMEXPRESS_EXPORTED_VARIABLES

export AC_EXP_TOLERANCE_PERCENT=0
export AC_EXP_TOLERANCE_AMT=0
export JOBNAME='J_IM_Lawson_Person_IDMart'

export AC_EXP_SQL_STATEMENT="select 'J_IM_Lawson_Person_IDMart'||','||cast(count(*) as varchar(20))||',' AS SOURCE_STRING
FROM
(
SELECT
M2.IM_Domain_Id, 
M2.Lawson_Person_User_Id, 
M2.Lawson_Empl_Status_Code,
M2.Lawson_Person_Supv_User_Id, 
M2.Lawson_Person_Empl_Num, 
M2.Lawson_Person_Action_Name,
M2.Lawson_Person_Hire_Date, 
M2.Lawson_Person_Posn_Start_Date, 
M2.Lawson_Person_Termn_Date,
M2.Lawson_Person_Last_Name, 
M2.Lawson_Person_First_Name, 
M2.Lawson_Person_Middle_Name,
M2.Lawson_Person_Department_Code,
M2.Lawson_Person_Job_Class_Code,
M2.Source_System_Code,
M2.DW_Last_Update_Date_Time

FROM 
(
        SELECT 
		M1.IM_Domain_Id,
		M1.Lawson_Person_User_Id,
		M1.Lawson_Empl_Status_Code,
		M1.Lawson_Person_Supv_User_Id,
		M1.Lawson_Person_Empl_Num,
		M1.Lawson_Person_Action_Name,
		M1.Lawson_Person_Hire_Date,
		M1.Lawson_Person_Posn_Start_Date,
		M1.Lawson_Person_Termn_Date,
		M1.Lawson_Person_Last_Name,
		M1.Lawson_Person_First_Name,
		M1.Lawson_Person_Middle_Name,
		M1.Lawson_Person_Department_Code,
		M1.Lawson_Person_Job_Class_Code,
		M1.Source_System_Code,
		M1.DW_Last_Update_Date_Time,
		 ROW_NUMBER() OVER (PARTITION BY  M1.Lawson_Person_User_Id
											 ORDER BY  M1.Status_Rank, M1.Lawson_Person_Termn_Date desc,
														M1.Lawson_Person_Hire_Date desc, M1.PRCS_LVL desc) AS Row_Rank

				FROM 
					(
					SELECT

					'21' AS IM_Domain_Id,
					t2.CMPY_NUM AS Lawson_Person_User_Id,
					t1.EMPL_STTS AS Lawson_Empl_Status_Code,
					t3.CMPY_NUM AS Lawson_Person_Supv_User_Id,
					t1.EMPL AS Lawson_Person_Empl_Num,
					t4.ACTN_CD AS Lawson_Person_Action_Name,
					t1.HIRE_DT AS Lawson_Person_Hire_Date,
					t1.HIRE_DT AS Lawson_Person_Posn_Start_Date,
					t1.TERMN_DT AS Lawson_Person_Termn_Date,
					t1.LST_NM AS Lawson_Person_Last_Name,
					t1.FRST_NM AS Lawson_Person_First_Name,
					t1.MIDL_NM AS Lawson_Person_Middle_Name,
					t5.DEPT AS Lawson_Person_Department_Code,
					t5.JOB_CL AS Lawson_Person_Job_Class_Code,
					t1.PRCS_LVL,
					'L' AS Source_System_Code,
					CURRENT_TIMESTAMP(0) AS DW_Last_Update_Date_Time,

					CASE
						WHEN t1.EMPL_STTS = '01' and COALESCE(CAST(t1.TERMN_DT AS varchar(255)), '2000/01/01'  ) =   '2000/01/01' 
							THEN 1
						WHEN t1.EMPL_STTS = '02' and COALESCE(CAST(t1.TERMN_DT AS varchar(255)), '2000/01/01'  ) =   '2000/01/01' 
							THEN 2
						WHEN t1.EMPL_STTS = '03' and COALESCE(CAST(t1.TERMN_DT AS varchar(255)), '2000/01/01'  ) =   '2000/01/01' 
							THEN 3
						WHEN t1.EMPL_STTS = '04' and COALESCE(CAST(t1.TERMN_DT AS varchar(255)), '2000/01/01'  ) =   '2000/01/01' 
							THEN 4
						WHEN t1.EMPL_STTS = '05' and COALESCE(CAST(t1.TERMN_DT AS varchar(255)), '2000/01/01'  ) =   '2000/01/01' 
							THEN 5
						WHEN t1.EMPL_STTS = '06' and COALESCE(CAST(t1.TERMN_DT AS varchar(255)), '2000/01/01'  ) =   '2000/01/01' 
							THEN 6
						WHEN t1.EMPL_STTS = '99' and COALESCE(CAST(t1.TERMN_DT AS varchar(255)), '2000/01/01'  ) =   '2000/01/01' 
							THEN 7													
						WHEN t1.EMPL_STTS = '08' and COALESCE(CAST(t1.TERMN_DT AS varchar(255)), '2000/01/01'  ) <>   '2000/01/01' 
							THEN 8
						WHEN t1.EMPL_STTS = '07' and COALESCE(CAST(t1.TERMN_DT AS varchar(255)), '2000/01/01'  ) <>   '2000/01/01'
							THEN 9
						WHEN t1.EMPL_STTS = '99' and COALESCE(CAST(t1.TERMN_DT AS varchar(255)), '2000/01/01'  ) <>   '2000/01/01'
							THEN 10						
						ELSE 11			
					END AS Status_Rank

				FROM EDWIM_Staging.Lawsn_eSAF_Employee t1
					JOIN EDWIM_Staging.Lawsn_eSAF_Personnel_Employee t2
					  ON t2.EMPL = t1.EMPL
					 AND t2.CMPY = t1.CMPY

					 LEFT
					 JOIN 
						 (
						 SELECT
						 
							s1.CMPY,
							s1.SUPV_CD,
							s2.CMPY_NUM

							FROM EDWIM_Staging.Lawsn_eSAF_Supervisor s1
							JOIN EDWIM_Staging.Lawsn_eSAF_Personnel_Employee s2
							  ON s2.EMPL = s1.EMPL

						 )t3
					 ON  t1.CMPY = t3.CMPY 
					 AND t1.SUPV = t3.SUPV_CD 

					 LEFT
					 JOIN
						(
							SELECT
							
							    s3.ACTN_CD,
								s3.CMPY,							
								s3.EMPL

								FROM 
									(							
										SELECT 

										is3.ACTN_CD,
										is3.CMPY,							
										is3.EMPL,

										 ROW_NUMBER() OVER (PARTITION BY is3.EMPL
											 ORDER BY  is3.EFF_DT desc) AS Row_Rank														

										FROM EDWIM_Staging.Lawsn_eSAF_Personnel_Action is3	
																															
									)s3
						WHERE s3.Row_Rank = 1

						)t4
					 ON t1.CMPY = t4.CMPY
					 AND t1.EMPL = t4.EMPL
 
					 LEFT
					 JOIN
						 (
									  SELECT
									  
										s4.CMPY,
										s4.POSN,
										s4.DEPT,										
										s4.JOB_CD,
										s4.JOB_CL
										
									FROM
									(
											SELECT DISTINCT 

												is4.CMPY,
												is4.POSN,
												is4.DEPT,												
												is4.JOB_CD,
												is5.JOB_CL,

												ROW_NUMBER() OVER (PARTITION BY is4.CMPY, is4.POSN
																 ORDER BY  is4.EFF_DT desc) AS Row_Rank

												FROM EDWIM_Staging.Lawsn_eSAF_Personnel_Position is4
												JOIN EDWIM_Staging.Lawsn_eSAF_Job is5
												  ON is4.CMPY = is5.CMPY
												 AND is4.JOB_CD = is5.JOB_CD												 
									 )s4
								 WHERE s4.Row_Rank = 1
						 )t5
					  ON t1.CMPY = t5.CMPY
					  AND t1.POSN = t5.POSN

		)M1
		
	)M2
	
	WHERE    M2.Row_Rank = 1

       ) A;" 

export AC_ACT_SQL_STATEMENT="select 'J_IM_Lawson_Person_IDMart'||','||cast(count(*) as varchar(20))||',' AS SOURCE_STRING from EDWIM.Lawson_Person;"


#  @@END DMEXPRESS_EXPORTED_VARIABLES
#  End of Exported variables section
#
