BEGIN
  TRUNCATE TABLE {{ params.param_hr_stage_dataset_name }}.junc_submission_tracking_motive_wrk;
   INSERT INTO {{ params.param_hr_stage_dataset_name }}.junc_submission_tracking_motive_wrk (file_date, submission_tracking_sid, tracking_motive_id, source_system_code, dw_last_update_date_time)

SELECT
        stg.file_date AS file_date,
		
        st.submission_tracking_sid AS submission_tracking_sid,
		
        stg.cswmotive_number AS tracking_motive_id,
		
        'T' AS source_system_code,
		
        DATETIME_TRUNC(CURRENT_DATETIME('US/Central'), SECOND) AS dw_last_update_date_time
		
      FROM
       {{ params.param_hr_stage_dataset_name }}.taleo_apptrackingcswitem_cswmotives AS stg
		
        INNER JOIN {{ params.param_hr_base_views_dataset_name }}.submission_tracking AS st ON stg.atcswitem_number = st.submission_tracking_num
		
         AND st.valid_to_date = DATETIME("9999-12-31 23:59:59")
		 
         AND upper(st.source_system_code) = 'T'
		 
    UNION ALL
	
    SELECT
        CURRENT_DATE('US/Central') AS file_date,
		
        st.submission_tracking_sid AS submission_tracking_sid,
		
        rm.motive_id AS tracking_motive_id,
		
        'B' AS source_system_code,
		
        DATETIME_TRUNC(CURRENT_DATETIME('US/Central'), SECOND) AS dw_last_update_date_time
		
      FROM
	  
        (
		
          SELECT
		  
              stg.createstamp,---new change
			  
              cp.candidate_profile_sid,
			  
              rse.submission_event_id,
			  
              rss.step_id,
			  
              rw.workflow_id,
			 -- 'A' as tst   --- need to check why it was aded in deepak's query
			  
              --stg.candidatedispositionreason    -- column is not present in STG
			  stg_v3.candidatedispositionreason --uncommneted above column and referring to correct table
			  
            FROM
			
              {{ params.param_hr_stage_dataset_name }}.ats_cust_v2_jobapplicationworkflowstephistory_stg AS stg -- new change
			  
------------------Newly added logic to get atsworkflowstepkey and candidatedispositionreason columns-----------------------------------
			  
			   inner join {{ params.param_hr_stage_dataset_name }}.ats_cust_v3_jobapplication_stg stg_v3 on
			  
         stg.hrorganization = stg_v3.hrorganization AND
		 
            stg.candidate = stg_v3.candidate AND
			
            stg.jobrequisition = stg_v3.jobrequisition AND
			
            stg.jobapplication = stg_v3.jobapplication
			
              INNER JOIN {{ params.param_hr_stage_dataset_name }}.ats_cust_v2_atsworkflowstep_stg AS aws ON stg_v3.atsworkflowstepkey= aws.atsworkflowstepkey
			  
-----------------------end of Newly added logic to get atsworkflowstepkey and candidatedispositionreason columns--------------------------
			  
              INNER JOIN {{ params.param_hr_base_views_dataset_name }}.candidate_profile AS cp 
			  
			  ON concat(stg.jobapplication, stg.candidate, stg.jobrequisition) = concat(cp.job_application_num, cp.candidate_num, cp.requisition_num)
			  
               AND valid_to_date = DATETIME("9999-12-31 23:59:59")
			   
			    and CP.SOURCE_SYSTEM_CODE='B' -- newly added filter
			   
              /*
			  INNER JOIN {{ params.param_hr_base_views_dataset_name }}.ref_submission_event AS rse ON upper(trim(stg._action)) = upper(trim(rse.submission_event_code))
               AND upper(rse.source_system_code) = 'B'
			   */
			   
			   /*
              -- INNER JOIN{{ params.param_hr_stage_dataset_name }}.ats_cust_v2_atsworkflowstep_stg AS aws ON stg.collectiveagreement_cube_dimension_value= aws.atsworkflowstepkey
              INNER JOIN{{ params.param_hr_stage_dataset_name }}.ats_cust_v2_atsworkflowstep_stg AS aws ON stg.atsworkflowstepkey= aws.atsworkflowstepkey
			  */

              

            INNER JOIN  {{ params.param_hr_base_views_dataset_name }}.ref_submission_event_category RSEC -- new changes

                  ON upper(STG._Action) = upper(RSEC.Submission_Event_Category_Code)

                  AND RSEC.SOURCE_SYSTEM_CODE = 'B'

            INNER JOIN  {{ params.param_hr_base_views_dataset_name }}.ref_submission_event RSE --moved join position

                 ON STG.TRIGGEREDBY = RSE.SUBMISSION_EVENT_CODE

                AND RSEC.Submission_Event_Category_Id = RSE.Submission_Event_Category_Id -- Added new filter                                              

                AND RSE.SOURCE_SYSTEM_CODE = 'B'
				
				
				
              INNER JOIN  {{ params.param_hr_base_views_dataset_name }}.ref_submission_step  RSS

             ON CASE WHEN (RSS.STEP_CODE IS NULL OR Char_Length(Trim(RSS.STEP_CODE))=0) THEN 'AA' ELSE Trim(RSS.STEP_CODE) END =

            CASE WHEN (STG.atsworkflowcategory IS NULL OR Char_Length(Trim(STG.atsworkflowcategory))=0)

            THEN 'AA' ELSE Trim('1/'||STG.jobrequisitionworkflow|| '/' ||STG.atsworkflowcategory) END

            AND RSS.SOURCE_SYSTEM_CODE='B'  

			
            -- Replaced join logic

         INNER JOIN {{ params.param_hr_base_views_dataset_name }}.ref_workflow RW

        ON RW.WORKFLOW_CODE = '1/'||STG.jobrequisitionworkflow -- Changed from AWS.ATSWORKFLOW_CDV to '1/'||STG.ATSWORKFLOW

        AND RW.SOURCE_SYSTEM_CODE='B'

 

            GROUP BY 1, 2, 3, 4, 5,6

        ) AS stg
		
		
        INNER JOIN {{ params.param_hr_base_views_dataset_name }}.submission_tracking AS st ON stg.candidate_profile_sid = st.candidate_profile_sid
		
        --  AND cast(stg.updatestamp as datetime) = st.creation_date_time
		
		AND cast(STG.CREATESTAMP as datetime) = ST.CREATION_DATE_TIME
		
         AND stg.submission_event_id = st.submission_event_id
		 
         AND stg.step_id = st.tracking_step_id
		 
         AND stg.workflow_id = st.tracking_workflow_id
		 
         AND upper(st.source_system_code) = 'B'
		 
         AND st.valid_to_date = DATETIME("9999-12-31 23:59:59")
		 
        INNER JOIN {{ params.param_hr_base_views_dataset_name }}.ref_motive AS rm ON trim(stg.candidatedispositionreason) = trim(rm.motive_code)
         AND upper(rm.source_system_code) = 'B'
		 
		  ;
END;
         