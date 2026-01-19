BEGIN
  declare  
    current_ts datetime;
  set 
    current_ts = datetime_trunc(current_datetime('US/Central'), SECOND);
    
  TRUNCATE TABLE {{ params.param_hr_stage_dataset_name }}.hca_pat_resp;

  INSERT INTO {{ params.param_hr_stage_dataset_name }}.hca_pat_resp (hca_unique, surv_id, adj_samp, survey_type, pg_unit, disdate, recdate, mde, question_id, coid, resp, ptype, qtype, category, cms_rpt, dw_last_update_date_time)
    SELECT DISTINCT
        hca_pat_resp_all.hca_unique,
        hca_pat_resp_all.surv_id,
        hca_pat_resp_all.adj_samp,
        hca_pat_resp_all.survey_type,
        hca_pat_resp_all.pg_unit,
        hca_pat_resp_all.disdate,
        hca_pat_resp_all.recdate,
        hca_pat_resp_all.mde,
        hca_pat_resp_all.question_id,
        hca_pat_resp_all.coid,
        hca_pat_resp_all.response AS resp,
        hca_pat_resp_all.ptype,
        hca_pat_resp_all.qtype,
        hca_pat_resp_all.category,
        hca_pat_resp_all.cms_rpt,
        hca_pat_resp_all.dw_last_update_date_time
      FROM
        {{ params.param_hr_stage_dataset_name }}.hca_pat_resp_all
      WHERE upper(hca_pat_resp_all.question_id) <> '0'
       AND hca_pat_resp_all.coid IS NOT NULL
  ;

  TRUNCATE TABLE {{ params.param_hr_stage_dataset_name }}.hca_pat_resp_detail;

  INSERT INTO {{ params.param_hr_stage_dataset_name }}.hca_pat_resp_detail (hca_unique, survey_id, adj_samp, survey_type, pg_unit, dis_date, rec_date, resp_mode, quest_id, survey, client_id, last_name, middle_name, first_name, address_line1, address_line2, city, state, zipcode, phone, drg, gender, race, dob, lang, mrn, unique_id, loc_code, attending_phys_id, admission_source, admission_date, admission_time, discharge_date, discharge_time, discharge_status, unit, service, payor, length_of_stay, room, bed, is_hospitalist, fasttrack, email, hospitalist_id_1, hospitalist_id_2, is_er_admission, facility_id, unit_code_id, patient_type, medicare_provider_num, medicare_provider_npi, facility_name, nurse_station, drg_type, service_category, mdc, diag_category, patient_age, eligible, exclusion_flag, financial_plan, admission_phys_code, primary_icd9_code, icd_code_version, cpt_code, iped_flag, admission_type, company_code, diag_1, diag_2, diag_3, diag_4, diag_5, diag_6, diag_7, diag_8, diag_9, diag_10, diag_11, diag_12, diag_13, diag_14, diag_15, bill_generated_date, surg_proc_code_1, surg_proc_code_2, surg_proc_code_3, surgeon_name_1, surgeon_name_2, surgeon_name_3, attending_phys_name, hospitalist_flag, consulting_phys_id, consulting_phys_name, coid, iped, cpt_flag, division_coid, market_coid, surg_proc_code_1_icd, surg_proc_code_2_icd, surg_proc_code_3_icd, ethnicity, hipps_code, mode_of_arrivl, day_of_week, cpt_code_2, cpt_code_3, cpt_code_4, cpt_code_5, cpt_code_6, deceased_flag, no_publicity, prim_cpt_svc_date, ptype, qtype, category, dw_last_update_date_time)
    SELECT DISTINCT
        hca_pat_resp_all.hca_unique,
        hca_pat_resp_all.surv_id AS survey_id,
        hca_pat_resp_all.adj_samp,
        hca_pat_resp_all.survey_type,
        hca_pat_resp_all.pg_unit,
        hca_pat_resp_all.disdate AS dis_date,
        hca_pat_resp_all.recdate AS rec_date,
        hca_pat_resp_all.mde AS resp_mode,
        hca_pat_resp_all.question_id AS question_id,
        nullif(trim(substr(hca_pat_resp_all.response, 1, 8)), '') AS survey,
        nullif(trim(substr(hca_pat_resp_all.response, 9, 7)), '') AS client_id,
        nullif(trim(substr(hca_pat_resp_all.response, 16, 25)), '') AS last_name,
        nullif(trim(substr(hca_pat_resp_all.response, 41, 1)), '') AS middle_name,
        nullif(trim(substr(hca_pat_resp_all.response, 42, 20)), '') AS firstname,
        nullif(trim(substr(hca_pat_resp_all.response, 62, 40)), '') AS addressline1,
        nullif(trim(substr(hca_pat_resp_all.response, 102, 40)), '') AS addressline2,
        nullif(trim(substr(hca_pat_resp_all.response, 142, 25)), '') AS city,
        nullif(trim(substr(hca_pat_resp_all.response, 167, 2)), '') AS state,
        nullif(trim(substr(hca_pat_resp_all.response, 169, 10)), '') AS zipcode,
        nullif(trim(substr(hca_pat_resp_all.response, 179, 10)), '') AS phone,
        nullif(trim(substr(hca_pat_resp_all.response, 189, 3)), '') AS drg,
        CASE
          WHEN trim(substr(hca_pat_resp_all.response, 192, 1)) = '1' THEN 'M'
          WHEN trim(substr(hca_pat_resp_all.response, 192, 1)) = '2' THEN 'F'
          ELSE CAST(NULL as STRING)
        END AS gender,
        nullif(trim(substr(hca_pat_resp_all.response, 193, 1)), '') AS race,
        CASE
          WHEN trim(substr(hca_pat_resp_all.response, 198, 4)) = '' THEN CAST(NULL as DATE)
          WHEN concat(substr(hca_pat_resp_all.response, 198, 4), substr(hca_pat_resp_all.response, 194, 4)) LIKE '%/%' THEN CAST(NULL as DATE)
          ELSE parse_date('%Y%m%d', concat(substr(hca_pat_resp_all.response, 198, 4), substr(hca_pat_resp_all.response, 194, 4)))
        END AS dateofbirth,
        CASE
          WHEN trim(substr(hca_pat_resp_all.response, 202, 2)) = '00' THEN 'ENGLISH'
          WHEN trim(substr(hca_pat_resp_all.response, 202, 2)) = '01' THEN 'SPANISH'
          WHEN trim(substr(hca_pat_resp_all.response, 202, 2)) = '03' THEN 'RUSSIAN'
          WHEN trim(substr(hca_pat_resp_all.response, 202, 2)) = '10' THEN 'CHINESE-TRADITIONAL'
          WHEN trim(substr(hca_pat_resp_all.response, 202, 2)) = '13' THEN 'VIETNAMESE'
        END AS lang,
        nullif(trim(substr(hca_pat_resp_all.response, 204, 15)), '') AS medicalrecordnumber,
        nullif(trim(substr(hca_pat_resp_all.response, 219, 15)), '') AS uniqueid,
        nullif(trim(substr(hca_pat_resp_all.response, 234, 8)), '') AS locationcode,
        nullif(trim(substr(hca_pat_resp_all.response, 242, 10)), '') AS attendingphysicianid,
        nullif(trim(substr(hca_pat_resp_all.response, 252, 1)), '') AS admissionsource,
        CASE
          WHEN trim(substr(hca_pat_resp_all.response, 257, 4)) = '' THEN CAST(NULL as DATE)
          WHEN concat(substr(hca_pat_resp_all.response, 257, 4), substr(hca_pat_resp_all.response, 253, 4)) LIKE '%/%' THEN CAST(NULL as DATE)
          ELSE parse_date('%Y%m%d', concat(substr(hca_pat_resp_all.response, 257, 4), substr(hca_pat_resp_all.response, 253, 4)))
        END AS admissiondate,
        trim(substr(hca_pat_resp_all.response, 261, 4)) AS admissiontime,
        CASE
          WHEN trim(substr(hca_pat_resp_all.response, 269, 4)) = '' THEN CAST(NULL as DATE)
          ELSE parse_date('%Y%m%d', concat(substr(hca_pat_resp_all.response, 269, 4), substr(hca_pat_resp_all.response, 265, 4)))
        END AS dischargedate,
        trim(substr(hca_pat_resp_all.response, 273, 4)) AS dischargetime,
        substr(concat(trim(substr(hca_pat_resp_all.response, 277, 2)), '  '), 1, 2) AS dischargestatus,
        nullif(trim(substr(hca_pat_resp_all.response, 279, 8)), '') AS unit,
        nullif(trim(substr(hca_pat_resp_all.response, 287, 8)), '') AS service,
        nullif(trim(substr(hca_pat_resp_all.response, 295, 8)), '') AS payor,
        CASE
           trim(substr(hca_pat_resp_all.response, 303, 6))
          WHEN '' THEN 0
          ELSE CAST(trim(substr(hca_pat_resp_all.response, 303, 6)) as INT64)
        END AS lengthofstay,
        nullif(trim(substr(hca_pat_resp_all.response, 309, 8)), '') AS room,
        nullif(trim(substr(hca_pat_resp_all.response, 317, 5)), '') AS bed,
        CASE
          WHEN trim(substr(hca_pat_resp_all.response, 322, 1)) = '1' THEN 'YES'
          WHEN trim(substr(hca_pat_resp_all.response, 322, 1)) = '2' THEN 'NO'
          ELSE CAST(NULL as STRING)
        END AS ishospitalist,
        nullif(trim(substr(hca_pat_resp_all.response, 323, 1)), '') AS fasttrack,
        nullif(trim(substr(hca_pat_resp_all.response, 324, 50)), '') AS emailaddress,
        nullif(trim(substr(hca_pat_resp_all.response, 374, 10)), '') AS hospitalistid1,
        nullif(trim(substr(hca_pat_resp_all.response, 384, 10)), '') AS hospitalistid2,
        CASE
          WHEN trim(substr(hca_pat_resp_all.response, 394, 1)) = '1' THEN 'YES'
          WHEN trim(substr(hca_pat_resp_all.response, 394, 1)) = '2' THEN 'NO'
        END AS iseradmission,
        nullif(trim(substr(hca_pat_resp_all.response, 395, 5)), '') AS facilityid,
        nullif(trim(substr(hca_pat_resp_all.response, 400, 5)), '') AS unit_code_id,
        CASE
          WHEN trim(substr(hca_pat_resp_all.response, 405, 1)) = '3' THEN 'S'
          WHEN trim(substr(hca_pat_resp_all.response, 405, 1)) = '4' THEN 'O'
          ELSE '0'
        END AS patienttype,
        nullif(trim(substr(hca_pat_resp_all.response, 406, 12)), '') AS medicare_provider_num,
        nullif(trim(substr(hca_pat_resp_all.response, 418, 12)), '') AS medicareprovidernpi,
        nullif(trim(substr(hca_pat_resp_all.response, 430, 55)), '') AS facilityname,
        nullif(trim(substr(hca_pat_resp_all.response, 485, 10)), '') AS nursestation,
        nullif(trim(substr(hca_pat_resp_all.response, 495, 2)), '') AS drg_type,
        CASE
          WHEN trim(substr(hca_pat_resp_all.response, 497, 2)) = '01' THEN 'MATERNITY DRG'
          WHEN trim(substr(hca_pat_resp_all.response, 497, 2)) = '02' THEN 'MEDICAL'
          WHEN trim(substr(hca_pat_resp_all.response, 497, 2)) = '03' THEN 'SURGICAL'
          WHEN trim(substr(hca_pat_resp_all.response, 497, 2)) = '04' THEN 'INELIGIBLE'
          WHEN trim(substr(hca_pat_resp_all.response, 497, 2)) = '00' THEN '00'
        END AS servicecategory,
        nullif(trim(substr(hca_pat_resp_all.response, 499, 2)), '') AS mdc,
        nullif(trim(substr(hca_pat_resp_all.response, 501, 10)), '') AS diagnosiscategory,
        nullif(trim(substr(hca_pat_resp_all.response, 511, 3)), '') AS patientage,
        nullif(trim(substr(hca_pat_resp_all.response, 514, 1)), '') AS eligible,
        nullif(trim(substr(hca_pat_resp_all.response, 515, 2)), '') AS exclusionflag,
        nullif(trim(substr(hca_pat_resp_all.response, 517, 10)), '') AS financialplan,
        nullif(trim(substr(hca_pat_resp_all.response, 527, 20)), '') AS admissionphysiciancode,
        nullif(trim(substr(hca_pat_resp_all.response, 547, 10)), '') AS primary_icd9_code,
        nullif(trim(substr(hca_pat_resp_all.response, 557, 2)), '') AS icd_codeversion,
        nullif(trim(substr(hca_pat_resp_all.response, 559, 8)), '') AS cpt_code,
        nullif(trim(substr(hca_pat_resp_all.response, 567, 1)), '') AS iped_flag,
        nullif(trim(substr(hca_pat_resp_all.response, 568, 1)), '') AS admissiontype,
        nullif(trim(substr(hca_pat_resp_all.response, 569, 1)), '') AS companycode,
        nullif(trim(substr(hca_pat_resp_all.response, 570, 7)), '') AS diag1,
        nullif(trim(substr(hca_pat_resp_all.response, 577, 7)), '') AS diag2,
        nullif(trim(substr(hca_pat_resp_all.response, 584, 7)), '') AS diag3,
        nullif(trim(substr(hca_pat_resp_all.response, 591, 7)), '') AS diag4,
        nullif(trim(substr(hca_pat_resp_all.response, 598, 7)), '') AS diag5,
        nullif(trim(substr(hca_pat_resp_all.response, 605, 7)), '') AS diag6,
        nullif(trim(substr(hca_pat_resp_all.response, 612, 7)), '') AS diag7,
        nullif(trim(substr(hca_pat_resp_all.response, 619, 7)), '') AS diag8,
        nullif(trim(substr(hca_pat_resp_all.response, 626, 7)), '') AS diag9,
        nullif(trim(substr(hca_pat_resp_all.response, 633, 7)), '') AS diag10,
        nullif(trim(substr(hca_pat_resp_all.response, 640, 7)), '') AS diag11,
        nullif(trim(substr(hca_pat_resp_all.response, 647, 7)), '') AS diag12,
        nullif(trim(substr(hca_pat_resp_all.response, 654, 7)), '') AS diag13,
        nullif(trim(substr(hca_pat_resp_all.response, 661, 7)), '') AS diag14,
        nullif(trim(substr(hca_pat_resp_all.response, 668, 7)), '') AS diag15,
        CASE
          WHEN substr(hca_pat_resp_all.response, 675, 8) = '01010001' THEN CAST(NULL as DATE)
          WHEN substr(hca_pat_resp_all.response, 675, 8) = '00000000' THEN CAST(NULL as DATE)
          WHEN trim(substr(hca_pat_resp_all.response, 679, 4)) = '' THEN CAST(NULL as DATE)
          WHEN concat(substr(hca_pat_resp_all.response, 679, 4), substr(hca_pat_resp_all.response, 675, 4)) LIKE '%/%' THEN CAST(NULL as DATE)
          ELSE parse_date('%Y%m%d', concat(substr(hca_pat_resp_all.response, 679, 4), substr(hca_pat_resp_all.response, 675, 4)))
        END AS billgenerateddate,
        nullif(trim(substr(hca_pat_resp_all.response, 683, 4)), '') AS surgproccode1,
        nullif(trim(substr(hca_pat_resp_all.response, 687, 4)), '') AS surgproccode2,
        nullif(trim(substr(hca_pat_resp_all.response, 691, 4)), '') AS surgproccode3,
        nullif(trim(substr(hca_pat_resp_all.response, 695, 24)), '') AS surgeonname1,
        nullif(trim(substr(hca_pat_resp_all.response, 719, 24)), '') AS surgeonname2,
        nullif(trim(substr(hca_pat_resp_all.response, 743, 24)), '') AS surgeonname3,
        nullif(trim(substr(hca_pat_resp_all.response, 767, 24)), '') AS attendingphysicianname,
        nullif(trim(substr(hca_pat_resp_all.response, 791, 24)), '') AS hospitalistflag,
        nullif(trim(substr(hca_pat_resp_all.response, 815, 4)), '') AS consultingphysicianid,
        nullif(trim(substr(hca_pat_resp_all.response, 819, 24)), '') AS consultingphysicianname,
        -- , TRIM(SUBSTR(RESPONSE,843,5))   AS COID
        nullif(hca_pat_resp_all.coid, '') AS coid,
        nullif(trim(substr(hca_pat_resp_all.response, 848, 1)), '') AS iped,
        nullif(trim(substr(hca_pat_resp_all.response, 849, 1)), '') AS cpt_flag,
        nullif(trim(substr(hca_pat_resp_all.response, 850, 5)), '') AS division_coid,
        nullif(trim(substr(hca_pat_resp_all.response, 855, 5)), '') AS market_coid,
        nullif(trim(substr(hca_pat_resp_all.response, 860, 7)), '') AS surg_proc_code_1_icd,
        nullif(trim(substr(hca_pat_resp_all.response, 867, 7)), '') AS surg_proc_code_2_icd,
        nullif(trim(substr(hca_pat_resp_all.response, 874, 7)), '') AS surg_proc_code_3_icd,
        nullif(trim(substr(hca_pat_resp_all.response, 881, 1)), '') AS ethnicity,
        nullif(trim(substr(hca_pat_resp_all.response, 882, 5)), '') AS hipps_code,
        nullif(trim(substr(hca_pat_resp_all.response, 887, 12)), '') AS mode_of_arrivl,
        nullif(trim(substr(hca_pat_resp_all.response, 899, 9)), '') AS day_of_week,
        nullif(trim(substr(hca_pat_resp_all.response, 908, 8)), '') AS cpt_code_2,
        nullif(trim(substr(hca_pat_resp_all.response, 916, 8)), '') AS cpt_code_3,
        nullif(trim(substr(hca_pat_resp_all.response, 924, 8)), '') AS cpt_code_4,
        nullif(trim(substr(hca_pat_resp_all.response, 932, 8)), '') AS cpt_code_5,
        nullif(trim(substr(hca_pat_resp_all.response, 940, 8)), '') AS cpt_code_6,
        nullif(trim(substr(hca_pat_resp_all.response, 948, 1)), '') AS deceased_flag,
        nullif(trim(substr(hca_pat_resp_all.response, 949, 1)), '') AS no_publicity,
        CASE
          WHEN substr(hca_pat_resp_all.response, 950, 8) = '' THEN CAST(NULL as DATE)
          WHEN substr(hca_pat_resp_all.response, 950, 8) = '00000000' THEN CAST(NULL as DATE)
          WHEN trim(substr(hca_pat_resp_all.response, 954, 4)) = '' THEN CAST(NULL as DATE)
          ELSE parse_date('%Y%m%d', concat(substr(hca_pat_resp_all.response, 954, 4), substr(hca_pat_resp_all.response, 950, 4)))
        END AS prim_cpt_svc_date,
        nullif(trim(hca_pat_resp_all.ptype), '') AS ptype,
        nullif(trim(hca_pat_resp_all.qtype), '') AS qtype,
        nullif(trim(hca_pat_resp_all.category), '') AS category,
        current_ts AS dw_last_update_time
      FROM
        {{ params.param_hr_stage_dataset_name }}.hca_pat_resp_all
      WHERE hca_pat_resp_all.coid IS NOT NULL
  ;

  INSERT INTO {{ params.param_hr_stage_dataset_name }}.hca_pat_resp_all_rej (hca_unique, surv_id, adj_samp, survey_type, pg_unit, disdate, recdate, mde, question_id, coid, response, ptype, qtype, category, cms_rpt, dw_last_update_date_time)
    SELECT
        hca_pat_resp_all.hca_unique,
        hca_pat_resp_all.surv_id,
        hca_pat_resp_all.adj_samp,
        hca_pat_resp_all.survey_type,
        hca_pat_resp_all.pg_unit,
        hca_pat_resp_all.disdate,
        hca_pat_resp_all.recdate,
        hca_pat_resp_all.mde,
        hca_pat_resp_all.question_id,
        hca_pat_resp_all.coid,
        hca_pat_resp_all.response,
        hca_pat_resp_all.ptype,
        hca_pat_resp_all.qtype,
        hca_pat_resp_all.category,
        hca_pat_resp_all.cms_rpt,
        hca_pat_resp_all.dw_last_update_date_time
      FROM
        {{ params.param_hr_stage_dataset_name }}.hca_pat_resp_all
      WHERE upper(hca_pat_resp_all.question_id) = '0'
       AND (concat(substr(hca_pat_resp_all.response, 257, 4), substr(hca_pat_resp_all.response, 253, 4)) LIKE '%/%'
       OR concat(substr(hca_pat_resp_all.response, 198, 4), substr(hca_pat_resp_all.response, 194, 4)) LIKE '%/%')
       AND hca_pat_resp_all.coid IS NOT NULL
  ;
END ;
