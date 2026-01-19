--  create view for PI report being ingested into Alteryx
--  view created by Thomas Jones 4/20/2021
--  11/1/2021 Thomas modified to wrap the msr_value in a sum() to allow it to work after an overflow error in Teradata
CREATE OR REPLACE VIEW {{ params.param_hr_stnd_views_dataset_name }}.pi_rn_movement AS (
  SELECT DISTINCT
      facf.group_name AS group_name,
      facf.division_name AS division_name,
      facf.unit_num AS unit_num,
      facf.coid_name AS coid_name,
      -- - User requested addition
      'Out-bound' AS mov_dir,
      CASE
        WHEN upper(tm.from_process_level_code) = '03216' THEN '27300'
        WHEN upper(tm.from_process_level_code) = '03191' THEN '27400'
        WHEN upper(tm.from_process_level_code) = '03192' THEN '27450'
        WHEN upper(tm.from_process_level_code) = '03175' THEN '27401'
        WHEN upper(tm.from_process_level_code) = '03167' THEN '27100'
        WHEN upper(tm.from_process_level_code) = '03166' THEN '27200'
        WHEN upper(tm.from_process_level_code) = '03170' THEN '27490'
        WHEN upper(tm.from_process_level_code) = '08936' THEN '27150'
        ELSE tm.from_process_level_code
      END AS process_level_code,
      -- - User Requested add to allow them to line up this data with other data they are using where these process level should line up with the changed number in their dataset.
      -- -
      -- -
      -- -
      -- -
      -- -
      -- -
      -- -
      -- -
      -- -
      -- - End change
      hrdrf.cost_center AS cost_center,
      tm.employee_num,
      concat(facf.coid, hrdrf.cost_center) AS coid_cost_center,
      -- - Added per Product team request
      staf.status_code AS aux_status,
      jobf.position_code AS pos_code,
      jclf.job_class_code,
      dt_hr.position_hire_date,
      emp.termination_date,
      CASE
        WHEN (tm.analytics_msr_sid) = 80200
         AND upper(deptf.dept_code) = '63110'
         AND upper(deptf.process_level_code) = '26624' THEN 'External Hire'
        WHEN upper(tm.action_reason_text) LIKE '%ACQ%' THEN 'Acquisition Hire'
        WHEN tm.from_position_sid IS NULL
         AND upper(tm.action_code) NOT LIKE '%1XFER%' THEN 'External Hire'
        WHEN (tm.analytics_msr_sid) = 80200
         AND upper(tm.action_code) LIKE '%1XFER%' THEN 'Internal Hire'
        WHEN (tm.analytics_msr_sid) = 80300
         AND upper(deptf.dept_code) = '63110'
         AND upper(deptf.process_level_code) = '26624' THEN 'External Term'
        WHEN (tm.analytics_msr_sid) = 80300
         AND upper(tm.action_code) = '1TERMPEND'
         AND tm.action_reason_text NOT IN(
          'TV-FCLTRNS', 'TV-RELOHCA', 'TV-PLCHG', 'TV-SPCLXFR', 'TI-OUTSRCE', 'TV-RESIDEN', 'TV-ENDASSG', 'TI-DIVEST', 'TV-NOSHOW', 'TV-NOSTART', 'TV-CHAP'
        ) THEN 'External Term'
        WHEN (tm.analytics_msr_sid) = 80300
         AND upper(tm.action_code) LIKE '%1XFER%' THEN 'Internal Term'
        WHEN (tm.analytics_msr_sid) = 80300
         AND upper(tm.action_code) = '1TERMPEND'
         AND tm.action_reason_text IN(
          'TV-FCLTRNS', 'TV-RELOHCA'
        ) THEN 'Internal Term'
        WHEN (tm.analytics_msr_sid) = 80300 THEN 'Other Term'
        ELSE 'XFER'
      END AS ext_int_type,
      CASE
        WHEN (tm.analytics_msr_sid) = 80200
         AND upper(CASE
          WHEN (tm.analytics_msr_sid) = 80200
           AND upper(deptf.dept_code) = '63110'
           AND upper(deptf.process_level_code) = '26624' THEN 'External Hire'
          WHEN upper(tm.action_reason_text) LIKE '%ACQ%' THEN 'Acquisition Hire'
          WHEN tm.from_position_sid IS NULL
           AND upper(tm.action_code) NOT LIKE '%1XFER%' THEN 'External Hire'
          WHEN (tm.analytics_msr_sid) = 80200
           AND upper(tm.action_code) LIKE '%1XFER%' THEN 'Internal Hire'
          WHEN (tm.analytics_msr_sid) = 80300
           AND upper(deptf.dept_code) = '63110'
           AND upper(deptf.process_level_code) = '26624' THEN 'External Term'
          WHEN (tm.analytics_msr_sid) = 80300
           AND upper(tm.action_code) = '1TERMPEND'
           AND tm.action_reason_text NOT IN(
            'TV-FCLTRNS', 'TV-RELOHCA', 'TV-PLCHG', 'TV-SPCLXFR', 'TI-OUTSRCE', 'TV-RESIDEN', 'TV-ENDASSG', 'TI-DIVEST', 'TV-NOSHOW', 'TV-NOSTART', 'TV-CHAP'
          ) THEN 'External Term'
          WHEN (tm.analytics_msr_sid) = 80300
           AND upper(tm.action_code) LIKE '%1XFER%' THEN 'Internal Term'
          WHEN (tm.analytics_msr_sid) = 80300
           AND upper(tm.action_code) = '1TERMPEND'
           AND tm.action_reason_text IN(
            'TV-FCLTRNS', 'TV-RELOHCA'
          ) THEN 'Internal Term'
          WHEN (tm.analytics_msr_sid) = 80300 THEN 'Other Term'
          ELSE 'XFER'
        END) = 'EXTERNAL HIRE' THEN 'Hire'
        WHEN (tm.analytics_msr_sid) = 80300
         AND upper(CASE
          WHEN (tm.analytics_msr_sid) = 80200
           AND upper(deptf.dept_code) = '63110'
           AND upper(deptf.process_level_code) = '26624' THEN 'External Hire'
          WHEN upper(tm.action_reason_text) LIKE '%ACQ%' THEN 'Acquisition Hire'
          WHEN tm.from_position_sid IS NULL
           AND upper(tm.action_code) NOT LIKE '%1XFER%' THEN 'External Hire'
          WHEN (tm.analytics_msr_sid) = 80200
           AND upper(tm.action_code) LIKE '%1XFER%' THEN 'Internal Hire'
          WHEN (tm.analytics_msr_sid) = 80300
           AND upper(deptf.dept_code) = '63110'
           AND upper(deptf.process_level_code) = '26624' THEN 'External Term'
          WHEN (tm.analytics_msr_sid) = 80300
           AND upper(tm.action_code) = '1TERMPEND'
           AND tm.action_reason_text NOT IN(
            'TV-FCLTRNS', 'TV-RELOHCA', 'TV-PLCHG', 'TV-SPCLXFR', 'TI-OUTSRCE', 'TV-RESIDEN', 'TV-ENDASSG', 'TI-DIVEST', 'TV-NOSHOW', 'TV-NOSTART', 'TV-CHAP'
          ) THEN 'External Term'
          WHEN (tm.analytics_msr_sid) = 80300
           AND upper(tm.action_code) LIKE '%1XFER%' THEN 'Internal Term'
          WHEN (tm.analytics_msr_sid) = 80300
           AND upper(tm.action_code) = '1TERMPEND'
           AND tm.action_reason_text IN(
            'TV-FCLTRNS', 'TV-RELOHCA'
          ) THEN 'Internal Term'
          WHEN (tm.analytics_msr_sid) = 80300 THEN 'Other Term'
          ELSE 'XFER'
        END) = 'EXTERNAL TERM' THEN 'Term'
        WHEN upper(jclf.job_class_code) = '103'
         AND upper(jclt.job_class_code) = '103'
         AND staf.status_code <> stato.status_code THEN 'Aux Status Change'
        WHEN upper(jclf.job_class_code) = '105'
         AND upper(jclt.job_class_code) = '103' THEN 'To RN from Tech'
        WHEN upper(jclf.job_class_code) = '101'
         AND upper(jclt.job_class_code) = '103' THEN 'To RN from Manager'
        WHEN upper(jclf.job_class_code) = '102'
         AND upper(jclt.job_class_code) = '103' THEN 'To RN from Clinical Spec/Prof'
        WHEN upper(jclf.job_class_code) = '104'
         AND upper(jclt.job_class_code) = '103' THEN 'To RN from LPN'
        WHEN upper(jclf.job_class_code) = '106'
         AND upper(jclt.job_class_code) = '103' THEN 'To RN from Clerical'
        WHEN upper(jclf.job_class_code) = '107'
         AND upper(jclt.job_class_code) = '103' THEN 'To RN from Environmental'
        WHEN upper(jclf.job_class_code) = '108'
         AND upper(jclt.job_class_code) = '103' THEN 'To RN from Physician'
        WHEN upper(jclf.job_class_code) = '109'
         AND upper(jclt.job_class_code) = '103' THEN 'To RN from Non-Phys Med Prac'
        WHEN upper(jclf.job_class_code) = '110'
         AND upper(jclt.job_class_code) = '103' THEN 'To RN from Non Clinical Spec/Prof'
        WHEN upper(jclf.job_class_code) = '111'
         AND upper(jclt.job_class_code) = '103' THEN 'To RN from Clinical Techs'
        WHEN upper(jclf.job_class_code) = '103'
         AND upper(jclt.job_class_code) = '101' THEN 'From RN to Manager'
        WHEN upper(jclf.job_class_code) = '103'
         AND upper(jclt.job_class_code) = '102' THEN 'From RN to Clinical Spec/Prof'
        WHEN upper(jclf.job_class_code) = '103'
         AND upper(jclt.job_class_code) = '104' THEN 'From RN to LPN'
        WHEN upper(jclf.job_class_code) = '103'
         AND upper(jclt.job_class_code) = '105' THEN 'From RN to Tech'
        WHEN upper(jclf.job_class_code) = '103'
         AND upper(jclt.job_class_code) = '106' THEN 'From RN to Clerical'
        WHEN upper(jclf.job_class_code) = '103'
         AND upper(jclt.job_class_code) = '107' THEN 'From RN to Environmental'
        WHEN upper(jclf.job_class_code) = '103'
         AND upper(jclt.job_class_code) = '108' THEN 'From RN to Physician'
        WHEN upper(jclf.job_class_code) = '103'
         AND upper(jclt.job_class_code) = '109' THEN 'From RN to Non-Phys Med Prac'
        WHEN upper(jclf.job_class_code) = '103'
         AND upper(jclt.job_class_code) = '110' THEN 'From RN to Non-Clinical Spec/Prof'
        WHEN upper(jclf.job_class_code) = '103'
         AND upper(jclt.job_class_code) = '111' THEN 'From RN to Clinical Techs'
        WHEN (tm.analytics_msr_sid) = 80200 THEN 'Other Hire'
        WHEN (tm.analytics_msr_sid) = 80300 THEN 'Other Term'
        WHEN jclf.job_class_code = jclt.job_class_code
         AND staf.status_code = stato.status_code THEN 'XFER No Chng'
      END AS change_type,
      -- ELSE 'XFER_No_Chng'
      CASE
        WHEN upper(CASE
          WHEN (tm.analytics_msr_sid) = 80200
           AND upper(CASE
            WHEN (tm.analytics_msr_sid) = 80200
             AND upper(deptf.dept_code) = '63110'
             AND upper(deptf.process_level_code) = '26624' THEN 'External Hire'
            WHEN upper(tm.action_reason_text) LIKE '%ACQ%' THEN 'Acquisition Hire'
            WHEN tm.from_position_sid IS NULL
             AND upper(tm.action_code) NOT LIKE '%1XFER%' THEN 'External Hire'
            WHEN (tm.analytics_msr_sid) = 80200
             AND upper(tm.action_code) LIKE '%1XFER%' THEN 'Internal Hire'
            WHEN (tm.analytics_msr_sid) = 80300
             AND upper(deptf.dept_code) = '63110'
             AND upper(deptf.process_level_code) = '26624' THEN 'External Term'
            WHEN (tm.analytics_msr_sid) = 80300
             AND upper(tm.action_code) = '1TERMPEND'
             AND tm.action_reason_text NOT IN(
              'TV-FCLTRNS', 'TV-RELOHCA', 'TV-PLCHG', 'TV-SPCLXFR', 'TI-OUTSRCE', 'TV-RESIDEN', 'TV-ENDASSG', 'TI-DIVEST', 'TV-NOSHOW', 'TV-NOSTART', 'TV-CHAP'
            ) THEN 'External Term'
            WHEN (tm.analytics_msr_sid) = 80300
             AND upper(tm.action_code) LIKE '%1XFER%' THEN 'Internal Term'
            WHEN (tm.analytics_msr_sid) = 80300
             AND upper(tm.action_code) = '1TERMPEND'
             AND tm.action_reason_text IN(
              'TV-FCLTRNS', 'TV-RELOHCA'
            ) THEN 'Internal Term'
            WHEN (tm.analytics_msr_sid) = 80300 THEN 'Other Term'
            ELSE 'XFER'
          END) = 'EXTERNAL HIRE' THEN 'Hire'
          WHEN (tm.analytics_msr_sid) = 80300
           AND upper(CASE
            WHEN (tm.analytics_msr_sid) = 80200
             AND upper(deptf.dept_code) = '63110'
             AND upper(deptf.process_level_code) = '26624' THEN 'External Hire'
            WHEN upper(tm.action_reason_text) LIKE '%ACQ%' THEN 'Acquisition Hire'
            WHEN tm.from_position_sid IS NULL
             AND upper(tm.action_code) NOT LIKE '%1XFER%' THEN 'External Hire'
            WHEN (tm.analytics_msr_sid) = 80200
             AND upper(tm.action_code) LIKE '%1XFER%' THEN 'Internal Hire'
            WHEN (tm.analytics_msr_sid) = 80300
             AND upper(deptf.dept_code) = '63110'
             AND upper(deptf.process_level_code) = '26624' THEN 'External Term'
            WHEN (tm.analytics_msr_sid) = 80300
             AND upper(tm.action_code) = '1TERMPEND'
             AND tm.action_reason_text NOT IN(
              'TV-FCLTRNS', 'TV-RELOHCA', 'TV-PLCHG', 'TV-SPCLXFR', 'TI-OUTSRCE', 'TV-RESIDEN', 'TV-ENDASSG', 'TI-DIVEST', 'TV-NOSHOW', 'TV-NOSTART', 'TV-CHAP'
            ) THEN 'External Term'
            WHEN (tm.analytics_msr_sid) = 80300
             AND upper(tm.action_code) LIKE '%1XFER%' THEN 'Internal Term'
            WHEN (tm.analytics_msr_sid) = 80300
             AND upper(tm.action_code) = '1TERMPEND'
             AND tm.action_reason_text IN(
              'TV-FCLTRNS', 'TV-RELOHCA'
            ) THEN 'Internal Term'
            WHEN (tm.analytics_msr_sid) = 80300 THEN 'Other Term'
            ELSE 'XFER'
          END) = 'EXTERNAL TERM' THEN 'Term'
          WHEN upper(jclf.job_class_code) = '103'
           AND upper(jclt.job_class_code) = '103'
           AND staf.status_code <> stato.status_code THEN 'Aux Status Change'
          WHEN upper(jclf.job_class_code) = '105'
           AND upper(jclt.job_class_code) = '103' THEN 'To RN from Tech'
          WHEN upper(jclf.job_class_code) = '101'
           AND upper(jclt.job_class_code) = '103' THEN 'To RN from Manager'
          WHEN upper(jclf.job_class_code) = '102'
           AND upper(jclt.job_class_code) = '103' THEN 'To RN from Clinical Spec/Prof'
          WHEN upper(jclf.job_class_code) = '104'
           AND upper(jclt.job_class_code) = '103' THEN 'To RN from LPN'
          WHEN upper(jclf.job_class_code) = '106'
           AND upper(jclt.job_class_code) = '103' THEN 'To RN from Clerical'
          WHEN upper(jclf.job_class_code) = '107'
           AND upper(jclt.job_class_code) = '103' THEN 'To RN from Environmental'
          WHEN upper(jclf.job_class_code) = '108'
           AND upper(jclt.job_class_code) = '103' THEN 'To RN from Physician'
          WHEN upper(jclf.job_class_code) = '109'
           AND upper(jclt.job_class_code) = '103' THEN 'To RN from Non-Phys Med Prac'
          WHEN upper(jclf.job_class_code) = '110'
           AND upper(jclt.job_class_code) = '103' THEN 'To RN from Non Clinical Spec/Prof'
          WHEN upper(jclf.job_class_code) = '111'
           AND upper(jclt.job_class_code) = '103' THEN 'To RN from Clinical Techs'
          WHEN upper(jclf.job_class_code) = '103'
           AND upper(jclt.job_class_code) = '101' THEN 'From RN to Manager'
          WHEN upper(jclf.job_class_code) = '103'
           AND upper(jclt.job_class_code) = '102' THEN 'From RN to Clinical Spec/Prof'
          WHEN upper(jclf.job_class_code) = '103'
           AND upper(jclt.job_class_code) = '104' THEN 'From RN to LPN'
          WHEN upper(jclf.job_class_code) = '103'
           AND upper(jclt.job_class_code) = '105' THEN 'From RN to Tech'
          WHEN upper(jclf.job_class_code) = '103'
           AND upper(jclt.job_class_code) = '106' THEN 'From RN to Clerical'
          WHEN upper(jclf.job_class_code) = '103'
           AND upper(jclt.job_class_code) = '107' THEN 'From RN to Environmental'
          WHEN upper(jclf.job_class_code) = '103'
           AND upper(jclt.job_class_code) = '108' THEN 'From RN to Physician'
          WHEN upper(jclf.job_class_code) = '103'
           AND upper(jclt.job_class_code) = '109' THEN 'From RN to Non-Phys Med Prac'
          WHEN upper(jclf.job_class_code) = '103'
           AND upper(jclt.job_class_code) = '110' THEN 'From RN to Non-Clinical Spec/Prof'
          WHEN upper(jclf.job_class_code) = '103'
           AND upper(jclt.job_class_code) = '111' THEN 'From RN to Clinical Techs'
          WHEN (tm.analytics_msr_sid) = 80200 THEN 'Other Hire'
          WHEN (tm.analytics_msr_sid) = 80300 THEN 'Other Term'
          WHEN jclf.job_class_code = jclt.job_class_code
           AND staf.status_code = stato.status_code THEN 'XFER No Chng'
        END) = 'TERM' THEN 'TERM'
        WHEN upper(CASE
          WHEN (tm.analytics_msr_sid) = 80200
           AND upper(CASE
            WHEN (tm.analytics_msr_sid) = 80200
             AND upper(deptf.dept_code) = '63110'
             AND upper(deptf.process_level_code) = '26624' THEN 'External Hire'
            WHEN upper(tm.action_reason_text) LIKE '%ACQ%' THEN 'Acquisition Hire'
            WHEN tm.from_position_sid IS NULL
             AND upper(tm.action_code) NOT LIKE '%1XFER%' THEN 'External Hire'
            WHEN (tm.analytics_msr_sid) = 80200
             AND upper(tm.action_code) LIKE '%1XFER%' THEN 'Internal Hire'
            WHEN (tm.analytics_msr_sid) = 80300
             AND upper(deptf.dept_code) = '63110'
             AND upper(deptf.process_level_code) = '26624' THEN 'External Term'
            WHEN (tm.analytics_msr_sid) = 80300
             AND upper(tm.action_code) = '1TERMPEND'
             AND tm.action_reason_text NOT IN(
              'TV-FCLTRNS', 'TV-RELOHCA', 'TV-PLCHG', 'TV-SPCLXFR', 'TI-OUTSRCE', 'TV-RESIDEN', 'TV-ENDASSG', 'TI-DIVEST', 'TV-NOSHOW', 'TV-NOSTART', 'TV-CHAP'
            ) THEN 'External Term'
            WHEN (tm.analytics_msr_sid) = 80300
             AND upper(tm.action_code) LIKE '%1XFER%' THEN 'Internal Term'
            WHEN (tm.analytics_msr_sid) = 80300
             AND upper(tm.action_code) = '1TERMPEND'
             AND tm.action_reason_text IN(
              'TV-FCLTRNS', 'TV-RELOHCA'
            ) THEN 'Internal Term'
            WHEN (tm.analytics_msr_sid) = 80300 THEN 'Other Term'
            ELSE 'XFER'
          END) = 'EXTERNAL HIRE' THEN 'Hire'
          WHEN (tm.analytics_msr_sid) = 80300
           AND upper(CASE
            WHEN (tm.analytics_msr_sid) = 80200
             AND upper(deptf.dept_code) = '63110'
             AND upper(deptf.process_level_code) = '26624' THEN 'External Hire'
            WHEN upper(tm.action_reason_text) LIKE '%ACQ%' THEN 'Acquisition Hire'
            WHEN tm.from_position_sid IS NULL
             AND upper(tm.action_code) NOT LIKE '%1XFER%' THEN 'External Hire'
            WHEN (tm.analytics_msr_sid) = 80200
             AND upper(tm.action_code) LIKE '%1XFER%' THEN 'Internal Hire'
            WHEN (tm.analytics_msr_sid) = 80300
             AND upper(deptf.dept_code) = '63110'
             AND upper(deptf.process_level_code) = '26624' THEN 'External Term'
            WHEN (tm.analytics_msr_sid) = 80300
             AND upper(tm.action_code) = '1TERMPEND'
             AND tm.action_reason_text NOT IN(
              'TV-FCLTRNS', 'TV-RELOHCA', 'TV-PLCHG', 'TV-SPCLXFR', 'TI-OUTSRCE', 'TV-RESIDEN', 'TV-ENDASSG', 'TI-DIVEST', 'TV-NOSHOW', 'TV-NOSTART', 'TV-CHAP'
            ) THEN 'External Term'
            WHEN (tm.analytics_msr_sid) = 80300
             AND upper(tm.action_code) LIKE '%1XFER%' THEN 'Internal Term'
            WHEN (tm.analytics_msr_sid) = 80300
             AND upper(tm.action_code) = '1TERMPEND'
             AND tm.action_reason_text IN(
              'TV-FCLTRNS', 'TV-RELOHCA'
            ) THEN 'Internal Term'
            WHEN (tm.analytics_msr_sid) = 80300 THEN 'Other Term'
            ELSE 'XFER'
          END) = 'EXTERNAL TERM' THEN 'Term'
          WHEN upper(jclf.job_class_code) = '103'
           AND upper(jclt.job_class_code) = '103'
           AND staf.status_code <> stato.status_code THEN 'Aux Status Change'
          WHEN upper(jclf.job_class_code) = '105'
           AND upper(jclt.job_class_code) = '103' THEN 'To RN from Tech'
          WHEN upper(jclf.job_class_code) = '101'
           AND upper(jclt.job_class_code) = '103' THEN 'To RN from Manager'
          WHEN upper(jclf.job_class_code) = '102'
           AND upper(jclt.job_class_code) = '103' THEN 'To RN from Clinical Spec/Prof'
          WHEN upper(jclf.job_class_code) = '104'
           AND upper(jclt.job_class_code) = '103' THEN 'To RN from LPN'
          WHEN upper(jclf.job_class_code) = '106'
           AND upper(jclt.job_class_code) = '103' THEN 'To RN from Clerical'
          WHEN upper(jclf.job_class_code) = '107'
           AND upper(jclt.job_class_code) = '103' THEN 'To RN from Environmental'
          WHEN upper(jclf.job_class_code) = '108'
           AND upper(jclt.job_class_code) = '103' THEN 'To RN from Physician'
          WHEN upper(jclf.job_class_code) = '109'
           AND upper(jclt.job_class_code) = '103' THEN 'To RN from Non-Phys Med Prac'
          WHEN upper(jclf.job_class_code) = '110'
           AND upper(jclt.job_class_code) = '103' THEN 'To RN from Non Clinical Spec/Prof'
          WHEN upper(jclf.job_class_code) = '111'
           AND upper(jclt.job_class_code) = '103' THEN 'To RN from Clinical Techs'
          WHEN upper(jclf.job_class_code) = '103'
           AND upper(jclt.job_class_code) = '101' THEN 'From RN to Manager'
          WHEN upper(jclf.job_class_code) = '103'
           AND upper(jclt.job_class_code) = '102' THEN 'From RN to Clinical Spec/Prof'
          WHEN upper(jclf.job_class_code) = '103'
           AND upper(jclt.job_class_code) = '104' THEN 'From RN to LPN'
          WHEN upper(jclf.job_class_code) = '103'
           AND upper(jclt.job_class_code) = '105' THEN 'From RN to Tech'
          WHEN upper(jclf.job_class_code) = '103'
           AND upper(jclt.job_class_code) = '106' THEN 'From RN to Clerical'
          WHEN upper(jclf.job_class_code) = '103'
           AND upper(jclt.job_class_code) = '107' THEN 'From RN to Environmental'
          WHEN upper(jclf.job_class_code) = '103'
           AND upper(jclt.job_class_code) = '108' THEN 'From RN to Physician'
          WHEN upper(jclf.job_class_code) = '103'
           AND upper(jclt.job_class_code) = '109' THEN 'From RN to Non-Phys Med Prac'
          WHEN upper(jclf.job_class_code) = '103'
           AND upper(jclt.job_class_code) = '110' THEN 'From RN to Non-Clinical Spec/Prof'
          WHEN upper(jclf.job_class_code) = '103'
           AND upper(jclt.job_class_code) = '111' THEN 'From RN to Clinical Techs'
          WHEN (tm.analytics_msr_sid) = 80200 THEN 'Other Hire'
          WHEN (tm.analytics_msr_sid) = 80300 THEN 'Other Term'
          WHEN jclf.job_class_code = jclt.job_class_code
           AND staf.status_code = stato.status_code THEN 'XFER No Chng'
        END) = 'HIRE' THEN 'Hire'
        WHEN upper(CASE
          WHEN (tm.analytics_msr_sid) = 80200
           AND upper(CASE
            WHEN (tm.analytics_msr_sid) = 80200
             AND upper(deptf.dept_code) = '63110'
             AND upper(deptf.process_level_code) = '26624' THEN 'External Hire'
            WHEN upper(tm.action_reason_text) LIKE '%ACQ%' THEN 'Acquisition Hire'
            WHEN tm.from_position_sid IS NULL
             AND upper(tm.action_code) NOT LIKE '%1XFER%' THEN 'External Hire'
            WHEN (tm.analytics_msr_sid) = 80200
             AND upper(tm.action_code) LIKE '%1XFER%' THEN 'Internal Hire'
            WHEN (tm.analytics_msr_sid) = 80300
             AND upper(deptf.dept_code) = '63110'
             AND upper(deptf.process_level_code) = '26624' THEN 'External Term'
            WHEN (tm.analytics_msr_sid) = 80300
             AND upper(tm.action_code) = '1TERMPEND'
             AND tm.action_reason_text NOT IN(
              'TV-FCLTRNS', 'TV-RELOHCA', 'TV-PLCHG', 'TV-SPCLXFR', 'TI-OUTSRCE', 'TV-RESIDEN', 'TV-ENDASSG', 'TI-DIVEST', 'TV-NOSHOW', 'TV-NOSTART', 'TV-CHAP'
            ) THEN 'External Term'
            WHEN (tm.analytics_msr_sid) = 80300
             AND upper(tm.action_code) LIKE '%1XFER%' THEN 'Internal Term'
            WHEN (tm.analytics_msr_sid) = 80300
             AND upper(tm.action_code) = '1TERMPEND'
             AND tm.action_reason_text IN(
              'TV-FCLTRNS', 'TV-RELOHCA'
            ) THEN 'Internal Term'
            WHEN (tm.analytics_msr_sid) = 80300 THEN 'Other Term'
            ELSE 'XFER'
          END) = 'EXTERNAL HIRE' THEN 'Hire'
          WHEN (tm.analytics_msr_sid) = 80300
           AND upper(CASE
            WHEN (tm.analytics_msr_sid) = 80200
             AND upper(deptf.dept_code) = '63110'
             AND upper(deptf.process_level_code) = '26624' THEN 'External Hire'
            WHEN upper(tm.action_reason_text) LIKE '%ACQ%' THEN 'Acquisition Hire'
            WHEN tm.from_position_sid IS NULL
             AND upper(tm.action_code) NOT LIKE '%1XFER%' THEN 'External Hire'
            WHEN (tm.analytics_msr_sid) = 80200
             AND upper(tm.action_code) LIKE '%1XFER%' THEN 'Internal Hire'
            WHEN (tm.analytics_msr_sid) = 80300
             AND upper(deptf.dept_code) = '63110'
             AND upper(deptf.process_level_code) = '26624' THEN 'External Term'
            WHEN (tm.analytics_msr_sid) = 80300
             AND upper(tm.action_code) = '1TERMPEND'
             AND tm.action_reason_text NOT IN(
              'TV-FCLTRNS', 'TV-RELOHCA', 'TV-PLCHG', 'TV-SPCLXFR', 'TI-OUTSRCE', 'TV-RESIDEN', 'TV-ENDASSG', 'TI-DIVEST', 'TV-NOSHOW', 'TV-NOSTART', 'TV-CHAP'
            ) THEN 'External Term'
            WHEN (tm.analytics_msr_sid) = 80300
             AND upper(tm.action_code) LIKE '%1XFER%' THEN 'Internal Term'
            WHEN (tm.analytics_msr_sid) = 80300
             AND upper(tm.action_code) = '1TERMPEND'
             AND tm.action_reason_text IN(
              'TV-FCLTRNS', 'TV-RELOHCA'
            ) THEN 'Internal Term'
            WHEN (tm.analytics_msr_sid) = 80300 THEN 'Other Term'
            ELSE 'XFER'
          END) = 'EXTERNAL TERM' THEN 'Term'
          WHEN upper(jclf.job_class_code) = '103'
           AND upper(jclt.job_class_code) = '103'
           AND staf.status_code <> stato.status_code THEN 'Aux Status Change'
          WHEN upper(jclf.job_class_code) = '105'
           AND upper(jclt.job_class_code) = '103' THEN 'To RN from Tech'
          WHEN upper(jclf.job_class_code) = '101'
           AND upper(jclt.job_class_code) = '103' THEN 'To RN from Manager'
          WHEN upper(jclf.job_class_code) = '102'
           AND upper(jclt.job_class_code) = '103' THEN 'To RN from Clinical Spec/Prof'
          WHEN upper(jclf.job_class_code) = '104'
           AND upper(jclt.job_class_code) = '103' THEN 'To RN from LPN'
          WHEN upper(jclf.job_class_code) = '106'
           AND upper(jclt.job_class_code) = '103' THEN 'To RN from Clerical'
          WHEN upper(jclf.job_class_code) = '107'
           AND upper(jclt.job_class_code) = '103' THEN 'To RN from Environmental'
          WHEN upper(jclf.job_class_code) = '108'
           AND upper(jclt.job_class_code) = '103' THEN 'To RN from Physician'
          WHEN upper(jclf.job_class_code) = '109'
           AND upper(jclt.job_class_code) = '103' THEN 'To RN from Non-Phys Med Prac'
          WHEN upper(jclf.job_class_code) = '110'
           AND upper(jclt.job_class_code) = '103' THEN 'To RN from Non Clinical Spec/Prof'
          WHEN upper(jclf.job_class_code) = '111'
           AND upper(jclt.job_class_code) = '103' THEN 'To RN from Clinical Techs'
          WHEN upper(jclf.job_class_code) = '103'
           AND upper(jclt.job_class_code) = '101' THEN 'From RN to Manager'
          WHEN upper(jclf.job_class_code) = '103'
           AND upper(jclt.job_class_code) = '102' THEN 'From RN to Clinical Spec/Prof'
          WHEN upper(jclf.job_class_code) = '103'
           AND upper(jclt.job_class_code) = '104' THEN 'From RN to LPN'
          WHEN upper(jclf.job_class_code) = '103'
           AND upper(jclt.job_class_code) = '105' THEN 'From RN to Tech'
          WHEN upper(jclf.job_class_code) = '103'
           AND upper(jclt.job_class_code) = '106' THEN 'From RN to Clerical'
          WHEN upper(jclf.job_class_code) = '103'
           AND upper(jclt.job_class_code) = '107' THEN 'From RN to Environmental'
          WHEN upper(jclf.job_class_code) = '103'
           AND upper(jclt.job_class_code) = '108' THEN 'From RN to Physician'
          WHEN upper(jclf.job_class_code) = '103'
           AND upper(jclt.job_class_code) = '109' THEN 'From RN to Non-Phys Med Prac'
          WHEN upper(jclf.job_class_code) = '103'
           AND upper(jclt.job_class_code) = '110' THEN 'From RN to Non-Clinical Spec/Prof'
          WHEN upper(jclf.job_class_code) = '103'
           AND upper(jclt.job_class_code) = '111' THEN 'From RN to Clinical Techs'
          WHEN (tm.analytics_msr_sid) = 80200 THEN 'Other Hire'
          WHEN (tm.analytics_msr_sid) = 80300 THEN 'Other Term'
          WHEN jclf.job_class_code = jclt.job_class_code
           AND staf.status_code = stato.status_code THEN 'XFER No Chng'
        END) = 'OTHER TERM' THEN 'Other Term'
        WHEN upper(CASE
          WHEN (tm.analytics_msr_sid) = 80200
           AND upper(CASE
            WHEN (tm.analytics_msr_sid) = 80200
             AND upper(deptf.dept_code) = '63110'
             AND upper(deptf.process_level_code) = '26624' THEN 'External Hire'
            WHEN upper(tm.action_reason_text) LIKE '%ACQ%' THEN 'Acquisition Hire'
            WHEN tm.from_position_sid IS NULL
             AND upper(tm.action_code) NOT LIKE '%1XFER%' THEN 'External Hire'
            WHEN (tm.analytics_msr_sid) = 80200
             AND upper(tm.action_code) LIKE '%1XFER%' THEN 'Internal Hire'
            WHEN (tm.analytics_msr_sid) = 80300
             AND upper(deptf.dept_code) = '63110'
             AND upper(deptf.process_level_code) = '26624' THEN 'External Term'
            WHEN (tm.analytics_msr_sid) = 80300
             AND upper(tm.action_code) = '1TERMPEND'
             AND tm.action_reason_text NOT IN(
              'TV-FCLTRNS', 'TV-RELOHCA', 'TV-PLCHG', 'TV-SPCLXFR', 'TI-OUTSRCE', 'TV-RESIDEN', 'TV-ENDASSG', 'TI-DIVEST', 'TV-NOSHOW', 'TV-NOSTART', 'TV-CHAP'
            ) THEN 'External Term'
            WHEN (tm.analytics_msr_sid) = 80300
             AND upper(tm.action_code) LIKE '%1XFER%' THEN 'Internal Term'
            WHEN (tm.analytics_msr_sid) = 80300
             AND upper(tm.action_code) = '1TERMPEND'
             AND tm.action_reason_text IN(
              'TV-FCLTRNS', 'TV-RELOHCA'
            ) THEN 'Internal Term'
            WHEN (tm.analytics_msr_sid) = 80300 THEN 'Other Term'
            ELSE 'XFER'
          END) = 'EXTERNAL HIRE' THEN 'Hire'
          WHEN (tm.analytics_msr_sid) = 80300
           AND upper(CASE
            WHEN (tm.analytics_msr_sid) = 80200
             AND upper(deptf.dept_code) = '63110'
             AND upper(deptf.process_level_code) = '26624' THEN 'External Hire'
            WHEN upper(tm.action_reason_text) LIKE '%ACQ%' THEN 'Acquisition Hire'
            WHEN tm.from_position_sid IS NULL
             AND upper(tm.action_code) NOT LIKE '%1XFER%' THEN 'External Hire'
            WHEN (tm.analytics_msr_sid) = 80200
             AND upper(tm.action_code) LIKE '%1XFER%' THEN 'Internal Hire'
            WHEN (tm.analytics_msr_sid) = 80300
             AND upper(deptf.dept_code) = '63110'
             AND upper(deptf.process_level_code) = '26624' THEN 'External Term'
            WHEN (tm.analytics_msr_sid) = 80300
             AND upper(tm.action_code) = '1TERMPEND'
             AND tm.action_reason_text NOT IN(
              'TV-FCLTRNS', 'TV-RELOHCA', 'TV-PLCHG', 'TV-SPCLXFR', 'TI-OUTSRCE', 'TV-RESIDEN', 'TV-ENDASSG', 'TI-DIVEST', 'TV-NOSHOW', 'TV-NOSTART', 'TV-CHAP'
            ) THEN 'External Term'
            WHEN (tm.analytics_msr_sid) = 80300
             AND upper(tm.action_code) LIKE '%1XFER%' THEN 'Internal Term'
            WHEN (tm.analytics_msr_sid) = 80300
             AND upper(tm.action_code) = '1TERMPEND'
             AND tm.action_reason_text IN(
              'TV-FCLTRNS', 'TV-RELOHCA'
            ) THEN 'Internal Term'
            WHEN (tm.analytics_msr_sid) = 80300 THEN 'Other Term'
            ELSE 'XFER'
          END) = 'EXTERNAL TERM' THEN 'Term'
          WHEN upper(jclf.job_class_code) = '103'
           AND upper(jclt.job_class_code) = '103'
           AND staf.status_code <> stato.status_code THEN 'Aux Status Change'
          WHEN upper(jclf.job_class_code) = '105'
           AND upper(jclt.job_class_code) = '103' THEN 'To RN from Tech'
          WHEN upper(jclf.job_class_code) = '101'
           AND upper(jclt.job_class_code) = '103' THEN 'To RN from Manager'
          WHEN upper(jclf.job_class_code) = '102'
           AND upper(jclt.job_class_code) = '103' THEN 'To RN from Clinical Spec/Prof'
          WHEN upper(jclf.job_class_code) = '104'
           AND upper(jclt.job_class_code) = '103' THEN 'To RN from LPN'
          WHEN upper(jclf.job_class_code) = '106'
           AND upper(jclt.job_class_code) = '103' THEN 'To RN from Clerical'
          WHEN upper(jclf.job_class_code) = '107'
           AND upper(jclt.job_class_code) = '103' THEN 'To RN from Environmental'
          WHEN upper(jclf.job_class_code) = '108'
           AND upper(jclt.job_class_code) = '103' THEN 'To RN from Physician'
          WHEN upper(jclf.job_class_code) = '109'
           AND upper(jclt.job_class_code) = '103' THEN 'To RN from Non-Phys Med Prac'
          WHEN upper(jclf.job_class_code) = '110'
           AND upper(jclt.job_class_code) = '103' THEN 'To RN from Non Clinical Spec/Prof'
          WHEN upper(jclf.job_class_code) = '111'
           AND upper(jclt.job_class_code) = '103' THEN 'To RN from Clinical Techs'
          WHEN upper(jclf.job_class_code) = '103'
           AND upper(jclt.job_class_code) = '101' THEN 'From RN to Manager'
          WHEN upper(jclf.job_class_code) = '103'
           AND upper(jclt.job_class_code) = '102' THEN 'From RN to Clinical Spec/Prof'
          WHEN upper(jclf.job_class_code) = '103'
           AND upper(jclt.job_class_code) = '104' THEN 'From RN to LPN'
          WHEN upper(jclf.job_class_code) = '103'
           AND upper(jclt.job_class_code) = '105' THEN 'From RN to Tech'
          WHEN upper(jclf.job_class_code) = '103'
           AND upper(jclt.job_class_code) = '106' THEN 'From RN to Clerical'
          WHEN upper(jclf.job_class_code) = '103'
           AND upper(jclt.job_class_code) = '107' THEN 'From RN to Environmental'
          WHEN upper(jclf.job_class_code) = '103'
           AND upper(jclt.job_class_code) = '108' THEN 'From RN to Physician'
          WHEN upper(jclf.job_class_code) = '103'
           AND upper(jclt.job_class_code) = '109' THEN 'From RN to Non-Phys Med Prac'
          WHEN upper(jclf.job_class_code) = '103'
           AND upper(jclt.job_class_code) = '110' THEN 'From RN to Non-Clinical Spec/Prof'
          WHEN upper(jclf.job_class_code) = '103'
           AND upper(jclt.job_class_code) = '111' THEN 'From RN to Clinical Techs'
          WHEN (tm.analytics_msr_sid) = 80200 THEN 'Other Hire'
          WHEN (tm.analytics_msr_sid) = 80300 THEN 'Other Term'
          WHEN jclf.job_class_code = jclt.job_class_code
           AND staf.status_code = stato.status_code THEN 'XFER No Chng'
        END) = 'OTHER HIRE' THEN 'Other Hire'
        WHEN upper(CASE
          WHEN (tm.analytics_msr_sid) = 80200
           AND upper(CASE
            WHEN (tm.analytics_msr_sid) = 80200
             AND upper(deptf.dept_code) = '63110'
             AND upper(deptf.process_level_code) = '26624' THEN 'External Hire'
            WHEN upper(tm.action_reason_text) LIKE '%ACQ%' THEN 'Acquisition Hire'
            WHEN tm.from_position_sid IS NULL
             AND upper(tm.action_code) NOT LIKE '%1XFER%' THEN 'External Hire'
            WHEN (tm.analytics_msr_sid) = 80200
             AND upper(tm.action_code) LIKE '%1XFER%' THEN 'Internal Hire'
            WHEN (tm.analytics_msr_sid) = 80300
             AND upper(deptf.dept_code) = '63110'
             AND upper(deptf.process_level_code) = '26624' THEN 'External Term'
            WHEN (tm.analytics_msr_sid) = 80300
             AND upper(tm.action_code) = '1TERMPEND'
             AND tm.action_reason_text NOT IN(
              'TV-FCLTRNS', 'TV-RELOHCA', 'TV-PLCHG', 'TV-SPCLXFR', 'TI-OUTSRCE', 'TV-RESIDEN', 'TV-ENDASSG', 'TI-DIVEST', 'TV-NOSHOW', 'TV-NOSTART', 'TV-CHAP'
            ) THEN 'External Term'
            WHEN (tm.analytics_msr_sid) = 80300
             AND upper(tm.action_code) LIKE '%1XFER%' THEN 'Internal Term'
            WHEN (tm.analytics_msr_sid) = 80300
             AND upper(tm.action_code) = '1TERMPEND'
             AND tm.action_reason_text IN(
              'TV-FCLTRNS', 'TV-RELOHCA'
            ) THEN 'Internal Term'
            WHEN (tm.analytics_msr_sid) = 80300 THEN 'Other Term'
            ELSE 'XFER'
          END) = 'EXTERNAL HIRE' THEN 'Hire'
          WHEN (tm.analytics_msr_sid) = 80300
           AND upper(CASE
            WHEN (tm.analytics_msr_sid) = 80200
             AND upper(deptf.dept_code) = '63110'
             AND upper(deptf.process_level_code) = '26624' THEN 'External Hire'
            WHEN upper(tm.action_reason_text) LIKE '%ACQ%' THEN 'Acquisition Hire'
            WHEN tm.from_position_sid IS NULL
             AND upper(tm.action_code) NOT LIKE '%1XFER%' THEN 'External Hire'
            WHEN (tm.analytics_msr_sid) = 80200
             AND upper(tm.action_code) LIKE '%1XFER%' THEN 'Internal Hire'
            WHEN (tm.analytics_msr_sid) = 80300
             AND upper(deptf.dept_code) = '63110'
             AND upper(deptf.process_level_code) = '26624' THEN 'External Term'
            WHEN (tm.analytics_msr_sid) = 80300
             AND upper(tm.action_code) = '1TERMPEND'
             AND tm.action_reason_text NOT IN(
              'TV-FCLTRNS', 'TV-RELOHCA', 'TV-PLCHG', 'TV-SPCLXFR', 'TI-OUTSRCE', 'TV-RESIDEN', 'TV-ENDASSG', 'TI-DIVEST', 'TV-NOSHOW', 'TV-NOSTART', 'TV-CHAP'
            ) THEN 'External Term'
            WHEN (tm.analytics_msr_sid) = 80300
             AND upper(tm.action_code) LIKE '%1XFER%' THEN 'Internal Term'
            WHEN (tm.analytics_msr_sid) = 80300
             AND upper(tm.action_code) = '1TERMPEND'
             AND tm.action_reason_text IN(
              'TV-FCLTRNS', 'TV-RELOHCA'
            ) THEN 'Internal Term'
            WHEN (tm.analytics_msr_sid) = 80300 THEN 'Other Term'
            ELSE 'XFER'
          END) = 'EXTERNAL TERM' THEN 'Term'
          WHEN upper(jclf.job_class_code) = '103'
           AND upper(jclt.job_class_code) = '103'
           AND staf.status_code <> stato.status_code THEN 'Aux Status Change'
          WHEN upper(jclf.job_class_code) = '105'
           AND upper(jclt.job_class_code) = '103' THEN 'To RN from Tech'
          WHEN upper(jclf.job_class_code) = '101'
           AND upper(jclt.job_class_code) = '103' THEN 'To RN from Manager'
          WHEN upper(jclf.job_class_code) = '102'
           AND upper(jclt.job_class_code) = '103' THEN 'To RN from Clinical Spec/Prof'
          WHEN upper(jclf.job_class_code) = '104'
           AND upper(jclt.job_class_code) = '103' THEN 'To RN from LPN'
          WHEN upper(jclf.job_class_code) = '106'
           AND upper(jclt.job_class_code) = '103' THEN 'To RN from Clerical'
          WHEN upper(jclf.job_class_code) = '107'
           AND upper(jclt.job_class_code) = '103' THEN 'To RN from Environmental'
          WHEN upper(jclf.job_class_code) = '108'
           AND upper(jclt.job_class_code) = '103' THEN 'To RN from Physician'
          WHEN upper(jclf.job_class_code) = '109'
           AND upper(jclt.job_class_code) = '103' THEN 'To RN from Non-Phys Med Prac'
          WHEN upper(jclf.job_class_code) = '110'
           AND upper(jclt.job_class_code) = '103' THEN 'To RN from Non Clinical Spec/Prof'
          WHEN upper(jclf.job_class_code) = '111'
           AND upper(jclt.job_class_code) = '103' THEN 'To RN from Clinical Techs'
          WHEN upper(jclf.job_class_code) = '103'
           AND upper(jclt.job_class_code) = '101' THEN 'From RN to Manager'
          WHEN upper(jclf.job_class_code) = '103'
           AND upper(jclt.job_class_code) = '102' THEN 'From RN to Clinical Spec/Prof'
          WHEN upper(jclf.job_class_code) = '103'
           AND upper(jclt.job_class_code) = '104' THEN 'From RN to LPN'
          WHEN upper(jclf.job_class_code) = '103'
           AND upper(jclt.job_class_code) = '105' THEN 'From RN to Tech'
          WHEN upper(jclf.job_class_code) = '103'
           AND upper(jclt.job_class_code) = '106' THEN 'From RN to Clerical'
          WHEN upper(jclf.job_class_code) = '103'
           AND upper(jclt.job_class_code) = '107' THEN 'From RN to Environmental'
          WHEN upper(jclf.job_class_code) = '103'
           AND upper(jclt.job_class_code) = '108' THEN 'From RN to Physician'
          WHEN upper(jclf.job_class_code) = '103'
           AND upper(jclt.job_class_code) = '109' THEN 'From RN to Non-Phys Med Prac'
          WHEN upper(jclf.job_class_code) = '103'
           AND upper(jclt.job_class_code) = '110' THEN 'From RN to Non-Clinical Spec/Prof'
          WHEN upper(jclf.job_class_code) = '103'
           AND upper(jclt.job_class_code) = '111' THEN 'From RN to Clinical Techs'
          WHEN (tm.analytics_msr_sid) = 80200 THEN 'Other Hire'
          WHEN (tm.analytics_msr_sid) = 80300 THEN 'Other Term'
          WHEN jclf.job_class_code = jclt.job_class_code
           AND staf.status_code = stato.status_code THEN 'XFER No Chng'
        END) = 'AUX STATUS CHANGE' THEN 'Aux Chg'
        WHEN upper(CASE
          WHEN (tm.analytics_msr_sid) = 80200
           AND upper(CASE
            WHEN (tm.analytics_msr_sid) = 80200
             AND upper(deptf.dept_code) = '63110'
             AND upper(deptf.process_level_code) = '26624' THEN 'External Hire'
            WHEN upper(tm.action_reason_text) LIKE '%ACQ%' THEN 'Acquisition Hire'
            WHEN tm.from_position_sid IS NULL
             AND upper(tm.action_code) NOT LIKE '%1XFER%' THEN 'External Hire'
            WHEN (tm.analytics_msr_sid) = 80200
             AND upper(tm.action_code) LIKE '%1XFER%' THEN 'Internal Hire'
            WHEN (tm.analytics_msr_sid) = 80300
             AND upper(deptf.dept_code) = '63110'
             AND upper(deptf.process_level_code) = '26624' THEN 'External Term'
            WHEN (tm.analytics_msr_sid) = 80300
             AND upper(tm.action_code) = '1TERMPEND'
             AND tm.action_reason_text NOT IN(
              'TV-FCLTRNS', 'TV-RELOHCA', 'TV-PLCHG', 'TV-SPCLXFR', 'TI-OUTSRCE', 'TV-RESIDEN', 'TV-ENDASSG', 'TI-DIVEST', 'TV-NOSHOW', 'TV-NOSTART', 'TV-CHAP'
            ) THEN 'External Term'
            WHEN (tm.analytics_msr_sid) = 80300
             AND upper(tm.action_code) LIKE '%1XFER%' THEN 'Internal Term'
            WHEN (tm.analytics_msr_sid) = 80300
             AND upper(tm.action_code) = '1TERMPEND'
             AND tm.action_reason_text IN(
              'TV-FCLTRNS', 'TV-RELOHCA'
            ) THEN 'Internal Term'
            WHEN (tm.analytics_msr_sid) = 80300 THEN 'Other Term'
            ELSE 'XFER'
          END) = 'EXTERNAL HIRE' THEN 'Hire'
          WHEN (tm.analytics_msr_sid) = 80300
           AND upper(CASE
            WHEN (tm.analytics_msr_sid) = 80200
             AND upper(deptf.dept_code) = '63110'
             AND upper(deptf.process_level_code) = '26624' THEN 'External Hire'
            WHEN upper(tm.action_reason_text) LIKE '%ACQ%' THEN 'Acquisition Hire'
            WHEN tm.from_position_sid IS NULL
             AND upper(tm.action_code) NOT LIKE '%1XFER%' THEN 'External Hire'
            WHEN (tm.analytics_msr_sid) = 80200
             AND upper(tm.action_code) LIKE '%1XFER%' THEN 'Internal Hire'
            WHEN (tm.analytics_msr_sid) = 80300
             AND upper(deptf.dept_code) = '63110'
             AND upper(deptf.process_level_code) = '26624' THEN 'External Term'
            WHEN (tm.analytics_msr_sid) = 80300
             AND upper(tm.action_code) = '1TERMPEND'
             AND tm.action_reason_text NOT IN(
              'TV-FCLTRNS', 'TV-RELOHCA', 'TV-PLCHG', 'TV-SPCLXFR', 'TI-OUTSRCE', 'TV-RESIDEN', 'TV-ENDASSG', 'TI-DIVEST', 'TV-NOSHOW', 'TV-NOSTART', 'TV-CHAP'
            ) THEN 'External Term'
            WHEN (tm.analytics_msr_sid) = 80300
             AND upper(tm.action_code) LIKE '%1XFER%' THEN 'Internal Term'
            WHEN (tm.analytics_msr_sid) = 80300
             AND upper(tm.action_code) = '1TERMPEND'
             AND tm.action_reason_text IN(
              'TV-FCLTRNS', 'TV-RELOHCA'
            ) THEN 'Internal Term'
            WHEN (tm.analytics_msr_sid) = 80300 THEN 'Other Term'
            ELSE 'XFER'
          END) = 'EXTERNAL TERM' THEN 'Term'
          WHEN upper(jclf.job_class_code) = '103'
           AND upper(jclt.job_class_code) = '103'
           AND staf.status_code <> stato.status_code THEN 'Aux Status Change'
          WHEN upper(jclf.job_class_code) = '105'
           AND upper(jclt.job_class_code) = '103' THEN 'To RN from Tech'
          WHEN upper(jclf.job_class_code) = '101'
           AND upper(jclt.job_class_code) = '103' THEN 'To RN from Manager'
          WHEN upper(jclf.job_class_code) = '102'
           AND upper(jclt.job_class_code) = '103' THEN 'To RN from Clinical Spec/Prof'
          WHEN upper(jclf.job_class_code) = '104'
           AND upper(jclt.job_class_code) = '103' THEN 'To RN from LPN'
          WHEN upper(jclf.job_class_code) = '106'
           AND upper(jclt.job_class_code) = '103' THEN 'To RN from Clerical'
          WHEN upper(jclf.job_class_code) = '107'
           AND upper(jclt.job_class_code) = '103' THEN 'To RN from Environmental'
          WHEN upper(jclf.job_class_code) = '108'
           AND upper(jclt.job_class_code) = '103' THEN 'To RN from Physician'
          WHEN upper(jclf.job_class_code) = '109'
           AND upper(jclt.job_class_code) = '103' THEN 'To RN from Non-Phys Med Prac'
          WHEN upper(jclf.job_class_code) = '110'
           AND upper(jclt.job_class_code) = '103' THEN 'To RN from Non Clinical Spec/Prof'
          WHEN upper(jclf.job_class_code) = '111'
           AND upper(jclt.job_class_code) = '103' THEN 'To RN from Clinical Techs'
          WHEN upper(jclf.job_class_code) = '103'
           AND upper(jclt.job_class_code) = '101' THEN 'From RN to Manager'
          WHEN upper(jclf.job_class_code) = '103'
           AND upper(jclt.job_class_code) = '102' THEN 'From RN to Clinical Spec/Prof'
          WHEN upper(jclf.job_class_code) = '103'
           AND upper(jclt.job_class_code) = '104' THEN 'From RN to LPN'
          WHEN upper(jclf.job_class_code) = '103'
           AND upper(jclt.job_class_code) = '105' THEN 'From RN to Tech'
          WHEN upper(jclf.job_class_code) = '103'
           AND upper(jclt.job_class_code) = '106' THEN 'From RN to Clerical'
          WHEN upper(jclf.job_class_code) = '103'
           AND upper(jclt.job_class_code) = '107' THEN 'From RN to Environmental'
          WHEN upper(jclf.job_class_code) = '103'
           AND upper(jclt.job_class_code) = '108' THEN 'From RN to Physician'
          WHEN upper(jclf.job_class_code) = '103'
           AND upper(jclt.job_class_code) = '109' THEN 'From RN to Non-Phys Med Prac'
          WHEN upper(jclf.job_class_code) = '103'
           AND upper(jclt.job_class_code) = '110' THEN 'From RN to Non-Clinical Spec/Prof'
          WHEN upper(jclf.job_class_code) = '103'
           AND upper(jclt.job_class_code) = '111' THEN 'From RN to Clinical Techs'
          WHEN (tm.analytics_msr_sid) = 80200 THEN 'Other Hire'
          WHEN (tm.analytics_msr_sid) = 80300 THEN 'Other Term'
          WHEN jclf.job_class_code = jclt.job_class_code
           AND staf.status_code = stato.status_code THEN 'XFER No Chng'
        END) = 'XFER NO CHNG' THEN 'XFER No Chng'
        ELSE 'SkillMix Chg'
      END AS chg_group,
      compdt.week_number,
      concat(CAST(compdt.week_start_dt as STRING), ' to ', CAST(compdt.week_end_dt as STRING)) AS week_range,
      CASE
        WHEN upper(jclf.job_class_code) = '103'
         AND upper(staf.status_code) = 'FT' THEN -1
        WHEN upper(jclf.job_class_code) = '103'
         AND upper(staf.status_code) = 'PT' THEN -1
        WHEN upper(jclf.job_class_code) = '103'
         AND upper(staf.status_code) = 'PRN' THEN -1
        WHEN upper(jclf.job_class_code) = '103'
         AND upper(staf.status_code) = 'TEMP' THEN -1
        ELSE 0
      END AS msr_value,
      facf.lob_code,
      facf.sub_lob_code,
      -1 AS comp_val_test,
      concat(trim(staf.status_code), ' to ', trim(stato.status_code)) AS aux_chg,
      current_date() AS date_stamp
    FROM
      {{ params.param_hr_base_views_dataset_name }}.fact_total_movement AS tm
      LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.job_class AS jclf ON tm.from_job_class_sid = jclf.job_class_sid
       AND date(jclf.valid_to_date) = '9999-12-31'
      LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.job_class AS jclt ON tm.to_job_class_sid = jclt.job_class_sid
       AND date(jclt.valid_to_date) = '9999-12-31'
      LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.status AS staf ON tm.from_auxiliary_status_sid = staf.status_sid
       AND upper(staf.status_type_code) = 'AUX'
       AND date(staf.valid_to_date) = '9999-12-31'
      LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.status AS stato ON tm.to_auxiliary_status_sid = stato.status_sid
       AND upper(stato.status_type_code) = 'AUX'
       AND date(stato.valid_to_date) = '9999-12-31'
      LEFT OUTER JOIN {{ params.param_pub_views_dataset_name }}.fact_facility AS facf ON tm.from_coid = facf.coid
       AND tm.from_company_code = facf.company_code
      LEFT OUTER JOIN {{ params.param_pub_views_dataset_name }}.fact_facility AS facto ON tm.to_coid = facto.coid
       AND tm.to_company_code = facto.company_code
      LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.department AS deptf ON tm.from_dept_sid = deptf.dept_sid
       AND date(deptf.valid_to_date) = '9999-12-31'
      LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.department AS deptt ON tm.to_dept_sid = deptt.dept_sid
       AND date(deptt.valid_to_date) = '9999-12-31'
      LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.process_level AS plf ON tm.from_process_level_code = plf.process_level_code
       AND tm.from_lawson_company_num = plf.lawson_company_num
       AND date(plf.valid_to_date) = '9999-12-31'
      LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.process_level AS plt ON tm.to_process_level_code = plt.process_level_code
       AND tm.to_lawson_company_num = plt.lawson_company_num
       AND date(plt.valid_to_date) = '9999-12-31'
      LEFT OUTER JOIN (
        SELECT
            fin.employee_sid,
            fin.position_sid,
            fin.employee_num,
            fin.position_level_sequence_num,
            max(fin.position_hire_date) AS position_hire_date
          FROM
            (
              SELECT
                  employee_position.employee_sid,
                  employee_position.position_sid,
                  employee_position.employee_num,
                  employee_position.position_level_sequence_num,
                  employee_position.eff_from_date AS position_hire_date
                FROM
                  {{ params.param_hr_base_views_dataset_name }}.employee_position
                WHERE date(employee_position.valid_to_date) = '9999-12-31'
                QUALIFY CASE
                  WHEN employee_position.position_sid <> lag(employee_position.position_sid, 1, 0) OVER (PARTITION BY employee_position.employee_sid ORDER BY position_hire_date) THEN 1
                  ELSE 0
                END = 1
            ) AS fin
          GROUP BY 1, 2, 3, 4
      ) AS dt_hr ON tm.employee_sid = dt_hr.employee_sid
       AND (tm.to_position_sid = dt_hr.position_sid
       AND (tm.analytics_msr_sid) <> 80300
       OR tm.from_position_sid = dt_hr.position_sid
       AND (tm.analytics_msr_sid) = 80300)
       AND (tm.to_position_level_sequence_num = dt_hr.position_level_sequence_num
       AND (tm.analytics_msr_sid) <> 80300
       OR tm.from_position_level_sequence_num = dt_hr.position_level_sequence_num
       AND (tm.analytics_msr_sid) = 80300)
      LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.job_position AS jobf ON tm.from_position_sid = jobf.position_sid
       AND tm.date_id BETWEEN jobf.eff_from_date AND jobf.eff_to_date
       AND date(jobf.valid_to_date) = '9999-12-31'
      LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.job_position AS jobt ON tm.to_position_sid = jobt.position_sid
       AND tm.date_id BETWEEN jobt.eff_from_date AND jobt.eff_to_date
       AND date(jobt.valid_to_date) = '9999-12-31'
      LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.employee AS emp ON tm.employee_sid = emp.employee_sid
       AND date(emp.valid_to_date) = '9999-12-31'
      LEFT OUTER JOIN {{ params.param_pub_views_dataset_name }}.lu_date AS dt ON tm.date_id = dt.date_id
      INNER JOIN (
        SELECT
            dt_0.week_end_date,
            date_diff(max(dt_0.date_id) , min(dt_0.date_id),day) AS dtdiff,
            min(dt_0.date_id) AS week_start_dt,
            max(dt_0.date_id) AS week_end_dt,
            row_number() OVER (PARTITION BY max(dt_0.date_id) - min(dt_0.date_id) ORDER BY min(dt_0.date_id)) AS week_number
          FROM
            {{ params.param_pub_views_dataset_name }}.lu_date AS dt_0
          WHERE dt_0.date_id >= (
            SELECT
                min(date_id)
              FROM
                {{ params.param_pub_views_dataset_name }}.lu_date
              WHERE date_id > date_sub(current_date(), interval 93 + extract(DAYOFWEEK from current_date()) DAY)
               AND date_id <= current_date()
               AND upper(dow_desc_l) = 'SUNDAY'
          )
           AND dt_0.date_id <= (
            SELECT
                max(date_id)
              FROM
                {{ params.param_pub_views_dataset_name }}.lu_date
              WHERE date_id > date_sub(current_date(), interval 93 + extract(DAYOFWEEK from current_date()) DAY)
               AND date_id <= current_date()
               AND upper(dow_desc_l) = 'SUNDAY'
          ) - 1
          GROUP BY 1
      ) AS compdt ON dt.week_end_date = compdt.week_end_date
       AND compdt.dtdiff = 6
      LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.hr_dept_rollup AS hrdrf ON tm.from_coid = hrdrf.coid
       AND tm.from_process_level_code = hrdrf.process_level_code
       AND tm.from_dept_sid = hrdrf.dept_sid
       AND tm.from_lawson_company_num = hrdrf.lawson_company_num
      LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.hr_dept_rollup AS hrdrt ON tm.to_coid = hrdrt.coid
       AND tm.to_process_level_code = hrdrt.process_level_code
       AND tm.to_dept_sid = hrdrt.dept_sid
       AND tm.to_lawson_company_num = hrdrt.lawson_company_num
    WHERE tm.date_id >= (
      SELECT
          min(date_id)
        FROM
          {{ params.param_pub_views_dataset_name }}.lu_date
        WHERE date_id > date_sub(current_date(), interval 93 + extract(DAYOFWEEK from current_date()) DAY)
         AND date_id <= current_date()
         AND upper(dow_desc_l) = 'SUNDAY'
    )
     AND tm.date_id <= (
      SELECT
          max(date_id)
        FROM
          {{ params.param_pub_views_dataset_name }}.lu_date
        WHERE date_id > date_sub(current_date(), interval 93 + extract(DAYOFWEEK from current_date()) DAY)
         AND date_id <= current_date()
         AND upper(dow_desc_l) = 'SUNDAY'
    )
     AND upper(facf.lob_code) = 'HOS'
     AND (upper(jclf.job_class_code) = '103'
     OR upper(jclt.job_class_code) = '103')
     AND tm.analytics_msr_sid IN(
      -- AND (facf.Sub_LOB_Code IN  ('MED', 'PSY') OR facto.Sub_LOB_Code IN  ('MED', 'PSY'))
      80700, 80300
    )
  UNION ALL
  SELECT
      facto.group_name AS group_name,
      facto.division_name AS division_name,
      facto.unit_num AS unit_num,
      facto.coid_name AS coid_name,
      -- - User requested addition
      'In-bound' AS mov_dir,
      CASE
        WHEN upper(tm.to_process_level_code) = '03216' THEN '27300'
        WHEN upper(tm.to_process_level_code) = '03191' THEN '27400'
        WHEN upper(tm.to_process_level_code) = '03192' THEN '27450'
        WHEN upper(tm.to_process_level_code) = '03175' THEN '27401'
        WHEN upper(tm.to_process_level_code) = '03167' THEN '27100'
        WHEN upper(tm.to_process_level_code) = '03166' THEN '27200'
        WHEN upper(tm.to_process_level_code) = '03170' THEN '27490'
        WHEN upper(tm.to_process_level_code) = '08936' THEN '27150'
        ELSE tm.to_process_level_code
      END AS process_level_code,
      -- - User Requested add to allow them to line up this data with other data they are using where these process level should line up with the changed number in their dataset.
      -- -
      -- -
      -- -
      -- -
      -- -
      -- -
      -- -
      -- -
      -- -
      -- - End change
      hrdrt.cost_center AS cost_center,
      tm.employee_num,
      concat(facto.coid, hrdrt.cost_center) AS coid_cost_center,
      -- - Added per Product team request
      stato.status_code AS aux_status,
      jobt.position_code AS pos_code,
      jclt.job_class_code,
      dt_hr.position_hire_date,
      emp.termination_date,
      CASE
        WHEN (tm.analytics_msr_sid) = 80200
         AND upper(dept_starn.dept_code) = '63110'
         AND upper(dept_starn.process_level_code) = '26624' THEN 'External Hire'
        WHEN upper(tm.action_reason_text) LIKE '%ACQ%' THEN 'Acquisition Hire'
        WHEN lb30.employee_num IS NULL
         AND upper(tm.action_code) NOT LIKE '%1XFER%' THEN 'External Hire'
        WHEN (tm.analytics_msr_sid) = 80200 THEN 'Internal Hire'
        WHEN (tm.analytics_msr_sid) = 80300
         AND upper(deptf.dept_code) = '63110'
         AND upper(deptf.process_level_code) = '26624' THEN 'External Term'
        WHEN (tm.analytics_msr_sid) = 80300
         AND upper(tm.action_code) = '1TERMPEND'
         AND tm.action_reason_text NOT IN(
          'TV-FCLTRNS', 'TV-RELOHCA', 'TV-PLCHG', 'TV-SPCLXFR', 'TI-OUTSRCE', 'TV-RESIDEN', 'TV-ENDASSG', 'TI-DIVEST', 'TV-NOSHOW', 'TV-NOSTART', 'TV-CHAP'
        ) THEN 'External Term'
        WHEN (tm.analytics_msr_sid) = 80300
         AND upper(tm.action_code) LIKE '%1XFER%' THEN 'Internal Term'
        WHEN (tm.analytics_msr_sid) = 80300
         AND upper(tm.action_code) = '1TERMPEND'
         AND tm.action_reason_text IN(
          'TV-FCLTRNS', 'TV-RELOHCA'
        ) THEN 'Internal Term'
        WHEN (tm.analytics_msr_sid) = 80300 THEN 'Other Term'
        ELSE 'XFER'
      END AS ext_int_type,
      CASE
        WHEN (tm.analytics_msr_sid) = 80200
         AND upper(CASE
          WHEN (tm.analytics_msr_sid) = 80200
           AND upper(dept_starn.dept_code) = '63110'
           AND upper(dept_starn.process_level_code) = '26624' THEN 'External Hire'
          WHEN upper(tm.action_reason_text) LIKE '%ACQ%' THEN 'Acquisition Hire'
          WHEN lb30.employee_num IS NULL
           AND upper(tm.action_code) NOT LIKE '%1XFER%' THEN 'External Hire'
          WHEN (tm.analytics_msr_sid) = 80200 THEN 'Internal Hire'
          WHEN (tm.analytics_msr_sid) = 80300
           AND upper(deptf.dept_code) = '63110'
           AND upper(deptf.process_level_code) = '26624' THEN 'External Term'
          WHEN (tm.analytics_msr_sid) = 80300
           AND upper(tm.action_code) = '1TERMPEND'
           AND tm.action_reason_text NOT IN(
            'TV-FCLTRNS', 'TV-RELOHCA', 'TV-PLCHG', 'TV-SPCLXFR', 'TI-OUTSRCE', 'TV-RESIDEN', 'TV-ENDASSG', 'TI-DIVEST', 'TV-NOSHOW', 'TV-NOSTART', 'TV-CHAP'
          ) THEN 'External Term'
          WHEN (tm.analytics_msr_sid) = 80300
           AND upper(tm.action_code) LIKE '%1XFER%' THEN 'Internal Term'
          WHEN (tm.analytics_msr_sid) = 80300
           AND upper(tm.action_code) = '1TERMPEND'
           AND tm.action_reason_text IN(
            'TV-FCLTRNS', 'TV-RELOHCA'
          ) THEN 'Internal Term'
          WHEN (tm.analytics_msr_sid) = 80300 THEN 'Other Term'
          ELSE 'XFER'
        END) = 'EXTERNAL HIRE' THEN 'Hire'
        WHEN (tm.analytics_msr_sid) = 80300
         AND upper(CASE
          WHEN (tm.analytics_msr_sid) = 80200
           AND upper(dept_starn.dept_code) = '63110'
           AND upper(dept_starn.process_level_code) = '26624' THEN 'External Hire'
          WHEN upper(tm.action_reason_text) LIKE '%ACQ%' THEN 'Acquisition Hire'
          WHEN lb30.employee_num IS NULL
           AND upper(tm.action_code) NOT LIKE '%1XFER%' THEN 'External Hire'
          WHEN (tm.analytics_msr_sid) = 80200 THEN 'Internal Hire'
          WHEN (tm.analytics_msr_sid) = 80300
           AND upper(deptf.dept_code) = '63110'
           AND upper(deptf.process_level_code) = '26624' THEN 'External Term'
          WHEN (tm.analytics_msr_sid) = 80300
           AND upper(tm.action_code) = '1TERMPEND'
           AND tm.action_reason_text NOT IN(
            'TV-FCLTRNS', 'TV-RELOHCA', 'TV-PLCHG', 'TV-SPCLXFR', 'TI-OUTSRCE', 'TV-RESIDEN', 'TV-ENDASSG', 'TI-DIVEST', 'TV-NOSHOW', 'TV-NOSTART', 'TV-CHAP'
          ) THEN 'External Term'
          WHEN (tm.analytics_msr_sid) = 80300
           AND upper(tm.action_code) LIKE '%1XFER%' THEN 'Internal Term'
          WHEN (tm.analytics_msr_sid) = 80300
           AND upper(tm.action_code) = '1TERMPEND'
           AND tm.action_reason_text IN(
            'TV-FCLTRNS', 'TV-RELOHCA'
          ) THEN 'Internal Term'
          WHEN (tm.analytics_msr_sid) = 80300 THEN 'Other Term'
          ELSE 'XFER'
        END) = 'EXTERNAL TERM' THEN 'Term'
        WHEN upper(jclf.job_class_code) = '103'
         AND upper(jclt.job_class_code) = '103'
         AND staf.status_code <> stato.status_code THEN 'Aux Status Change'
        WHEN upper(jclf.job_class_code) = '105'
         AND upper(jclt.job_class_code) = '103' THEN 'To RN from Tech'
        WHEN upper(jclf.job_class_code) = '101'
         AND upper(jclt.job_class_code) = '103' THEN 'To RN from Manager'
        WHEN upper(jclf.job_class_code) = '102'
         AND upper(jclt.job_class_code) = '103' THEN 'To RN from Clinical Spec/Prof'
        WHEN upper(jclf.job_class_code) = '104'
         AND upper(jclt.job_class_code) = '103' THEN 'To RN from LPN'
        WHEN upper(jclf.job_class_code) = '106'
         AND upper(jclt.job_class_code) = '103' THEN 'To RN from Clerical'
        WHEN upper(jclf.job_class_code) = '107'
         AND upper(jclt.job_class_code) = '103' THEN 'To RN from Environmental'
        WHEN upper(jclf.job_class_code) = '108'
         AND upper(jclt.job_class_code) = '103' THEN 'To RN from Physician'
        WHEN upper(jclf.job_class_code) = '109'
         AND upper(jclt.job_class_code) = '103' THEN 'To RN from Non-Phys Med Prac'
        WHEN upper(jclf.job_class_code) = '110'
         AND upper(jclt.job_class_code) = '103' THEN 'To RN from Non Clinical Spec/Prof'
        WHEN upper(jclf.job_class_code) = '111'
         AND upper(jclt.job_class_code) = '103' THEN 'To RN from Clinical Techs'
        WHEN upper(jclf.job_class_code) = '103'
         AND upper(jclt.job_class_code) = '101' THEN 'From RN to Manager'
        WHEN upper(jclf.job_class_code) = '103'
         AND upper(jclt.job_class_code) = '102' THEN 'From RN to Clinical Spec/Prof'
        WHEN upper(jclf.job_class_code) = '103'
         AND upper(jclt.job_class_code) = '104' THEN 'From RN to LPN'
        WHEN upper(jclf.job_class_code) = '103'
         AND upper(jclt.job_class_code) = '105' THEN 'From RN to Tech'
        WHEN upper(jclf.job_class_code) = '103'
         AND upper(jclt.job_class_code) = '106' THEN 'From RN to Clerical'
        WHEN upper(jclf.job_class_code) = '103'
         AND upper(jclt.job_class_code) = '107' THEN 'From RN to Environmental'
        WHEN upper(jclf.job_class_code) = '103'
         AND upper(jclt.job_class_code) = '108' THEN 'From RN to Physician'
        WHEN upper(jclf.job_class_code) = '103'
         AND upper(jclt.job_class_code) = '109' THEN 'From RN to Non-Phys Med Prac'
        WHEN upper(jclf.job_class_code) = '103'
         AND upper(jclt.job_class_code) = '110' THEN 'From RN to Non-Clinical Spec/Prof'
        WHEN upper(jclf.job_class_code) = '103'
         AND upper(jclt.job_class_code) = '111' THEN 'From RN to Clinical Techs'
        WHEN (tm.analytics_msr_sid) = 80200 THEN 'Other Hire'
        WHEN (tm.analytics_msr_sid) = 80300 THEN 'Other Term'
        WHEN jclf.job_class_code = jclt.job_class_code
         AND staf.status_code = stato.status_code THEN 'XFER No Chng'
      END AS change_type,
      -- ELSE 'XFER_No_Chng'
      CASE
        WHEN upper(CASE
          WHEN (tm.analytics_msr_sid) = 80200
           AND upper(CASE
            WHEN (tm.analytics_msr_sid) = 80200
             AND upper(dept_starn.dept_code) = '63110'
             AND upper(dept_starn.process_level_code) = '26624' THEN 'External Hire'
            WHEN upper(tm.action_reason_text) LIKE '%ACQ%' THEN 'Acquisition Hire'
            WHEN lb30.employee_num IS NULL
             AND upper(tm.action_code) NOT LIKE '%1XFER%' THEN 'External Hire'
            WHEN (tm.analytics_msr_sid) = 80200 THEN 'Internal Hire'
            WHEN (tm.analytics_msr_sid) = 80300
             AND upper(deptf.dept_code) = '63110'
             AND upper(deptf.process_level_code) = '26624' THEN 'External Term'
            WHEN (tm.analytics_msr_sid) = 80300
             AND upper(tm.action_code) = '1TERMPEND'
             AND tm.action_reason_text NOT IN(
              'TV-FCLTRNS', 'TV-RELOHCA', 'TV-PLCHG', 'TV-SPCLXFR', 'TI-OUTSRCE', 'TV-RESIDEN', 'TV-ENDASSG', 'TI-DIVEST', 'TV-NOSHOW', 'TV-NOSTART', 'TV-CHAP'
            ) THEN 'External Term'
            WHEN (tm.analytics_msr_sid) = 80300
             AND upper(tm.action_code) LIKE '%1XFER%' THEN 'Internal Term'
            WHEN (tm.analytics_msr_sid) = 80300
             AND upper(tm.action_code) = '1TERMPEND'
             AND tm.action_reason_text IN(
              'TV-FCLTRNS', 'TV-RELOHCA'
            ) THEN 'Internal Term'
            WHEN (tm.analytics_msr_sid) = 80300 THEN 'Other Term'
            ELSE 'XFER'
          END) = 'EXTERNAL HIRE' THEN 'Hire'
          WHEN (tm.analytics_msr_sid) = 80300
           AND upper(CASE
            WHEN (tm.analytics_msr_sid) = 80200
             AND upper(dept_starn.dept_code) = '63110'
             AND upper(dept_starn.process_level_code) = '26624' THEN 'External Hire'
            WHEN upper(tm.action_reason_text) LIKE '%ACQ%' THEN 'Acquisition Hire'
            WHEN lb30.employee_num IS NULL
             AND upper(tm.action_code) NOT LIKE '%1XFER%' THEN 'External Hire'
            WHEN (tm.analytics_msr_sid) = 80200 THEN 'Internal Hire'
            WHEN (tm.analytics_msr_sid) = 80300
             AND upper(deptf.dept_code) = '63110'
             AND upper(deptf.process_level_code) = '26624' THEN 'External Term'
            WHEN (tm.analytics_msr_sid) = 80300
             AND upper(tm.action_code) = '1TERMPEND'
             AND tm.action_reason_text NOT IN(
              'TV-FCLTRNS', 'TV-RELOHCA', 'TV-PLCHG', 'TV-SPCLXFR', 'TI-OUTSRCE', 'TV-RESIDEN', 'TV-ENDASSG', 'TI-DIVEST', 'TV-NOSHOW', 'TV-NOSTART', 'TV-CHAP'
            ) THEN 'External Term'
            WHEN (tm.analytics_msr_sid) = 80300
             AND upper(tm.action_code) LIKE '%1XFER%' THEN 'Internal Term'
            WHEN (tm.analytics_msr_sid) = 80300
             AND upper(tm.action_code) = '1TERMPEND'
             AND tm.action_reason_text IN(
              'TV-FCLTRNS', 'TV-RELOHCA'
            ) THEN 'Internal Term'
            WHEN (tm.analytics_msr_sid) = 80300 THEN 'Other Term'
            ELSE 'XFER'
          END) = 'EXTERNAL TERM' THEN 'Term'
          WHEN upper(jclf.job_class_code) = '103'
           AND upper(jclt.job_class_code) = '103'
           AND staf.status_code <> stato.status_code THEN 'Aux Status Change'
          WHEN upper(jclf.job_class_code) = '105'
           AND upper(jclt.job_class_code) = '103' THEN 'To RN from Tech'
          WHEN upper(jclf.job_class_code) = '101'
           AND upper(jclt.job_class_code) = '103' THEN 'To RN from Manager'
          WHEN upper(jclf.job_class_code) = '102'
           AND upper(jclt.job_class_code) = '103' THEN 'To RN from Clinical Spec/Prof'
          WHEN upper(jclf.job_class_code) = '104'
           AND upper(jclt.job_class_code) = '103' THEN 'To RN from LPN'
          WHEN upper(jclf.job_class_code) = '106'
           AND upper(jclt.job_class_code) = '103' THEN 'To RN from Clerical'
          WHEN upper(jclf.job_class_code) = '107'
           AND upper(jclt.job_class_code) = '103' THEN 'To RN from Environmental'
          WHEN upper(jclf.job_class_code) = '108'
           AND upper(jclt.job_class_code) = '103' THEN 'To RN from Physician'
          WHEN upper(jclf.job_class_code) = '109'
           AND upper(jclt.job_class_code) = '103' THEN 'To RN from Non-Phys Med Prac'
          WHEN upper(jclf.job_class_code) = '110'
           AND upper(jclt.job_class_code) = '103' THEN 'To RN from Non Clinical Spec/Prof'
          WHEN upper(jclf.job_class_code) = '111'
           AND upper(jclt.job_class_code) = '103' THEN 'To RN from Clinical Techs'
          WHEN upper(jclf.job_class_code) = '103'
           AND upper(jclt.job_class_code) = '101' THEN 'From RN to Manager'
          WHEN upper(jclf.job_class_code) = '103'
           AND upper(jclt.job_class_code) = '102' THEN 'From RN to Clinical Spec/Prof'
          WHEN upper(jclf.job_class_code) = '103'
           AND upper(jclt.job_class_code) = '104' THEN 'From RN to LPN'
          WHEN upper(jclf.job_class_code) = '103'
           AND upper(jclt.job_class_code) = '105' THEN 'From RN to Tech'
          WHEN upper(jclf.job_class_code) = '103'
           AND upper(jclt.job_class_code) = '106' THEN 'From RN to Clerical'
          WHEN upper(jclf.job_class_code) = '103'
           AND upper(jclt.job_class_code) = '107' THEN 'From RN to Environmental'
          WHEN upper(jclf.job_class_code) = '103'
           AND upper(jclt.job_class_code) = '108' THEN 'From RN to Physician'
          WHEN upper(jclf.job_class_code) = '103'
           AND upper(jclt.job_class_code) = '109' THEN 'From RN to Non-Phys Med Prac'
          WHEN upper(jclf.job_class_code) = '103'
           AND upper(jclt.job_class_code) = '110' THEN 'From RN to Non-Clinical Spec/Prof'
          WHEN upper(jclf.job_class_code) = '103'
           AND upper(jclt.job_class_code) = '111' THEN 'From RN to Clinical Techs'
          WHEN (tm.analytics_msr_sid) = 80200 THEN 'Other Hire'
          WHEN (tm.analytics_msr_sid) = 80300 THEN 'Other Term'
          WHEN jclf.job_class_code = jclt.job_class_code
           AND staf.status_code = stato.status_code THEN 'XFER No Chng'
        END) = 'TERM' THEN 'TERM'
        WHEN upper(CASE
          WHEN (tm.analytics_msr_sid) = 80200
           AND upper(CASE
            WHEN (tm.analytics_msr_sid) = 80200
             AND upper(dept_starn.dept_code) = '63110'
             AND upper(dept_starn.process_level_code) = '26624' THEN 'External Hire'
            WHEN upper(tm.action_reason_text) LIKE '%ACQ%' THEN 'Acquisition Hire'
            WHEN lb30.employee_num IS NULL
             AND upper(tm.action_code) NOT LIKE '%1XFER%' THEN 'External Hire'
            WHEN (tm.analytics_msr_sid) = 80200 THEN 'Internal Hire'
            WHEN (tm.analytics_msr_sid) = 80300
             AND upper(deptf.dept_code) = '63110'
             AND upper(deptf.process_level_code) = '26624' THEN 'External Term'
            WHEN (tm.analytics_msr_sid) = 80300
             AND upper(tm.action_code) = '1TERMPEND'
             AND tm.action_reason_text NOT IN(
              'TV-FCLTRNS', 'TV-RELOHCA', 'TV-PLCHG', 'TV-SPCLXFR', 'TI-OUTSRCE', 'TV-RESIDEN', 'TV-ENDASSG', 'TI-DIVEST', 'TV-NOSHOW', 'TV-NOSTART', 'TV-CHAP'
            ) THEN 'External Term'
            WHEN (tm.analytics_msr_sid) = 80300
             AND upper(tm.action_code) LIKE '%1XFER%' THEN 'Internal Term'
            WHEN (tm.analytics_msr_sid) = 80300
             AND upper(tm.action_code) = '1TERMPEND'
             AND tm.action_reason_text IN(
              'TV-FCLTRNS', 'TV-RELOHCA'
            ) THEN 'Internal Term'
            WHEN (tm.analytics_msr_sid) = 80300 THEN 'Other Term'
            ELSE 'XFER'
          END) = 'EXTERNAL HIRE' THEN 'Hire'
          WHEN (tm.analytics_msr_sid) = 80300
           AND upper(CASE
            WHEN (tm.analytics_msr_sid) = 80200
             AND upper(dept_starn.dept_code) = '63110'
             AND upper(dept_starn.process_level_code) = '26624' THEN 'External Hire'
            WHEN upper(tm.action_reason_text) LIKE '%ACQ%' THEN 'Acquisition Hire'
            WHEN lb30.employee_num IS NULL
             AND upper(tm.action_code) NOT LIKE '%1XFER%' THEN 'External Hire'
            WHEN (tm.analytics_msr_sid) = 80200 THEN 'Internal Hire'
            WHEN (tm.analytics_msr_sid) = 80300
             AND upper(deptf.dept_code) = '63110'
             AND upper(deptf.process_level_code) = '26624' THEN 'External Term'
            WHEN (tm.analytics_msr_sid) = 80300
             AND upper(tm.action_code) = '1TERMPEND'
             AND tm.action_reason_text NOT IN(
              'TV-FCLTRNS', 'TV-RELOHCA', 'TV-PLCHG', 'TV-SPCLXFR', 'TI-OUTSRCE', 'TV-RESIDEN', 'TV-ENDASSG', 'TI-DIVEST', 'TV-NOSHOW', 'TV-NOSTART', 'TV-CHAP'
            ) THEN 'External Term'
            WHEN (tm.analytics_msr_sid) = 80300
             AND upper(tm.action_code) LIKE '%1XFER%' THEN 'Internal Term'
            WHEN (tm.analytics_msr_sid) = 80300
             AND upper(tm.action_code) = '1TERMPEND'
             AND tm.action_reason_text IN(
              'TV-FCLTRNS', 'TV-RELOHCA'
            ) THEN 'Internal Term'
            WHEN (tm.analytics_msr_sid) = 80300 THEN 'Other Term'
            ELSE 'XFER'
          END) = 'EXTERNAL TERM' THEN 'Term'
          WHEN upper(jclf.job_class_code) = '103'
           AND upper(jclt.job_class_code) = '103'
           AND staf.status_code <> stato.status_code THEN 'Aux Status Change'
          WHEN upper(jclf.job_class_code) = '105'
           AND upper(jclt.job_class_code) = '103' THEN 'To RN from Tech'
          WHEN upper(jclf.job_class_code) = '101'
           AND upper(jclt.job_class_code) = '103' THEN 'To RN from Manager'
          WHEN upper(jclf.job_class_code) = '102'
           AND upper(jclt.job_class_code) = '103' THEN 'To RN from Clinical Spec/Prof'
          WHEN upper(jclf.job_class_code) = '104'
           AND upper(jclt.job_class_code) = '103' THEN 'To RN from LPN'
          WHEN upper(jclf.job_class_code) = '106'
           AND upper(jclt.job_class_code) = '103' THEN 'To RN from Clerical'
          WHEN upper(jclf.job_class_code) = '107'
           AND upper(jclt.job_class_code) = '103' THEN 'To RN from Environmental'
          WHEN upper(jclf.job_class_code) = '108'
           AND upper(jclt.job_class_code) = '103' THEN 'To RN from Physician'
          WHEN upper(jclf.job_class_code) = '109'
           AND upper(jclt.job_class_code) = '103' THEN 'To RN from Non-Phys Med Prac'
          WHEN upper(jclf.job_class_code) = '110'
           AND upper(jclt.job_class_code) = '103' THEN 'To RN from Non Clinical Spec/Prof'
          WHEN upper(jclf.job_class_code) = '111'
           AND upper(jclt.job_class_code) = '103' THEN 'To RN from Clinical Techs'
          WHEN upper(jclf.job_class_code) = '103'
           AND upper(jclt.job_class_code) = '101' THEN 'From RN to Manager'
          WHEN upper(jclf.job_class_code) = '103'
           AND upper(jclt.job_class_code) = '102' THEN 'From RN to Clinical Spec/Prof'
          WHEN upper(jclf.job_class_code) = '103'
           AND upper(jclt.job_class_code) = '104' THEN 'From RN to LPN'
          WHEN upper(jclf.job_class_code) = '103'
           AND upper(jclt.job_class_code) = '105' THEN 'From RN to Tech'
          WHEN upper(jclf.job_class_code) = '103'
           AND upper(jclt.job_class_code) = '106' THEN 'From RN to Clerical'
          WHEN upper(jclf.job_class_code) = '103'
           AND upper(jclt.job_class_code) = '107' THEN 'From RN to Environmental'
          WHEN upper(jclf.job_class_code) = '103'
           AND upper(jclt.job_class_code) = '108' THEN 'From RN to Physician'
          WHEN upper(jclf.job_class_code) = '103'
           AND upper(jclt.job_class_code) = '109' THEN 'From RN to Non-Phys Med Prac'
          WHEN upper(jclf.job_class_code) = '103'
           AND upper(jclt.job_class_code) = '110' THEN 'From RN to Non-Clinical Spec/Prof'
          WHEN upper(jclf.job_class_code) = '103'
           AND upper(jclt.job_class_code) = '111' THEN 'From RN to Clinical Techs'
          WHEN (tm.analytics_msr_sid) = 80200 THEN 'Other Hire'
          WHEN (tm.analytics_msr_sid) = 80300 THEN 'Other Term'
          WHEN jclf.job_class_code = jclt.job_class_code
           AND staf.status_code = stato.status_code THEN 'XFER No Chng'
        END) = 'HIRE' THEN 'Hire'
        WHEN upper(CASE
          WHEN (tm.analytics_msr_sid) = 80200
           AND upper(CASE
            WHEN (tm.analytics_msr_sid) = 80200
             AND upper(dept_starn.dept_code) = '63110'
             AND upper(dept_starn.process_level_code) = '26624' THEN 'External Hire'
            WHEN upper(tm.action_reason_text) LIKE '%ACQ%' THEN 'Acquisition Hire'
            WHEN lb30.employee_num IS NULL
             AND upper(tm.action_code) NOT LIKE '%1XFER%' THEN 'External Hire'
            WHEN (tm.analytics_msr_sid) = 80200 THEN 'Internal Hire'
            WHEN (tm.analytics_msr_sid) = 80300
             AND upper(deptf.dept_code) = '63110'
             AND upper(deptf.process_level_code) = '26624' THEN 'External Term'
            WHEN (tm.analytics_msr_sid) = 80300
             AND upper(tm.action_code) = '1TERMPEND'
             AND tm.action_reason_text NOT IN(
              'TV-FCLTRNS', 'TV-RELOHCA', 'TV-PLCHG', 'TV-SPCLXFR', 'TI-OUTSRCE', 'TV-RESIDEN', 'TV-ENDASSG', 'TI-DIVEST', 'TV-NOSHOW', 'TV-NOSTART', 'TV-CHAP'
            ) THEN 'External Term'
            WHEN (tm.analytics_msr_sid) = 80300
             AND upper(tm.action_code) LIKE '%1XFER%' THEN 'Internal Term'
            WHEN (tm.analytics_msr_sid) = 80300
             AND upper(tm.action_code) = '1TERMPEND'
             AND tm.action_reason_text IN(
              'TV-FCLTRNS', 'TV-RELOHCA'
            ) THEN 'Internal Term'
            WHEN (tm.analytics_msr_sid) = 80300 THEN 'Other Term'
            ELSE 'XFER'
          END) = 'EXTERNAL HIRE' THEN 'Hire'
          WHEN (tm.analytics_msr_sid) = 80300
           AND upper(CASE
            WHEN (tm.analytics_msr_sid) = 80200
             AND upper(dept_starn.dept_code) = '63110'
             AND upper(dept_starn.process_level_code) = '26624' THEN 'External Hire'
            WHEN upper(tm.action_reason_text) LIKE '%ACQ%' THEN 'Acquisition Hire'
            WHEN lb30.employee_num IS NULL
             AND upper(tm.action_code) NOT LIKE '%1XFER%' THEN 'External Hire'
            WHEN (tm.analytics_msr_sid) = 80200 THEN 'Internal Hire'
            WHEN (tm.analytics_msr_sid) = 80300
             AND upper(deptf.dept_code) = '63110'
             AND upper(deptf.process_level_code) = '26624' THEN 'External Term'
            WHEN (tm.analytics_msr_sid) = 80300
             AND upper(tm.action_code) = '1TERMPEND'
             AND tm.action_reason_text NOT IN(
              'TV-FCLTRNS', 'TV-RELOHCA', 'TV-PLCHG', 'TV-SPCLXFR', 'TI-OUTSRCE', 'TV-RESIDEN', 'TV-ENDASSG', 'TI-DIVEST', 'TV-NOSHOW', 'TV-NOSTART', 'TV-CHAP'
            ) THEN 'External Term'
            WHEN (tm.analytics_msr_sid) = 80300
             AND upper(tm.action_code) LIKE '%1XFER%' THEN 'Internal Term'
            WHEN (tm.analytics_msr_sid) = 80300
             AND upper(tm.action_code) = '1TERMPEND'
             AND tm.action_reason_text IN(
              'TV-FCLTRNS', 'TV-RELOHCA'
            ) THEN 'Internal Term'
            WHEN (tm.analytics_msr_sid) = 80300 THEN 'Other Term'
            ELSE 'XFER'
          END) = 'EXTERNAL TERM' THEN 'Term'
          WHEN upper(jclf.job_class_code) = '103'
           AND upper(jclt.job_class_code) = '103'
           AND staf.status_code <> stato.status_code THEN 'Aux Status Change'
          WHEN upper(jclf.job_class_code) = '105'
           AND upper(jclt.job_class_code) = '103' THEN 'To RN from Tech'
          WHEN upper(jclf.job_class_code) = '101'
           AND upper(jclt.job_class_code) = '103' THEN 'To RN from Manager'
          WHEN upper(jclf.job_class_code) = '102'
           AND upper(jclt.job_class_code) = '103' THEN 'To RN from Clinical Spec/Prof'
          WHEN upper(jclf.job_class_code) = '104'
           AND upper(jclt.job_class_code) = '103' THEN 'To RN from LPN'
          WHEN upper(jclf.job_class_code) = '106'
           AND upper(jclt.job_class_code) = '103' THEN 'To RN from Clerical'
          WHEN upper(jclf.job_class_code) = '107'
           AND upper(jclt.job_class_code) = '103' THEN 'To RN from Environmental'
          WHEN upper(jclf.job_class_code) = '108'
           AND upper(jclt.job_class_code) = '103' THEN 'To RN from Physician'
          WHEN upper(jclf.job_class_code) = '109'
           AND upper(jclt.job_class_code) = '103' THEN 'To RN from Non-Phys Med Prac'
          WHEN upper(jclf.job_class_code) = '110'
           AND upper(jclt.job_class_code) = '103' THEN 'To RN from Non Clinical Spec/Prof'
          WHEN upper(jclf.job_class_code) = '111'
           AND upper(jclt.job_class_code) = '103' THEN 'To RN from Clinical Techs'
          WHEN upper(jclf.job_class_code) = '103'
           AND upper(jclt.job_class_code) = '101' THEN 'From RN to Manager'
          WHEN upper(jclf.job_class_code) = '103'
           AND upper(jclt.job_class_code) = '102' THEN 'From RN to Clinical Spec/Prof'
          WHEN upper(jclf.job_class_code) = '103'
           AND upper(jclt.job_class_code) = '104' THEN 'From RN to LPN'
          WHEN upper(jclf.job_class_code) = '103'
           AND upper(jclt.job_class_code) = '105' THEN 'From RN to Tech'
          WHEN upper(jclf.job_class_code) = '103'
           AND upper(jclt.job_class_code) = '106' THEN 'From RN to Clerical'
          WHEN upper(jclf.job_class_code) = '103'
           AND upper(jclt.job_class_code) = '107' THEN 'From RN to Environmental'
          WHEN upper(jclf.job_class_code) = '103'
           AND upper(jclt.job_class_code) = '108' THEN 'From RN to Physician'
          WHEN upper(jclf.job_class_code) = '103'
           AND upper(jclt.job_class_code) = '109' THEN 'From RN to Non-Phys Med Prac'
          WHEN upper(jclf.job_class_code) = '103'
           AND upper(jclt.job_class_code) = '110' THEN 'From RN to Non-Clinical Spec/Prof'
          WHEN upper(jclf.job_class_code) = '103'
           AND upper(jclt.job_class_code) = '111' THEN 'From RN to Clinical Techs'
          WHEN (tm.analytics_msr_sid) = 80200 THEN 'Other Hire'
          WHEN (tm.analytics_msr_sid) = 80300 THEN 'Other Term'
          WHEN jclf.job_class_code = jclt.job_class_code
           AND staf.status_code = stato.status_code THEN 'XFER No Chng'
        END) = 'OTHER TERM' THEN 'Other Term'
        WHEN upper(CASE
          WHEN (tm.analytics_msr_sid) = 80200
           AND upper(CASE
            WHEN (tm.analytics_msr_sid) = 80200
             AND upper(dept_starn.dept_code) = '63110'
             AND upper(dept_starn.process_level_code) = '26624' THEN 'External Hire'
            WHEN upper(tm.action_reason_text) LIKE '%ACQ%' THEN 'Acquisition Hire'
            WHEN lb30.employee_num IS NULL
             AND upper(tm.action_code) NOT LIKE '%1XFER%' THEN 'External Hire'
            WHEN (tm.analytics_msr_sid) = 80200 THEN 'Internal Hire'
            WHEN (tm.analytics_msr_sid) = 80300
             AND upper(deptf.dept_code) = '63110'
             AND upper(deptf.process_level_code) = '26624' THEN 'External Term'
            WHEN (tm.analytics_msr_sid) = 80300
             AND upper(tm.action_code) = '1TERMPEND'
             AND tm.action_reason_text NOT IN(
              'TV-FCLTRNS', 'TV-RELOHCA', 'TV-PLCHG', 'TV-SPCLXFR', 'TI-OUTSRCE', 'TV-RESIDEN', 'TV-ENDASSG', 'TI-DIVEST', 'TV-NOSHOW', 'TV-NOSTART', 'TV-CHAP'
            ) THEN 'External Term'
            WHEN (tm.analytics_msr_sid) = 80300
             AND upper(tm.action_code) LIKE '%1XFER%' THEN 'Internal Term'
            WHEN (tm.analytics_msr_sid) = 80300
             AND upper(tm.action_code) = '1TERMPEND'
             AND tm.action_reason_text IN(
              'TV-FCLTRNS', 'TV-RELOHCA'
            ) THEN 'Internal Term'
            WHEN (tm.analytics_msr_sid) = 80300 THEN 'Other Term'
            ELSE 'XFER'
          END) = 'EXTERNAL HIRE' THEN 'Hire'
          WHEN (tm.analytics_msr_sid) = 80300
           AND upper(CASE
            WHEN (tm.analytics_msr_sid) = 80200
             AND upper(dept_starn.dept_code) = '63110'
             AND upper(dept_starn.process_level_code) = '26624' THEN 'External Hire'
            WHEN upper(tm.action_reason_text) LIKE '%ACQ%' THEN 'Acquisition Hire'
            WHEN lb30.employee_num IS NULL
             AND upper(tm.action_code) NOT LIKE '%1XFER%' THEN 'External Hire'
            WHEN (tm.analytics_msr_sid) = 80200 THEN 'Internal Hire'
            WHEN (tm.analytics_msr_sid) = 80300
             AND upper(deptf.dept_code) = '63110'
             AND upper(deptf.process_level_code) = '26624' THEN 'External Term'
            WHEN (tm.analytics_msr_sid) = 80300
             AND upper(tm.action_code) = '1TERMPEND'
             AND tm.action_reason_text NOT IN(
              'TV-FCLTRNS', 'TV-RELOHCA', 'TV-PLCHG', 'TV-SPCLXFR', 'TI-OUTSRCE', 'TV-RESIDEN', 'TV-ENDASSG', 'TI-DIVEST', 'TV-NOSHOW', 'TV-NOSTART', 'TV-CHAP'
            ) THEN 'External Term'
            WHEN (tm.analytics_msr_sid) = 80300
             AND upper(tm.action_code) LIKE '%1XFER%' THEN 'Internal Term'
            WHEN (tm.analytics_msr_sid) = 80300
             AND upper(tm.action_code) = '1TERMPEND'
             AND tm.action_reason_text IN(
              'TV-FCLTRNS', 'TV-RELOHCA'
            ) THEN 'Internal Term'
            WHEN (tm.analytics_msr_sid) = 80300 THEN 'Other Term'
            ELSE 'XFER'
          END) = 'EXTERNAL TERM' THEN 'Term'
          WHEN upper(jclf.job_class_code) = '103'
           AND upper(jclt.job_class_code) = '103'
           AND staf.status_code <> stato.status_code THEN 'Aux Status Change'
          WHEN upper(jclf.job_class_code) = '105'
           AND upper(jclt.job_class_code) = '103' THEN 'To RN from Tech'
          WHEN upper(jclf.job_class_code) = '101'
           AND upper(jclt.job_class_code) = '103' THEN 'To RN from Manager'
          WHEN upper(jclf.job_class_code) = '102'
           AND upper(jclt.job_class_code) = '103' THEN 'To RN from Clinical Spec/Prof'
          WHEN upper(jclf.job_class_code) = '104'
           AND upper(jclt.job_class_code) = '103' THEN 'To RN from LPN'
          WHEN upper(jclf.job_class_code) = '106'
           AND upper(jclt.job_class_code) = '103' THEN 'To RN from Clerical'
          WHEN upper(jclf.job_class_code) = '107'
           AND upper(jclt.job_class_code) = '103' THEN 'To RN from Environmental'
          WHEN upper(jclf.job_class_code) = '108'
           AND upper(jclt.job_class_code) = '103' THEN 'To RN from Physician'
          WHEN upper(jclf.job_class_code) = '109'
           AND upper(jclt.job_class_code) = '103' THEN 'To RN from Non-Phys Med Prac'
          WHEN upper(jclf.job_class_code) = '110'
           AND upper(jclt.job_class_code) = '103' THEN 'To RN from Non Clinical Spec/Prof'
          WHEN upper(jclf.job_class_code) = '111'
           AND upper(jclt.job_class_code) = '103' THEN 'To RN from Clinical Techs'
          WHEN upper(jclf.job_class_code) = '103'
           AND upper(jclt.job_class_code) = '101' THEN 'From RN to Manager'
          WHEN upper(jclf.job_class_code) = '103'
           AND upper(jclt.job_class_code) = '102' THEN 'From RN to Clinical Spec/Prof'
          WHEN upper(jclf.job_class_code) = '103'
           AND upper(jclt.job_class_code) = '104' THEN 'From RN to LPN'
          WHEN upper(jclf.job_class_code) = '103'
           AND upper(jclt.job_class_code) = '105' THEN 'From RN to Tech'
          WHEN upper(jclf.job_class_code) = '103'
           AND upper(jclt.job_class_code) = '106' THEN 'From RN to Clerical'
          WHEN upper(jclf.job_class_code) = '103'
           AND upper(jclt.job_class_code) = '107' THEN 'From RN to Environmental'
          WHEN upper(jclf.job_class_code) = '103'
           AND upper(jclt.job_class_code) = '108' THEN 'From RN to Physician'
          WHEN upper(jclf.job_class_code) = '103'
           AND upper(jclt.job_class_code) = '109' THEN 'From RN to Non-Phys Med Prac'
          WHEN upper(jclf.job_class_code) = '103'
           AND upper(jclt.job_class_code) = '110' THEN 'From RN to Non-Clinical Spec/Prof'
          WHEN upper(jclf.job_class_code) = '103'
           AND upper(jclt.job_class_code) = '111' THEN 'From RN to Clinical Techs'
          WHEN (tm.analytics_msr_sid) = 80200 THEN 'Other Hire'
          WHEN (tm.analytics_msr_sid) = 80300 THEN 'Other Term'
          WHEN jclf.job_class_code = jclt.job_class_code
           AND staf.status_code = stato.status_code THEN 'XFER No Chng'
        END) = 'OTHER HIRE' THEN 'Other Hire'
        WHEN upper(CASE
          WHEN (tm.analytics_msr_sid) = 80200
           AND upper(CASE
            WHEN (tm.analytics_msr_sid) = 80200
             AND upper(dept_starn.dept_code) = '63110'
             AND upper(dept_starn.process_level_code) = '26624' THEN 'External Hire'
            WHEN upper(tm.action_reason_text) LIKE '%ACQ%' THEN 'Acquisition Hire'
            WHEN lb30.employee_num IS NULL
             AND upper(tm.action_code) NOT LIKE '%1XFER%' THEN 'External Hire'
            WHEN (tm.analytics_msr_sid) = 80200 THEN 'Internal Hire'
            WHEN (tm.analytics_msr_sid) = 80300
             AND upper(deptf.dept_code) = '63110'
             AND upper(deptf.process_level_code) = '26624' THEN 'External Term'
            WHEN (tm.analytics_msr_sid) = 80300
             AND upper(tm.action_code) = '1TERMPEND'
             AND tm.action_reason_text NOT IN(
              'TV-FCLTRNS', 'TV-RELOHCA', 'TV-PLCHG', 'TV-SPCLXFR', 'TI-OUTSRCE', 'TV-RESIDEN', 'TV-ENDASSG', 'TI-DIVEST', 'TV-NOSHOW', 'TV-NOSTART', 'TV-CHAP'
            ) THEN 'External Term'
            WHEN (tm.analytics_msr_sid) = 80300
             AND upper(tm.action_code) LIKE '%1XFER%' THEN 'Internal Term'
            WHEN (tm.analytics_msr_sid) = 80300
             AND upper(tm.action_code) = '1TERMPEND'
             AND tm.action_reason_text IN(
              'TV-FCLTRNS', 'TV-RELOHCA'
            ) THEN 'Internal Term'
            WHEN (tm.analytics_msr_sid) = 80300 THEN 'Other Term'
            ELSE 'XFER'
          END) = 'EXTERNAL HIRE' THEN 'Hire'
          WHEN (tm.analytics_msr_sid) = 80300
           AND upper(CASE
            WHEN (tm.analytics_msr_sid) = 80200
             AND upper(dept_starn.dept_code) = '63110'
             AND upper(dept_starn.process_level_code) = '26624' THEN 'External Hire'
            WHEN upper(tm.action_reason_text) LIKE '%ACQ%' THEN 'Acquisition Hire'
            WHEN lb30.employee_num IS NULL
             AND upper(tm.action_code) NOT LIKE '%1XFER%' THEN 'External Hire'
            WHEN (tm.analytics_msr_sid) = 80200 THEN 'Internal Hire'
            WHEN (tm.analytics_msr_sid) = 80300
             AND upper(deptf.dept_code) = '63110'
             AND upper(deptf.process_level_code) = '26624' THEN 'External Term'
            WHEN (tm.analytics_msr_sid) = 80300
             AND upper(tm.action_code) = '1TERMPEND'
             AND tm.action_reason_text NOT IN(
              'TV-FCLTRNS', 'TV-RELOHCA', 'TV-PLCHG', 'TV-SPCLXFR', 'TI-OUTSRCE', 'TV-RESIDEN', 'TV-ENDASSG', 'TI-DIVEST', 'TV-NOSHOW', 'TV-NOSTART', 'TV-CHAP'
            ) THEN 'External Term'
            WHEN (tm.analytics_msr_sid) = 80300
             AND upper(tm.action_code) LIKE '%1XFER%' THEN 'Internal Term'
            WHEN (tm.analytics_msr_sid) = 80300
             AND upper(tm.action_code) = '1TERMPEND'
             AND tm.action_reason_text IN(
              'TV-FCLTRNS', 'TV-RELOHCA'
            ) THEN 'Internal Term'
            WHEN (tm.analytics_msr_sid) = 80300 THEN 'Other Term'
            ELSE 'XFER'
          END) = 'EXTERNAL TERM' THEN 'Term'
          WHEN upper(jclf.job_class_code) = '103'
           AND upper(jclt.job_class_code) = '103'
           AND staf.status_code <> stato.status_code THEN 'Aux Status Change'
          WHEN upper(jclf.job_class_code) = '105'
           AND upper(jclt.job_class_code) = '103' THEN 'To RN from Tech'
          WHEN upper(jclf.job_class_code) = '101'
           AND upper(jclt.job_class_code) = '103' THEN 'To RN from Manager'
          WHEN upper(jclf.job_class_code) = '102'
           AND upper(jclt.job_class_code) = '103' THEN 'To RN from Clinical Spec/Prof'
          WHEN upper(jclf.job_class_code) = '104'
           AND upper(jclt.job_class_code) = '103' THEN 'To RN from LPN'
          WHEN upper(jclf.job_class_code) = '106'
           AND upper(jclt.job_class_code) = '103' THEN 'To RN from Clerical'
          WHEN upper(jclf.job_class_code) = '107'
           AND upper(jclt.job_class_code) = '103' THEN 'To RN from Environmental'
          WHEN upper(jclf.job_class_code) = '108'
           AND upper(jclt.job_class_code) = '103' THEN 'To RN from Physician'
          WHEN upper(jclf.job_class_code) = '109'
           AND upper(jclt.job_class_code) = '103' THEN 'To RN from Non-Phys Med Prac'
          WHEN upper(jclf.job_class_code) = '110'
           AND upper(jclt.job_class_code) = '103' THEN 'To RN from Non Clinical Spec/Prof'
          WHEN upper(jclf.job_class_code) = '111'
           AND upper(jclt.job_class_code) = '103' THEN 'To RN from Clinical Techs'
          WHEN upper(jclf.job_class_code) = '103'
           AND upper(jclt.job_class_code) = '101' THEN 'From RN to Manager'
          WHEN upper(jclf.job_class_code) = '103'
           AND upper(jclt.job_class_code) = '102' THEN 'From RN to Clinical Spec/Prof'
          WHEN upper(jclf.job_class_code) = '103'
           AND upper(jclt.job_class_code) = '104' THEN 'From RN to LPN'
          WHEN upper(jclf.job_class_code) = '103'
           AND upper(jclt.job_class_code) = '105' THEN 'From RN to Tech'
          WHEN upper(jclf.job_class_code) = '103'
           AND upper(jclt.job_class_code) = '106' THEN 'From RN to Clerical'
          WHEN upper(jclf.job_class_code) = '103'
           AND upper(jclt.job_class_code) = '107' THEN 'From RN to Environmental'
          WHEN upper(jclf.job_class_code) = '103'
           AND upper(jclt.job_class_code) = '108' THEN 'From RN to Physician'
          WHEN upper(jclf.job_class_code) = '103'
           AND upper(jclt.job_class_code) = '109' THEN 'From RN to Non-Phys Med Prac'
          WHEN upper(jclf.job_class_code) = '103'
           AND upper(jclt.job_class_code) = '110' THEN 'From RN to Non-Clinical Spec/Prof'
          WHEN upper(jclf.job_class_code) = '103'
           AND upper(jclt.job_class_code) = '111' THEN 'From RN to Clinical Techs'
          WHEN (tm.analytics_msr_sid) = 80200 THEN 'Other Hire'
          WHEN (tm.analytics_msr_sid) = 80300 THEN 'Other Term'
          WHEN jclf.job_class_code = jclt.job_class_code
           AND staf.status_code = stato.status_code THEN 'XFER No Chng'
        END) = 'AUX STATUS CHANGE' THEN 'Aux Chg'
        WHEN upper(CASE
          WHEN (tm.analytics_msr_sid) = 80200
           AND upper(CASE
            WHEN (tm.analytics_msr_sid) = 80200
             AND upper(dept_starn.dept_code) = '63110'
             AND upper(dept_starn.process_level_code) = '26624' THEN 'External Hire'
            WHEN upper(tm.action_reason_text) LIKE '%ACQ%' THEN 'Acquisition Hire'
            WHEN lb30.employee_num IS NULL
             AND upper(tm.action_code) NOT LIKE '%1XFER%' THEN 'External Hire'
            WHEN (tm.analytics_msr_sid) = 80200 THEN 'Internal Hire'
            WHEN (tm.analytics_msr_sid) = 80300
             AND upper(deptf.dept_code) = '63110'
             AND upper(deptf.process_level_code) = '26624' THEN 'External Term'
            WHEN (tm.analytics_msr_sid) = 80300
             AND upper(tm.action_code) = '1TERMPEND'
             AND tm.action_reason_text NOT IN(
              'TV-FCLTRNS', 'TV-RELOHCA', 'TV-PLCHG', 'TV-SPCLXFR', 'TI-OUTSRCE', 'TV-RESIDEN', 'TV-ENDASSG', 'TI-DIVEST', 'TV-NOSHOW', 'TV-NOSTART', 'TV-CHAP'
            ) THEN 'External Term'
            WHEN (tm.analytics_msr_sid) = 80300
             AND upper(tm.action_code) LIKE '%1XFER%' THEN 'Internal Term'
            WHEN (tm.analytics_msr_sid) = 80300
             AND upper(tm.action_code) = '1TERMPEND'
             AND tm.action_reason_text IN(
              'TV-FCLTRNS', 'TV-RELOHCA'
            ) THEN 'Internal Term'
            WHEN (tm.analytics_msr_sid) = 80300 THEN 'Other Term'
            ELSE 'XFER'
          END) = 'EXTERNAL HIRE' THEN 'Hire'
          WHEN (tm.analytics_msr_sid) = 80300
           AND upper(CASE
            WHEN (tm.analytics_msr_sid) = 80200
             AND upper(dept_starn.dept_code) = '63110'
             AND upper(dept_starn.process_level_code) = '26624' THEN 'External Hire'
            WHEN upper(tm.action_reason_text) LIKE '%ACQ%' THEN 'Acquisition Hire'
            WHEN lb30.employee_num IS NULL
             AND upper(tm.action_code) NOT LIKE '%1XFER%' THEN 'External Hire'
            WHEN (tm.analytics_msr_sid) = 80200 THEN 'Internal Hire'
            WHEN (tm.analytics_msr_sid) = 80300
             AND upper(deptf.dept_code) = '63110'
             AND upper(deptf.process_level_code) = '26624' THEN 'External Term'
            WHEN (tm.analytics_msr_sid) = 80300
             AND upper(tm.action_code) = '1TERMPEND'
             AND tm.action_reason_text NOT IN(
              'TV-FCLTRNS', 'TV-RELOHCA', 'TV-PLCHG', 'TV-SPCLXFR', 'TI-OUTSRCE', 'TV-RESIDEN', 'TV-ENDASSG', 'TI-DIVEST', 'TV-NOSHOW', 'TV-NOSTART', 'TV-CHAP'
            ) THEN 'External Term'
            WHEN (tm.analytics_msr_sid) = 80300
             AND upper(tm.action_code) LIKE '%1XFER%' THEN 'Internal Term'
            WHEN (tm.analytics_msr_sid) = 80300
             AND upper(tm.action_code) = '1TERMPEND'
             AND tm.action_reason_text IN(
              'TV-FCLTRNS', 'TV-RELOHCA'
            ) THEN 'Internal Term'
            WHEN (tm.analytics_msr_sid) = 80300 THEN 'Other Term'
            ELSE 'XFER'
          END) = 'EXTERNAL TERM' THEN 'Term'
          WHEN upper(jclf.job_class_code) = '103'
           AND upper(jclt.job_class_code) = '103'
           AND staf.status_code <> stato.status_code THEN 'Aux Status Change'
          WHEN upper(jclf.job_class_code) = '105'
           AND upper(jclt.job_class_code) = '103' THEN 'To RN from Tech'
          WHEN upper(jclf.job_class_code) = '101'
           AND upper(jclt.job_class_code) = '103' THEN 'To RN from Manager'
          WHEN upper(jclf.job_class_code) = '102'
           AND upper(jclt.job_class_code) = '103' THEN 'To RN from Clinical Spec/Prof'
          WHEN upper(jclf.job_class_code) = '104'
           AND upper(jclt.job_class_code) = '103' THEN 'To RN from LPN'
          WHEN upper(jclf.job_class_code) = '106'
           AND upper(jclt.job_class_code) = '103' THEN 'To RN from Clerical'
          WHEN upper(jclf.job_class_code) = '107'
           AND upper(jclt.job_class_code) = '103' THEN 'To RN from Environmental'
          WHEN upper(jclf.job_class_code) = '108'
           AND upper(jclt.job_class_code) = '103' THEN 'To RN from Physician'
          WHEN upper(jclf.job_class_code) = '109'
           AND upper(jclt.job_class_code) = '103' THEN 'To RN from Non-Phys Med Prac'
          WHEN upper(jclf.job_class_code) = '110'
           AND upper(jclt.job_class_code) = '103' THEN 'To RN from Non Clinical Spec/Prof'
          WHEN upper(jclf.job_class_code) = '111'
           AND upper(jclt.job_class_code) = '103' THEN 'To RN from Clinical Techs'
          WHEN upper(jclf.job_class_code) = '103'
           AND upper(jclt.job_class_code) = '101' THEN 'From RN to Manager'
          WHEN upper(jclf.job_class_code) = '103'
           AND upper(jclt.job_class_code) = '102' THEN 'From RN to Clinical Spec/Prof'
          WHEN upper(jclf.job_class_code) = '103'
           AND upper(jclt.job_class_code) = '104' THEN 'From RN to LPN'
          WHEN upper(jclf.job_class_code) = '103'
           AND upper(jclt.job_class_code) = '105' THEN 'From RN to Tech'
          WHEN upper(jclf.job_class_code) = '103'
           AND upper(jclt.job_class_code) = '106' THEN 'From RN to Clerical'
          WHEN upper(jclf.job_class_code) = '103'
           AND upper(jclt.job_class_code) = '107' THEN 'From RN to Environmental'
          WHEN upper(jclf.job_class_code) = '103'
           AND upper(jclt.job_class_code) = '108' THEN 'From RN to Physician'
          WHEN upper(jclf.job_class_code) = '103'
           AND upper(jclt.job_class_code) = '109' THEN 'From RN to Non-Phys Med Prac'
          WHEN upper(jclf.job_class_code) = '103'
           AND upper(jclt.job_class_code) = '110' THEN 'From RN to Non-Clinical Spec/Prof'
          WHEN upper(jclf.job_class_code) = '103'
           AND upper(jclt.job_class_code) = '111' THEN 'From RN to Clinical Techs'
          WHEN (tm.analytics_msr_sid) = 80200 THEN 'Other Hire'
          WHEN (tm.analytics_msr_sid) = 80300 THEN 'Other Term'
          WHEN jclf.job_class_code = jclt.job_class_code
           AND staf.status_code = stato.status_code THEN 'XFER No Chng'
        END) = 'XFER NO CHNG' THEN 'XFER No Chng'
        ELSE 'SkillMix Chg'
      END AS chg_group,
      compdt.week_number,
      concat(CAST(compdt.week_start_dt as STRING), ' to ', CAST(compdt.week_end_dt as STRING)) AS week_range,
      CASE
        WHEN upper(jclt.job_class_code) = '103'
         AND upper(stato.status_code) = 'FT' THEN 1
        WHEN upper(jclt.job_class_code) = '103'
         AND upper(stato.status_code) = 'PT' THEN 1
        WHEN upper(jclt.job_class_code) = '103'
         AND upper(stato.status_code) = 'PRN' THEN 1
        WHEN upper(jclt.job_class_code) = '103'
         AND upper(stato.status_code) = 'TEMP' THEN 1
        ELSE 0
      END AS msr_value,
      facto.lob_code,
      facto.sub_lob_code,
      1 AS comp_val_test,
      concat(trim(staf.status_code), ' to ', trim(stato.status_code)) AS aux_chg,
      current_date() AS date_stamp
    FROM
      {{ params.param_hr_base_views_dataset_name }}.fact_total_movement AS tm
      LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.job_class AS jclf ON tm.from_job_class_sid = jclf.job_class_sid
       AND date(jclf.valid_to_date) = '9999-12-31'
      LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.job_class AS jclt ON tm.to_job_class_sid = jclt.job_class_sid
       AND date(jclt.valid_to_date) = '9999-12-31'
      LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.status AS staf ON tm.from_auxiliary_status_sid = staf.status_sid
       AND upper(staf.status_type_code) = 'AUX'
       AND date(staf.valid_to_date) = '9999-12-31'
      LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.status AS stato ON tm.to_auxiliary_status_sid = stato.status_sid
       AND upper(stato.status_type_code) = 'AUX'
       AND date(stato.valid_to_date) = '9999-12-31'
      LEFT OUTER JOIN {{ params.param_pub_views_dataset_name }}.fact_facility AS facf ON tm.from_coid = facf.coid
       AND tm.from_company_code = facf.company_code
      LEFT OUTER JOIN {{ params.param_pub_views_dataset_name }}.fact_facility AS facto ON tm.to_coid = facto.coid
       AND tm.to_company_code = facto.company_code
      LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.department AS deptf ON tm.from_dept_sid = deptf.dept_sid
       AND DATE(deptf.valid_to_date) = '9999-12-31'
      LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.department AS deptt ON tm.to_dept_sid = deptt.dept_sid
       AND date(deptt.valid_to_date) = '9999-12-31'
      LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.process_level AS plf ON tm.from_process_level_code = plf.process_level_code
       AND tm.from_lawson_company_num = plf.lawson_company_num
       AND date(plf.valid_to_date) = '9999-12-31'
      LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.process_level AS plt ON tm.to_process_level_code = plt.process_level_code
       AND tm.to_lawson_company_num = plt.lawson_company_num
       AND date(plt.valid_to_date) = '9999-12-31'
      LEFT OUTER JOIN (
        SELECT
            fin.employee_sid,
            fin.position_sid,
            fin.employee_num,
            fin.position_level_sequence_num,
            max(fin.position_hire_date) AS position_hire_date
          FROM
            (
              SELECT
                  employee_position.employee_sid,
                  employee_position.position_sid,
                  employee_position.employee_num,
                  employee_position.position_level_sequence_num,
                  employee_position.eff_from_date AS position_hire_date
                FROM
                  {{ params.param_hr_base_views_dataset_name }}.employee_position
                WHERE date(employee_position.valid_to_date) = '9999-12-31'
                QUALIFY CASE
                  WHEN employee_position.position_sid <> lag(employee_position.position_sid, 1, 0) OVER (PARTITION BY employee_position.employee_sid ORDER BY position_hire_date) THEN 1
                  ELSE 0
                END = 1
            ) AS fin
          GROUP BY 1, 2, 3, 4
      ) AS dt_hr ON tm.employee_sid = dt_hr.employee_sid
       AND (tm.to_position_sid = dt_hr.position_sid
       AND (tm.analytics_msr_sid) <> 80300
       OR tm.from_position_sid = dt_hr.position_sid
       AND (tm.analytics_msr_sid) = 80300)
       AND (tm.to_position_level_sequence_num = dt_hr.position_level_sequence_num
       AND (tm.analytics_msr_sid) <> 80300
       OR tm.from_position_level_sequence_num = dt_hr.position_level_sequence_num
       AND (tm.analytics_msr_sid) = 80300)
      LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.job_position AS jobf ON tm.from_position_sid = jobf.position_sid
       AND tm.date_id BETWEEN jobf.eff_from_date AND jobf.eff_to_date
       AND date(jobf.valid_to_date) = '9999-12-31'
      LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.job_position AS jobt ON tm.to_position_sid = jobt.position_sid
       AND tm.date_id BETWEEN jobt.eff_from_date AND jobt.eff_to_date
       AND date(jobt.valid_to_date) = '9999-12-31'
      LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.employee AS emp ON tm.employee_sid = emp.employee_sid
       AND date(emp.valid_to_date) = '9999-12-31'
      LEFT OUTER JOIN {{ params.param_pub_views_dataset_name }}.lu_date AS dt ON tm.date_id = dt.date_id
      INNER JOIN (
        SELECT
            dt_0.week_end_date,
            date_diff(max(dt_0.date_id) , min(dt_0.date_id),day) AS dtdiff,
            min(dt_0.date_id) AS week_start_dt,
            max(dt_0.date_id) AS week_end_dt,
            row_number() OVER (PARTITION BY max(dt_0.date_id) - min(dt_0.date_id) ORDER BY min(dt_0.date_id)) AS week_number
          FROM
            {{ params.param_pub_views_dataset_name }}.lu_date AS dt_0
          WHERE dt_0.date_id >= (
            SELECT
                min(date_id)
              FROM
                {{ params.param_pub_views_dataset_name }}.lu_date
              WHERE date_id > date_sub(current_date(), interval 93 + extract(DAYOFWEEK from current_date()) DAY)
               AND date_id <= current_date()
               AND upper(dow_desc_l) = 'SUNDAY'
          )
           AND dt_0.date_id <= (
            SELECT
                max(date_id)
              FROM
                {{ params.param_pub_views_dataset_name }}.lu_date
              WHERE date_id > date_sub(current_date(), interval 93 + extract(DAYOFWEEK from current_date()) DAY)
               AND date_id <= current_date()
               AND upper(dow_desc_l) = 'SUNDAY'
          ) - 1
          GROUP BY 1
      ) AS compdt ON dt.week_end_date = compdt.week_end_date
       AND compdt.dtdiff = 6
      LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.hr_dept_rollup AS hrdrf ON tm.from_coid = hrdrf.coid
       AND tm.from_process_level_code = hrdrf.process_level_code
       AND tm.from_dept_sid = hrdrf.dept_sid
       AND tm.from_lawson_company_num = hrdrf.lawson_company_num
      LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.hr_dept_rollup AS hrdrt ON tm.to_coid = hrdrt.coid
       AND tm.to_process_level_code = hrdrt.process_level_code
       AND tm.to_dept_sid = hrdrt.dept_sid
       AND tm.to_lawson_company_num = hrdrt.lawson_company_num
      LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.fact_hr_metric AS lb30 ON tm.employee_num = lb30.employee_num
       AND DATE(tm.date_id - INTERVAL 30 DAY) = lb30.date_id
       AND (lb30.analytics_msr_sid) = 80100
      LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.department AS dept_starn ON lb30.dept_sid = dept_starn.dept_sid
       AND date(dept_starn.valid_to_date) = '9999-12-31'
    WHERE tm.date_id >= (
      SELECT
          min(date_id)
        FROM
          {{ params.param_pub_views_dataset_name }}.lu_date
        WHERE date_id > date_sub(current_date(), interval 93 + extract(DAYOFWEEK from current_date()) DAY)
         AND date_id <= current_date()
         AND upper(dow_desc_l) = 'SUNDAY'
    )
     AND tm.date_id <= (
      SELECT
          max(date_id)
        FROM
          {{ params.param_pub_views_dataset_name }}.lu_date
        WHERE date_id > date_sub(current_date(), interval 93 + extract(DAYOFWEEK from current_date()) DAY)
         AND date_id <= current_date()
         AND upper(dow_desc_l) = 'SUNDAY'
    )
     AND upper(facto.lob_code) = 'HOS'
     AND (upper(jclf.job_class_code) = '103'
     OR upper(jclt.job_class_code) = '103')
     AND tm.analytics_msr_sid IN(
      -- AND (facf.Sub_LOB_Code IN  ('MED', 'PSY') OR facto.Sub_LOB_Code IN  ('MED', 'PSY'))
      80700, 80200
    )
)
UNION DISTINCT
SELECT
    facf.group_name,
    facf.division_name,
    facf.unit_num,
    facf.coid_name,
    -- - User requested addition
    '' AS mov_dir,
    max(CASE
      WHEN upper(hc.process_level_code) = '03216' THEN '27300'
      WHEN upper(hc.process_level_code) = '03191' THEN '27400'
      WHEN upper(hc.process_level_code) = '03192' THEN '27450'
      WHEN upper(hc.process_level_code) = '03175' THEN '27401'
      WHEN upper(hc.process_level_code) = '03167' THEN '27100'
      WHEN upper(hc.process_level_code) = '03166' THEN '27200'
      WHEN upper(hc.process_level_code) = '03170' THEN '27490'
      WHEN upper(hc.process_level_code) = '08936' THEN '27150'
      ELSE hc.process_level_code
    END) AS process_level_code,
    hrdrt.cost_center,
    0 AS employee_num,
    '0' AS coid_cost_center,
    staf.status_code AS aux_status,
    jobf.position_code AS pos_code,
    jclf.job_class_code,
    CAST(NULL as date) AS position_hire_date,
    cast(NULL as date) AS termination_date,
    '' AS ext_int_type,
    'CURRENT RN HEADCOUNT' AS change_type,
    'HEADCOUNT' AS chg_group,
    0 AS week_number,
    ' ' AS week_range,
    count(DISTINCT hc.employee_num) AS msr_value,
    facf.lob_code,
    facf.sub_lob_code,
    0 AS comp_val_test,
    '' AS aux_chg,
    current_date() AS date_stamp
  FROM
    {{ params.param_hr_base_views_dataset_name }}.fact_hr_metric AS hc
    LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.job_class AS jclf ON hc.job_class_sid = jclf.job_class_sid
     AND date(jclf.valid_to_date) = '9999-12-31'
    LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.status AS staf ON hc.auxiliary_status_sid = staf.status_sid
     AND date(staf.valid_to_date) = '9999-12-31'
    LEFT OUTER JOIN {{ params.param_pub_views_dataset_name }}.fact_facility AS facf ON hc.coid = facf.coid
     AND hc.company_code = facf.company_code
    LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.department AS deptf ON hc.dept_sid = deptf.dept_sid
     AND date(deptf.valid_to_date) = '9999-12-31'
    LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.job_position AS jobf ON hc.position_sid = jobf.position_sid
     AND hc.date_id BETWEEN jobf.eff_from_date AND jobf.eff_to_date
     AND date(jobf.valid_to_date) = '9999-12-31'
    LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.job_position AS jobt ON hc.position_sid = jobt.position_sid
     AND hc.date_id BETWEEN jobt.eff_from_date AND jobt.eff_to_date
     AND date(jobt.valid_to_date) = '9999-12-31'
    LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.process_level AS plf ON hc.process_level_code = plf.process_level_code
     AND hc.lawson_company_num = plf.lawson_company_num
     AND date(plf.valid_to_date) = '9999-12-31'
    LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.hr_dept_rollup AS hrdrt ON hc.coid = hrdrt.coid
     AND hc.process_level_code = hrdrt.process_level_code
     AND hc.dept_sid = hrdrt.dept_sid
     AND hc.lawson_company_num = hrdrt.lawson_company_num
  WHERE hc.date_id = (
    SELECT
        max(date_id)
      FROM
        {{ params.param_pub_views_dataset_name }}.lu_date
      WHERE date_id > date_sub(current_date(), interval 93 + extract(DAYOFWEEK from current_date()) DAY)
       AND date_id <= current_date()
       AND upper(dow_desc_l) = 'SUNDAY'
  )
   AND upper(jclf.job_class_code) = '103'
   AND (analytics_msr_sid) = 80100
   AND upper(facf.lob_code) = 'HOS'
   AND staf.status_code IN(
    -- AND Sub_LOB_Code IN  ('MED', 'PSY')
    'FT', 'PT', 'PRN', 'Temp'
  )
  GROUP BY 1, 2, 3, 4, upper(CASE
    WHEN upper(hc.process_level_code) = '03216' THEN '27300'
    WHEN upper(hc.process_level_code) = '03191' THEN '27400'
    WHEN upper(hc.process_level_code) = '03192' THEN '27450'
    WHEN upper(hc.process_level_code) = '03175' THEN '27401'
    WHEN upper(hc.process_level_code) = '03167' THEN '27100'
    WHEN upper(hc.process_level_code) = '03166' THEN '27200'
    WHEN upper(hc.process_level_code) = '03170' THEN '27490'
    WHEN upper(hc.process_level_code) = '08936' THEN '27150'
    ELSE hc.process_level_code
  END), 7, 8, 9, 10, 11, 12, 13, 14, 18, 19, 21, 22, 18
;
-- AND hc.Process_Level_Code NOT IN ( '18345','26394','00637' )
