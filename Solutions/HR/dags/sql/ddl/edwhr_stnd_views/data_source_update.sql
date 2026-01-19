-- Translation time: 2023-04-25T18:33:50.275152Z
-- Translation job ID: 8b921d45-66b0-461e-880c-7106e48c9005
-- Source: eim-cs-da-gmek-5764-dev/HRG/Bteq_Source_Files/SARANYADEVI/missing_views/EDWHR_STND_VIEWS/data_source_update.sql
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW {{ params.param_hr_stnd_views_dataset_name }}.data_source_update AS 
  SELECT
      max(CASE
        WHEN upper(tgt_tbl_nm) = 'EDWHR.EMPLOYEE_POSITION' THEN 'Lawson'
        WHEN upper(tgt_tbl_nm) = 'EDWHR.TIME_ENTRY' THEN 'Kronos'
        WHEN upper(tgt_tbl_nm) = 'EDWHR.OFFER_STATUS' THEN 'Taleo'
        WHEN upper(tgt_tbl_nm) = 'EDWHR.FACT_HR_METRIC' THEN 'Fact Tables'
        WHEN upper(tgt_tbl_nm) = 'EDWHR.FACT_TOTAL_MOVEMENT' THEN 'Total Movement'
        WHEN upper(tgt_tbl_nm) = 'EDWHR.EMPLOYEE_ROSTER' THEN 'Employee Roster'
        WHEN upper(tgt_tbl_nm) = 'EDWHR.FACT_HR_METRIC_SNAPSHOT' THEN 'Fact Tables Snapshot'
      END) AS data_source_name,
      max(load_end_time) AS last_update_date_time
    FROM
      {{ params.param_hr_audit_dataset_name }}.audit_control
    WHERE upper(tgt_tbl_nm) IN(
      'EDWHR.EMPLOYEE_POSITION','EDWHR.TIME_ENTRY', 'EDWHR.OFFER_STATUS', 'EDWHR.FACT_HR_METRIC', 
      'EDWHR.FACT_TOTAL_MOVEMENT', 'EDWHR.EMPLOYEE_ROSTER', 'EDWHR.FACT_HR_METRIC_SNAPSHOT'

    )
    GROUP BY upper(CASE
        WHEN upper(tgt_tbl_nm) = 'EDWHR.EMPLOYEE_POSITION' THEN 'Lawson'
        WHEN upper(tgt_tbl_nm) = 'EDWHR.TIME_ENTRY' THEN 'Kronos'
        WHEN upper(tgt_tbl_nm) = 'EDWHR.OFFER_STATUS' THEN 'Taleo'
        WHEN upper(tgt_tbl_nm) = 'EDWHR.FACT_HR_METRIC' THEN 'Fact Tables'
        WHEN upper(tgt_tbl_nm) = 'EDWHR.FACT_TOTAL_MOVEMENT' THEN 'Total Movement'
        WHEN upper(tgt_tbl_nm) = 'EDWHR.EMPLOYEE_ROSTER' THEN 'Employee Roster'
        WHEN upper(tgt_tbl_nm) = 'EDWHR.FACT_HR_METRIC_SNAPSHOT' THEN 'Fact Tables Snapshot'
    END)
  ;

