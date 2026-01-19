-- Translation time: 2023-12-01T12:21:28.647193Z
-- Translation job ID: 4144eabf-8284-495a-81e3-de22d755910b
-- Source: eim-clin-pdoc-ccda-dev-0001/ca_bulk_conversion_validation/20231201_0618/input/exp/j_cdm_adhoc_ca_patient_hosp_prm.sql
-- Translated from: Teradata
-- Translated to: BigQuery

SELECT
    count(*)
  FROM
    (
      SELECT DISTINCT
          cap.patient_sk,
          cah.hospital_sk,
          cas.server_sk,
          cahs.hospitalizationid AS source_patient_hosp_id,
          cahs.attendsurg AS attending_consultant_id,
          cahs.hicnumber AS hic_num,
          CAST(trim(concat(substr(CAST(cahs.admitdt as STRING), 1, 10), substr(CAST(cahs.admittime as STRING), 11, 9))) as DATETIME) AS admission_date_time,
          cahs.disloctn AS discharge_location_id,
          CAST(trim(substr(CAST(cahs.dbdischdt as STRING), 1, 10)) as DATE) AS db_discharge_date,
          cahs.mtdbdisstat AS db_discharge_mortality_status_id,
          cahs.readmit30 AS readmission_30_day_id,
          CAST(trim(substr(CAST(cahs.dischdt as STRING), 1, 10)) as DATE) AS discharge_date,
          CAST(trim(substr(CAST(cahs.readmitdt as STRING), 1, 10)) as DATE) AS readmission_date,
          cahs.mtdcstat AS discharge_mortality_status_id,
          cahs.readmitrsn AS readmission_reason_id,
          CAST(trim(substr(CAST(cahs.createdate as STRING), 1, 19)) as DATETIME) AS source_create_date_time,
          CAST(trim(substr(CAST(cahs.lastupdate as STRING), 1, 19)) as DATETIME) AS source_last_update_date_time,
          cahs.updateby AS updated_by_3_4_id,
          cahs.acctnum AS pat_acct_num_an,
          cahs.insprimtype AS primary_insurance_type_id,
          cahs.admitfromloc AS admission_location_id
        FROM
          `hca-hin-dev-cur-clinical`.edwcdm_staging.cardioaccess_hospitalization_stg AS cahs
          INNER JOIN `hca-hin-dev-cur-clinical`.edwcdm.ca_server AS cas ON upper(cahs.full_server_nm) = upper(cas.server_name)
          LEFT OUTER JOIN `hca-hin-dev-cur-clinical`.edwcdm.ca_patient AS cap ON cahs.patid = cap.source_patient_id
           AND cas.server_sk = cap.server_sk
          LEFT OUTER JOIN `hca-hin-dev-cur-clinical`.edwcdm.ca_hospital AS cah ON cahs.hospname = cah.source_hospital_id
           AND cas.server_sk = cah.server_sk
    ) AS a
;
