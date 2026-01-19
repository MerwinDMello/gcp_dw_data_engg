-- Translation time: 2023-12-01T12:21:28.647193Z
-- Translation job ID: 4144eabf-8284-495a-81e3-de22d755910b
-- Source: eim-clin-pdoc-ccda-dev-0001/ca_bulk_conversion_validation/20231201_0618/input/exp/j_cdm_adhoc_ca_case_proc_prm.sql
-- Translated from: Teradata
-- Translated to: BigQuery

SELECT
    count(*)
  FROM
    (
      SELECT
          p.*
        FROM
          (
            SELECT
                1 AS case_proc_sk,
                trim(format('%11d', a.patient_case_sk)) AS patient_case_sk,
                trim(format('%11d', s.server_sk)) AS server_sk,
                trim(format('%11d', stg.procid)) AS proc_list_sk,
                trim(format('%11d', stg.procedureid)) AS procedureid,
                trim(format('%11d', stg.hospitalid)) AS hospitalid,
                trim(format('%11d', stg.casenumber)) AS casenumber,
                trim(stg.procedurename) AS procedurename,
                trim(stg.cptcode) AS cptcode,
                trim(stg.price) AS price,
                trim(format('%11d', stg.procid)) AS procid,
                trim(stg.modifier) AS modifier,
                trim(stg.proccateg) AS proccateg,
                trim(stg.procshrtlst) AS procshrtlst,
                trim(stg.icd_9code) AS icd_9code,
                trim(stg.icd_10code) AS icd_10code,
                trim(stg.code1) AS code1,
                trim(stg.code2) AS code2,
                trim(stg.code3) AS code3,
                trim(stg.code4) AS code4,
                trim(stg.code5) AS code5,
                trim(stg.code6) AS code6,
                trim(stg.code7) AS code7,
                trim(stg.code8) AS code8,
                trim(stg.code9) AS code9,
                trim(stg.code10) AS code10,
                trim(stg.code11) AS code11,
                trim(stg.code12) AS code12,
                trim(format('%11d', stg.primproc)) AS primproc,
                trim(format('%11d', stg.sort)) AS sort,
                trim(stg.other) AS other,
                trim(stg.recur) AS recur,
                trim(stg.aristotlescore) AS aristotlescore,
                trim(format('%11d', stg.rachsscore)) AS rachsscore,
                stg.createdate AS createdate,
                stg.updatedate AS updatedate,
                stg.updateby AS updateby,
                trim(format('%11d', stg.psf)) AS psf,
                trim(stg.server_name) AS server_name,
                trim(stg.full_server_nm) AS full_server_nm,
                stg.dw_last_update_date_time
              FROM
                `hca-hin-dev-cur-clinical`.edwcdm_staging.cardioaccess_procedures_stg AS stg
                LEFT OUTER JOIN (
                  SELECT
                      c.patient_case_sk,
                      c.source_patient_case_num,
                      s_0.server_name
                    FROM
                      `hca-hin-dev-cur-clinical`.edwcdm.ca_patient_case AS c
                      INNER JOIN `hca-hin-dev-cur-clinical`.edwcdm.ca_server AS s_0 ON trim(format('%11d', c.server_sk)) = trim(format('%11d', s_0.server_sk))
                ) AS a ON upper(trim(stg.full_server_nm)) = upper(trim(a.server_name))
                 AND trim(format('%11d', stg.casenumber)) = trim(format('%11d', a.source_patient_case_num))
                INNER JOIN `hca-hin-dev-cur-clinical`.edwcdm.ca_server AS s ON upper(trim(stg.full_server_nm)) = upper(trim(s.server_name))
                LEFT OUTER JOIN (
                  SELECT
                      c.source_proc_list_id,
                      p_0.server_name
                    FROM
                      `hca-hin-dev-cur-clinical`.edwcdm.ca_proc_list AS c
                      INNER JOIN `hca-hin-dev-cur-clinical`.edwcdm.ca_server AS p_0 ON trim(format('%11d', c.server_sk)) = trim(format('%11d', p_0.server_sk))
                ) AS b ON trim(format('%11d', stg.procid)) = trim(format('%11d', b.source_proc_list_id))
                 AND upper(trim(stg.full_server_nm)) = upper(trim(b.server_name))
          ) AS p
    ) AS q
;
