-- Translation time: 2023-12-01T12:21:28.647193Z
-- Translation job ID: 4144eabf-8284-495a-81e3-de22d755910b
-- Source: eim-clin-pdoc-ccda-dev-0001/ca_bulk_conversion_validation/20231201_0618/input/exp/j_cdm_adhoc_ca_case_transfusion_prm.sql
-- Translated from: Teradata
-- Translated to: BigQuery

SELECT count(*)
FROM
  (SELECT a.*
   FROM
     (SELECT NULL AS case_transfusion_sk,
             a_0.patient_case_sk AS patient_case_sk,
             trim(format('%11d', ca_server.server_sk)) AS server_sk,
             stg.transfusionid,
             stg.casenumber,
             stg.transfusion,
             stg.bldprodprbcdur,
             stg.bldprodffpdur,
             stg.bldprodfreshpdur,
             stg.bldprodsnglplatdur,
             stg.bldprodindplatdur,
             stg.bldprodcryodur,
             stg.bldprodfreshwbdur,
             stg.bldprodwbdur,
             stg.transfusbldprodlt24,
             stg.bldprodprbclt24,
             stg.bldprodffplt24,
             stg.bldprodfreshplt24,
             stg.bldprodsnglplatlt24,
             stg.bldprodindplatlt24,
             stg.bldprodcryolt24,
             stg.bldprodfreshwblt24,
             stg.bldprodwblt24,
             stg.transfusbldprodgt24,
             stg.bldprodprbcgt24,
             stg.bldprodffpgt24,
             stg.bldprodfreshpgt24,
             stg.bldprodsnglplatgt24,
             stg.bldprodindplatgt24,
             stg.bldprodcryogt24,
             stg.bldprodfreshwbgt24,
             stg.bldprodwbgt24,
             stg.dirdonorunits,
             stg.autologoustrans,
             stg.cellsavsal,
             stg.transfusbldprodany,
             stg.createdate,
             stg.lastupdate,
             trim(stg.updatedby) AS updatedby,
             stg.bldprodcryomlbef,
             stg.bldprodcryomlgt24,
             stg.bldprodcryomllt24,
             stg.bldprodffpmlbef,
             stg.bldprodffpmlgt24,
             stg.bldprodffpmllt24,
             stg.bldprodfreshpmlbef,
             stg.bldprodfreshpmlgt24,
             stg.bldprodfreshpmllt24,
             stg.bldprodfreshwbmlbef,
             stg.bldprodfreshwbmlgt24,
             stg.bldprodfreshwbmllt24,
             stg.bldprodplatmlbef,
             stg.bldprodplatmlgt24,
             stg.bldprodplatmllt24,
             stg.bldprodprbcmlbef,
             stg.bldprodprbcmlgt24,
             stg.bldprodprbcmllt24,
             stg.bldprodwbmlbef,
             stg.bldprodwbmlgt24,
             stg.bldprodwbmllt24,
             stg.transfusbldprodbefore,
             stg.cellsavsalml,
             trim(stg.server_name) AS SERVER_NAME,
             trim(stg.full_server_nm) AS full_server_nm,
             stg.dw_last_update_date_time
      FROM {{ params.param_clinical_cdm_stage_dataset_name }}.cardioaccess_transfusion_stg AS stg
      INNER JOIN {{ params.param_clinical_cdm_core_dataset_name }}.ca_server
      FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central') ON upper(trim(stg.full_server_nm)) = upper(trim(ca_server.server_name))
      LEFT OUTER JOIN
        (SELECT c.patient_case_sk,
                c.source_patient_case_num,
                s.server_name
         FROM {{ params.param_clinical_cdm_core_dataset_name }}.ca_patient_case AS c
         FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central')
         INNER JOIN {{ params.param_clinical_cdm_core_dataset_name }}.ca_server AS s
         FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central') ON c.server_sk = s.server_sk) AS a_0 ON stg.casenumber = a_0.source_patient_case_num
      AND upper(trim(stg.full_server_nm)) = upper(trim(a_0.server_name))) AS a) AS b