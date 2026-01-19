-- Translation time: 2023-12-01T12:21:28.647193Z
-- Translation job ID: 4144eabf-8284-495a-81e3-de22d755910b
-- Source: eim-clin-pdoc-ccda-dev-0001/ca_bulk_conversion_validation/20231201_0618/input/exp/j_cdm_adhoc_ca_proc_list_prm.sql
-- Translated from: Teradata
-- Translated to: BigQuery

SELECT count(*)
FROM
  (SELECT p.*
   FROM
     (SELECT NULL AS proc_list_sk,
             trim(format('%11d', dc.proc_category_id)) AS proc_category_id,
             trim(format('%11d', s.server_sk)) AS server_sk,
             trim(format('%11d', stg.id)) AS id,
             trim(stg.wrkgrpcode) AS wrkgrpcode,
             trim(format('%11d', stg.kingdomcode)) AS kingdomcode,
             trim(stg.kingdom) AS kingdom,
             trim(stg.phylum) AS phylum,
             trim(stg.procedurename) AS procedurename,
             trim(stg.dbtype) AS dbtype,
             trim(format('%11d', stg.stsdup250)) AS stsdup250,
             trim(regexp_replace(format('%#5.1f', stg.stsdup30), r'^(.*?)(-)?0(\..*)', r'\1 \2\3')) AS stsdup30,
             trim(stg.ststerm250) AS ststerm250,
             trim(stg.stsid250) AS stsid250,
             trim(stg.ststerm30) AS ststerm30,
             trim(stg.stsid30) AS stsid30,
             trim(stg.ipccc) AS ipccc,
             trim(stg.cptcode) AS cptcode,
             trim(stg.icd9code) AS icd9code,
             trim(stg.rachs1) AS rachs1,
             trim(stg.price) AS price,
             trim(format('%11d', stg.inactive)) AS inactive,
             trim(stg.pxmcategory) AS pxmcategory,
             trim(stg.pxscategory) AS pxscategory,
             trim(format('%11d', stg.pxvalve)) AS pxvalve,
             trim(format('%11d', stg.pxcabg)) AS pxcabg,
             trim(format('%11d', stg.pxmechsupp)) AS pxmechsupp,
             trim(format('%11d', stg.pxtx)) AS pxtx,
             trim(format('%11d', stg.pxhcsp)) AS pxhcsp,
             trim(format('%11d', stg.pxpacemaker)) AS pxpacemaker,
             trim(format('%11d', stg.pxvad)) AS pxvad,
             trim(format('%11d', stg.accproc)) AS accproc,
             trim(format('%11d', stg.pxnos)) AS pxnos,
             trim(format('%11d', stg.pxmodifier)) AS pxmodifier,
             trim(format('%11d', stg.aristlebasiclvl)) AS aristlebasiclvl,
             trim(regexp_replace(format('%#5.1f', stg.aristlebasicscore), r'^(.*?)(-)?0(\..*)', r'\1 \2\3')) AS aristlebasicscore,
             trim(stg.ststerm30_sp) AS ststerm30_sp,
             trim(stg.stsid30_sp) AS stsid30_sp,
             trim(stg.abtscode) AS abtscode,
             trim(regexp_replace(format('%#5.1f', stg.statscore), r'^(.*?)(-)?0(\..*)', r'\1 \2\3')) AS statscore,
             trim(format('%11d', stg.statcategory)) AS statcategory,
             trim(format('%11d', stg.stsrankorder30)) AS stsrankorder30,
             trim(regexp_replace(format('%#5.1f', stg.stsdup32), r'^(.*?)(-)?0(\..*)', r'\1 \2\3')) AS stsdup32,
             trim(stg.ststerm32) AS ststerm32,
             trim(stg.stsid32) AS stsid32,
             trim(format('%11d', stg.stsbaseterm30)) AS stsbaseterm30,
             trim(stg.ststerm32_sp) AS ststerm32_sp,
             trim(stg.stsid32_sp) AS stsid32_sp,
             trim(format('%11d', stg.stsbaseterm32)) AS stsbaseterm32,
             trim(stg.ststerm33) AS ststerm33,
             trim(stg.stsid33) AS stsid33,
             trim(regexp_replace(format('%#5.1f', stg.stsdup33), r'^(.*?)(-)?0(\..*)', r'\1 \2\3')) AS stsdup33,
             trim(stg.ststerm33_sp) AS ststerm33_sp,
             trim(stg.stsid33_sp) AS stsid33_sp,
             trim(format('%11d', stg.stsbaseterm33)) AS stsbaseterm33,
             trim(stg.server_name) AS SERVER_NAME,
             trim(stg.full_server_nm) AS full_server_nm,
             stg.dw_last_update_date_time AS dw_last_update_date_time
      FROM `hca-hin-dev-cur-clinical`.edwcdm_staging.cardioaccess_procedurelist_stg AS stg
      LEFT OUTER JOIN `hca-hin-dev-cur-clinical`.edwcdm.ref_ca_proc_category AS dc ON upper(trim(stg.pxmcategory)) = upper(trim(dc.proc_category_name))
      AND upper(trim(stg.pxscategory)) = upper(trim(dc.proc_sub_category_name))
      INNER JOIN `hca-hin-dev-cur-clinical`.edwcdm.ca_server AS s ON upper(trim(stg.full_server_nm)) = upper(trim(s.server_name))) AS p) AS q