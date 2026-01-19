-- Translation time: 2023-12-01T12:21:28.647193Z
-- Translation job ID: 4144eabf-8284-495a-81e3-de22d755910b
-- Source: eim-clin-pdoc-ccda-dev-0001/ca_bulk_conversion_validation/20231201_0618/input/exp/j_cdm_adhoc_ca_contact_detail_prm.sql
-- Translated from: Teradata
-- Translated to: BigQuery

SELECT count(*)
FROM
  (SELECT DISTINCT b.*
   FROM
     (SELECT cac.contact_sk AS contact_sk,
             29 AS contact_detail_measure_id,
             cacs.aparticid AS contact_detail_measure_value_text,
             CAST(NULL AS INT64) AS contact_detail_measure_value_num
      FROM `hca-hin-dev-cur-clinical`.edwcdm_staging.cardioaccess_contacts_stg AS cacs
      INNER JOIN `hca-hin-dev-cur-clinical`.edwcdm.ca_server AS cas ON upper(cas.server_name) = upper(cacs.full_server_nm)
      INNER JOIN `hca-hin-dev-cur-clinical`.edwcdm.ca_contact AS cac ON cas.server_sk = cac.server_sk
      AND cacs.contactid = cac.source_contact_id
      WHERE cacs.aparticid IS NOT NULL
      UNION DISTINCT SELECT cac.contact_sk AS contact_sk,
                            30 AS contact_detail_measure_id,
                            cacs.cparticid AS contact_detail_measure_value_text,
                            CAST(NULL AS INT64) AS contact_detail_measure_value_num
      FROM `hca-hin-dev-cur-clinical`.edwcdm_staging.cardioaccess_contacts_stg AS cacs
      INNER JOIN `hca-hin-dev-cur-clinical`.edwcdm.ca_server AS cas ON upper(cas.server_name) = upper(cacs.full_server_nm)
      INNER JOIN `hca-hin-dev-cur-clinical`.edwcdm.ca_contact AS cac ON cas.server_sk = cac.server_sk
      AND cacs.contactid = cac.source_contact_id
      WHERE cacs.cparticid IS NOT NULL
      UNION DISTINCT SELECT cac.contact_sk AS contact_sk,
                            31 AS contact_detail_measure_id,
                            contact_detail_measure_value_text AS contact_detail_measure_value_text,
                            CAST(NULL AS INT64) AS contact_detail_measure_value_num
      FROM `hca-hin-dev-cur-clinical`.edwcdm_staging.cardioaccess_contacts_stg AS cacs
      INNER JOIN `hca-hin-dev-cur-clinical`.edwcdm.ca_server AS cas ON upper(cas.server_name) = upper(cacs.full_server_nm)
      INNER JOIN `hca-hin-dev-cur-clinical`.edwcdm.ca_contact AS cac ON cas.server_sk = cac.server_sk
      AND cacs.contactid = cac.source_contact_id
      CROSS JOIN UNNEST(ARRAY[ substr(cacs.tparticid, 1, 5) ]) AS contact_detail_measure_value_text
      WHERE contact_detail_measure_value_text IS NOT NULL
      UNION DISTINCT SELECT cac.contact_sk AS contact_sk,
                            32 AS contact_detail_measure_id,
                            contact_detail_measure_value_text AS contact_detail_measure_value_text,
                            CAST(NULL AS INT64) AS contact_detail_measure_value_num
      FROM `hca-hin-dev-cur-clinical`.edwcdm_staging.cardioaccess_contacts_stg AS cacs
      INNER JOIN `hca-hin-dev-cur-clinical`.edwcdm.ca_server AS cas ON upper(cas.server_name) = upper(cacs.full_server_nm)
      INNER JOIN `hca-hin-dev-cur-clinical`.edwcdm.ca_contact AS cac ON cas.server_sk = cac.server_sk
      AND cacs.contactid = cac.source_contact_id
      CROSS JOIN UNNEST(ARRAY[ substr(cacs.mdnum, 1, 5) ]) AS contact_detail_measure_value_text
      WHERE contact_detail_measure_value_text IS NOT NULL
      UNION DISTINCT SELECT cac.contact_sk AS contact_sk,
                            33 AS contact_detail_measure_id,
                            contact_detail_measure_value_text AS contact_detail_measure_value_text,
                            CAST(NULL AS INT64) AS contact_detail_measure_value_num
      FROM `hca-hin-dev-cur-clinical`.edwcdm_staging.cardioaccess_contacts_stg AS cacs
      INNER JOIN `hca-hin-dev-cur-clinical`.edwcdm.ca_server AS cas ON upper(cas.server_name) = upper(cacs.full_server_nm)
      INNER JOIN `hca-hin-dev-cur-clinical`.edwcdm.ca_contact AS cac ON cas.server_sk = cac.server_sk
      AND cacs.contactid = cac.source_contact_id
      CROSS JOIN UNNEST(ARRAY[ substr(cacs.deanum, 1, 5) ]) AS contact_detail_measure_value_text
      WHERE contact_detail_measure_value_text IS NOT NULL
      UNION DISTINCT SELECT cac.contact_sk AS contact_sk,
                            34 AS contact_detail_measure_id,
                            contact_detail_measure_value_text AS contact_detail_measure_value_text,
                            CAST(NULL AS INT64) AS contact_detail_measure_value_num
      FROM `hca-hin-dev-cur-clinical`.edwcdm_staging.cardioaccess_contacts_stg AS cacs
      INNER JOIN `hca-hin-dev-cur-clinical`.edwcdm.ca_server AS cas ON upper(cas.server_name) = upper(cacs.full_server_nm)
      INNER JOIN `hca-hin-dev-cur-clinical`.edwcdm.ca_contact AS cac ON cas.server_sk = cac.server_sk
      AND cacs.contactid = cac.source_contact_id
      CROSS JOIN UNNEST(ARRAY[ substr(cacs.dear, 1, 5) ]) AS contact_detail_measure_value_text
      WHERE contact_detail_measure_value_text IS NOT NULL
      UNION DISTINCT SELECT cac.contact_sk AS contact_sk,
                            35 AS contact_detail_measure_id,
                            CAST(NULL AS STRING) AS contact_detail_measure_value_text,
                            cacs.dbcong AS contact_detail_measure_value_num
      FROM `hca-hin-dev-cur-clinical`.edwcdm_staging.cardioaccess_contacts_stg AS cacs
      INNER JOIN `hca-hin-dev-cur-clinical`.edwcdm.ca_server AS cas ON upper(cas.server_name) = upper(cacs.full_server_nm)
      INNER JOIN `hca-hin-dev-cur-clinical`.edwcdm.ca_contact AS cac ON cas.server_sk = cac.server_sk
      AND cacs.contactid = cac.source_contact_id
      WHERE cacs.dbcong IS NOT NULL
      UNION DISTINCT SELECT cac.contact_sk AS contact_sk,
                            36 AS contact_detail_measure_id,
                            CAST(NULL AS STRING) AS contact_detail_measure_value_text,
                            cacs.dbadlt AS contact_detail_measure_value_num
      FROM `hca-hin-dev-cur-clinical`.edwcdm_staging.cardioaccess_contacts_stg AS cacs
      INNER JOIN `hca-hin-dev-cur-clinical`.edwcdm.ca_server AS cas ON upper(cas.server_name) = upper(cacs.full_server_nm)
      INNER JOIN `hca-hin-dev-cur-clinical`.edwcdm.ca_contact AS cac ON cas.server_sk = cac.server_sk
      AND cacs.contactid = cac.source_contact_id
      WHERE cacs.dbadlt IS NOT NULL
      UNION DISTINCT SELECT cac.contact_sk AS contact_sk,
                            37 AS contact_detail_measure_id,
                            CAST(NULL AS STRING) AS contact_detail_measure_value_text,
                            cacs.dbthor AS contact_detail_measure_value_num
      FROM `hca-hin-dev-cur-clinical`.edwcdm_staging.cardioaccess_contacts_stg AS cacs
      INNER JOIN `hca-hin-dev-cur-clinical`.edwcdm.ca_server AS cas ON upper(cas.server_name) = upper(cacs.full_server_nm)
      INNER JOIN `hca-hin-dev-cur-clinical`.edwcdm.ca_contact AS cac ON cas.server_sk = cac.server_sk
      AND cacs.contactid = cac.source_contact_id
      WHERE cacs.dbthor IS NOT NULL
      UNION DISTINCT SELECT cac.contact_sk AS contact_sk,
                            38 AS contact_detail_measure_id,
                            CAST(NULL AS STRING) AS contact_detail_measure_value_text,
                            cacs.presphy AS contact_detail_measure_value_num
      FROM `hca-hin-dev-cur-clinical`.edwcdm_staging.cardioaccess_contacts_stg AS cacs
      INNER JOIN `hca-hin-dev-cur-clinical`.edwcdm.ca_server AS cas ON upper(cas.server_name) = upper(cacs.full_server_nm)
      INNER JOIN `hca-hin-dev-cur-clinical`.edwcdm.ca_contact AS cac ON cas.server_sk = cac.server_sk
      AND cacs.contactid = cac.source_contact_id
      WHERE cacs.presphy IS NOT NULL
      UNION DISTINCT SELECT cac.contact_sk AS contact_sk,
                            39 AS contact_detail_measure_id,
                            contact_detail_measure_value_text AS contact_detail_measure_value_text,
                            CAST(NULL AS INT64) AS contact_detail_measure_value_num
      FROM `hca-hin-dev-cur-clinical`.edwcdm_staging.cardioaccess_contacts_stg AS cacs
      INNER JOIN `hca-hin-dev-cur-clinical`.edwcdm.ca_server AS cas ON upper(cas.server_name) = upper(cacs.full_server_nm)
      INNER JOIN `hca-hin-dev-cur-clinical`.edwcdm.ca_contact AS cac ON cas.server_sk = cac.server_sk
      AND cacs.contactid = cac.source_contact_id
      CROSS JOIN UNNEST(ARRAY[ substr(cacs.rxpwd, 1, 5) ]) AS contact_detail_measure_value_text
      WHERE contact_detail_measure_value_text IS NOT NULL
      UNION DISTINCT SELECT cac.contact_sk AS contact_sk,
                            40 AS contact_detail_measure_id,
                            contact_detail_measure_value_text AS contact_detail_measure_value_text,
                            CAST(NULL AS INT64) AS contact_detail_measure_value_num
      FROM `hca-hin-dev-cur-clinical`.edwcdm_staging.cardioaccess_contacts_stg AS cacs
      INNER JOIN `hca-hin-dev-cur-clinical`.edwcdm.ca_server AS cas ON upper(cas.server_name) = upper(cacs.full_server_nm)
      INNER JOIN `hca-hin-dev-cur-clinical`.edwcdm.ca_contact AS cac ON cas.server_sk = cac.server_sk
      AND cacs.contactid = cac.source_contact_id
      CROSS JOIN UNNEST(ARRAY[ substr(cacs.surgid, 1, 5) ]) AS contact_detail_measure_value_text
      WHERE contact_detail_measure_value_text IS NOT NULL
      UNION DISTINCT SELECT cac.contact_sk AS contact_sk,
                            41 AS contact_detail_measure_id,
                            contact_detail_measure_value_text AS contact_detail_measure_value_text,
                            CAST(NULL AS INT64) AS contact_detail_measure_value_num
      FROM `hca-hin-dev-cur-clinical`.edwcdm_staging.cardioaccess_contacts_stg AS cacs
      INNER JOIN `hca-hin-dev-cur-clinical`.edwcdm.ca_server AS cas ON upper(cas.server_name) = upper(cacs.full_server_nm)
      INNER JOIN `hca-hin-dev-cur-clinical`.edwcdm.ca_contact AS cac ON cas.server_sk = cac.server_sk
      AND cacs.contactid = cac.source_contact_id
      CROSS JOIN UNNEST(ARRAY[ substr(cacs.surgnpi, 1, 5) ]) AS contact_detail_measure_value_text
      WHERE contact_detail_measure_value_text IS NOT NULL
      UNION DISTINCT SELECT cac.contact_sk AS contact_sk,
                            42 AS contact_detail_measure_id,
                            contact_detail_measure_value_text AS contact_detail_measure_value_text,
                            CAST(NULL AS INT64) AS contact_detail_measure_value_num
      FROM `hca-hin-dev-cur-clinical`.edwcdm_staging.cardioaccess_contacts_stg AS cacs
      INNER JOIN `hca-hin-dev-cur-clinical`.edwcdm.ca_server AS cas ON upper(cas.server_name) = upper(cacs.full_server_nm)
      INNER JOIN `hca-hin-dev-cur-clinical`.edwcdm.ca_contact AS cac ON cas.server_sk = cac.server_sk
      AND cacs.contactid = cac.source_contact_id
      CROSS JOIN UNNEST(ARRAY[ substr(cacs.tin, 1, 5) ]) AS contact_detail_measure_value_text
      WHERE contact_detail_measure_value_text IS NOT NULL
      UNION DISTINCT SELECT cac.contact_sk AS contact_sk,
                            43 AS contact_detail_measure_id,
                            contact_detail_measure_value_text AS contact_detail_measure_value_text,
                            CAST(NULL AS INT64) AS contact_detail_measure_value_num
      FROM `hca-hin-dev-cur-clinical`.edwcdm_staging.cardioaccess_contacts_stg AS cacs
      INNER JOIN `hca-hin-dev-cur-clinical`.edwcdm.ca_server AS cas ON upper(cas.server_name) = upper(cacs.full_server_nm)
      INNER JOIN `hca-hin-dev-cur-clinical`.edwcdm.ca_contact AS cac ON cas.server_sk = cac.server_sk
      AND cacs.contactid = cac.source_contact_id
      CROSS JOIN UNNEST(ARRAY[ substr(cacs.upin, 1, 5) ]) AS contact_detail_measure_value_text
      WHERE contact_detail_measure_value_text IS NOT NULL
      UNION DISTINCT SELECT cac.contact_sk AS contact_sk,
                            44 AS contact_detail_measure_id,
                            contact_detail_measure_value_text AS contact_detail_measure_value_text,
                            CAST(NULL AS INT64) AS contact_detail_measure_value_num
      FROM `hca-hin-dev-cur-clinical`.edwcdm_staging.cardioaccess_contacts_stg AS cacs
      INNER JOIN `hca-hin-dev-cur-clinical`.edwcdm.ca_server AS cas ON upper(cas.server_name) = upper(cacs.full_server_nm)
      INNER JOIN `hca-hin-dev-cur-clinical`.edwcdm.ca_contact AS cac ON cas.server_sk = cac.server_sk
      AND cacs.contactid = cac.source_contact_id
      CROSS JOIN UNNEST(ARRAY[ substr(cacs.contactidft, 1, 5) ]) AS contact_detail_measure_value_text
      WHERE contact_detail_measure_value_text IS NOT NULL
      UNION DISTINCT SELECT cac.contact_sk AS contact_sk,
                            45 AS contact_detail_measure_id,
                            contact_detail_measure_value_text AS contact_detail_measure_value_text,
                            CAST(NULL AS INT64) AS contact_detail_measure_value_num
      FROM `hca-hin-dev-cur-clinical`.edwcdm_staging.cardioaccess_contacts_stg AS cacs
      INNER JOIN `hca-hin-dev-cur-clinical`.edwcdm.ca_server AS cas ON upper(cas.server_name) = upper(cacs.full_server_nm)
      INNER JOIN `hca-hin-dev-cur-clinical`.edwcdm.ca_contact AS cac ON cas.server_sk = cac.server_sk
      AND cacs.contactid = cac.source_contact_id
      CROSS JOIN UNNEST(ARRAY[ substr(cacs.eclscenterid, 1, 5) ]) AS contact_detail_measure_value_text
      WHERE contact_detail_measure_value_text IS NOT NULL
      UNION DISTINCT SELECT cac.contact_sk AS contact_sk,
                            46 AS contact_detail_measure_id,
                            CAST(NULL AS STRING) AS contact_detail_measure_value_text,
                            cacs.providerid AS contact_detail_measure_value_num
      FROM `hca-hin-dev-cur-clinical`.edwcdm_staging.cardioaccess_contacts_stg AS cacs
      INNER JOIN `hca-hin-dev-cur-clinical`.edwcdm.ca_server AS cas ON upper(cas.server_name) = upper(cacs.full_server_nm)
      INNER JOIN `hca-hin-dev-cur-clinical`.edwcdm.ca_contact AS cac ON cas.server_sk = cac.server_sk
      AND cacs.contactid = cac.source_contact_id
      WHERE cacs.providerid IS NOT NULL
      UNION DISTINCT SELECT cac.contact_sk AS contact_sk,
                            47 AS contact_detail_measure_id,
                            CAST(NULL AS STRING) AS contact_detail_measure_value_text,
                            cacs.accredidation AS contact_detail_measure_value_num
      FROM `hca-hin-dev-cur-clinical`.edwcdm_staging.cardioaccess_contacts_stg AS cacs
      INNER JOIN `hca-hin-dev-cur-clinical`.edwcdm.ca_server AS cas ON upper(cas.server_name) = upper(cacs.full_server_nm)
      INNER JOIN `hca-hin-dev-cur-clinical`.edwcdm.ca_contact AS cac ON cas.server_sk = cac.server_sk
      AND cacs.contactid = cac.source_contact_id
      WHERE cacs.accredidation IS NOT NULL ) AS b
   WHERE upper(trim(b.contact_detail_measure_value_text)) <> ''
     OR b.contact_detail_measure_value_num IS NOT NULL ) AS c