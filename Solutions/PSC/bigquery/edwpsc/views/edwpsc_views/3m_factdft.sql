CREATE OR REPLACE VIEW edwpsc_views.`3m_factdft`
AS SELECT
  `3m_factdft`.dft3mkey,
  `3m_factdft`.batchdatekey,
  `3m_factdft`.batchdatetime,
  `3m_factdft`.visitnumber,
  `3m_factdft`.patientaccountnumber,
  `3m_factdft`.procedurecode,
  `3m_factdft`.proceduretype,
  `3m_factdft`.servicedate,
  `3m_factdft`.cptunit,
  `3m_factdft`.facilityid,
  `3m_factdft`.sendernote,
  `3m_factdft`.filename,
  `3m_factdft`.sourceaprimarykeyvalue,
  `3m_factdft`.insertedby,
  `3m_factdft`.inserteddtm,
  `3m_factdft`.modifiedby,
  `3m_factdft`.modifieddtm,
  `3m_factdft`.dwlastupdatedatetime
  FROM
    edwpsc_base_views.`3m_factdft`
;