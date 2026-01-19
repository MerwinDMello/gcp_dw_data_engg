CREATE OR REPLACE VIEW edwpsc_views.`3m_factuseractivity`
AS SELECT
  `3m_factuseractivity`.useractivitykey,
  `3m_factuseractivity`.cacaccountnumber,
  `3m_factuseractivity`.visitnumber,
  `3m_factuseractivity`.facilityid,
  `3m_factuseractivity`.activitydate,
  `3m_factuseractivity`.holdreason,
  `3m_factuseractivity`.codingstatus,
  `3m_factuseractivity`.username,
  `3m_factuseractivity`.userid,
  `3m_factuseractivity`.comment,
  `3m_factuseractivity`.worklistname,
  `3m_factuseractivity`.filename,
  `3m_factuseractivity`.filedate,
  `3m_factuseractivity`.insertedby,
  `3m_factuseractivity`.inserteddtm,
  `3m_factuseractivity`.modifiedby,
  `3m_factuseractivity`.modifieddtm,
  `3m_factuseractivity`.dwlastupdatedatetime
  FROM
    edwpsc_base_views.`3m_factuseractivity`
;