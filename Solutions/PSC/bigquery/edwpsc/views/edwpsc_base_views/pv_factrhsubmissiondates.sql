CREATE OR REPLACE VIEW edwpsc_base_views.`pv_factrhsubmissiondates`
AS SELECT
  `pv_factrhsubmissiondates`.rhsubmissiondatekey,
  `pv_factrhsubmissiondates`.claimkey,
  `pv_factrhsubmissiondates`.claimnumber,
  `pv_factrhsubmissiondates`.coid,
  `pv_factrhsubmissiondates`.regionkey,
  `pv_factrhsubmissiondates`.practice,
  `pv_factrhsubmissiondates`.claimsubmissiondatekey,
  `pv_factrhsubmissiondates`.transmissiontype,
  `pv_factrhsubmissiondates`.cpidkey,
  `pv_factrhsubmissiondates`.payerindicator,
  `pv_factrhsubmissiondates`.payername,
  `pv_factrhsubmissiondates`.importdatekey,
  `pv_factrhsubmissiondates`.sourceaprimarykeyvalue,
  `pv_factrhsubmissiondates`.sourcesystemcode,
  `pv_factrhsubmissiondates`.dwlastupdatedatetime,
  `pv_factrhsubmissiondates`.insertedby,
  `pv_factrhsubmissiondates`.inserteddtm,
  `pv_factrhsubmissiondates`.modifiedby,
  `pv_factrhsubmissiondates`.modifieddtm
  FROM
    edwpsc.`pv_factrhsubmissiondates`
;