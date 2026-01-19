CREATE OR REPLACE VIEW edwpsc_base_views.`pk_factchangedata`
AS SELECT
  `pk_factchangedata`.pkchangedatakey,
  `pk_factchangedata`.createdtime,
  `pk_factchangedata`.modifiedtime,
  `pk_factchangedata`.pktransactiondetailskey,
  `pk_factchangedata`.chargeid,
  `pk_factchangedata`.fieldid,
  `pk_factchangedata`.fieldcategory,
  `pk_factchangedata`.fieldvaluetext,
  `pk_factchangedata`.fieldvalueid,
  `pk_factchangedata`.syncversion,
  `pk_factchangedata`.sourceaprimarykeyvalue,
  `pk_factchangedata`.sourcebprimarykeyvalue,
  `pk_factchangedata`.pkregionname,
  `pk_factchangedata`.insertedby,
  `pk_factchangedata`.inserteddtm,
  `pk_factchangedata`.modifiedby,
  `pk_factchangedata`.modifieddtm,
  `pk_factchangedata`.dwlastupdatedatetime
  FROM
    edwpsc.`pk_factchangedata`
;