CREATE OR REPLACE VIEW edwpsc_base_views.`ccu_factpvinventoryproductivity`
AS SELECT
  `ccu_factpvinventoryproductivity`.ccupvinventoryproductivitykey,
  `ccu_factpvinventoryproductivity`.productivitydate,
  `ccu_factpvinventoryproductivity`.user34,
  `ccu_factpvinventoryproductivity`.regionkey,
  `ccu_factpvinventoryproductivity`.claimnumber,
  `ccu_factpvinventoryproductivity`.practice,
  `ccu_factpvinventoryproductivity`.claimstatuschangedto,
  `ccu_factpvinventoryproductivity`.actiontime,
  `ccu_factpvinventoryproductivity`.dwlastupdatedatetime,
  `ccu_factpvinventoryproductivity`.insertedby,
  `ccu_factpvinventoryproductivity`.inserteddtm,
  `ccu_factpvinventoryproductivity`.modifiedby,
  `ccu_factpvinventoryproductivity`.modifieddtm
  FROM
    edwpsc.`ccu_factpvinventoryproductivity`
;