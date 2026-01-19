CREATE OR REPLACE VIEW edwpsc_views.`ccu_rprtproductivity`
AS SELECT
  `ccu_rprtproductivity`.coder34id,
  `ccu_rprtproductivity`.touchdatekey,
  `ccu_rprtproductivity`.eomonthdate,
  `ccu_rprtproductivity`.sourcesystemcode,
  `ccu_rprtproductivity`.notinventoryclaims,
  `ccu_rprtproductivity`.notinventorycpts,
  `ccu_rprtproductivity`.inventoryclaims,
  `ccu_rprtproductivity`.inventorycpts,
  `ccu_rprtproductivity`.statuschanges,
  `ccu_rprtproductivity`.coidcount,
  `ccu_rprtproductivity`.dwlastupdatedatetime
  FROM
    edwpsc_base_views.`ccu_rprtproductivity`
;