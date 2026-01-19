SELECT
databasename, tablename, wherecond, refreshappendcdc
FROM edw_pub_views.mirroring_status
WHERE databasename = 'edwpbs'
AND tablename = 'collection_encounter_detail'
Order By 1, 2;