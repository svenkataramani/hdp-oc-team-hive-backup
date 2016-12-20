SET hive.support.concurrency=FALSE;
SET hive.cli.print.current.db=TRUE;
SET hive.exec.dynamic.partition = true;
SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.exec.max.dynamic.partitions=100000;
SET hive.exec.max.dynamic.partitions.pernode=100000;
SET hive.groupby.orderby.position.alias=TRUE;

use hdp_oc_team;

-- CREATE TABLE hdp_oc_team.SupportContactInfo stored as textfile as
-- SELECT DISTINCT catalog_marketID, privateLabelResellerTypeID, d.gdshop_supportTypeID, st1.supportTypeDescription, d.contactInfo
-- FROM 
-- goDaddy.gdshop_supportMarket_mtm_supportType_snap d
-- INNER JOIN 
-- goDaddy.gdshop_supportType_snap st1 ON d.gdshop_supportTypeID = st1.gdshop_supportTypeID
-- WHERE 1= 1
-- AND st1.supportTypeDescription LIKE 'tes.%';

insert overwrite table hdp_oc_team.SupportContactInfo
SELECT DISTINCT catalog_marketID, privateLabelResellerTypeID, d.gdshop_supportTypeID, st1.supportTypeDescription, d.contactInfo
FROM 
goDaddy.gdshop_supportMarket_mtm_supportType_snap d
INNER JOIN 
goDaddy.gdshop_supportType_snap st1 ON d.gdshop_supportTypeID = st1.gdshop_supportTypeID
WHERE 1 = 1
AND st1.supportTypeDescription LIKE 'tes.%';