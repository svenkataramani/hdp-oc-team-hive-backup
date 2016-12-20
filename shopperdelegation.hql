SET hive.support.concurrency=FALSE;
SET hive.cli.print.current.db=TRUE;
SET hive.exec.dynamic.partition = true;
SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.exec.max.dynamic.partitions=100000;
SET hive.exec.max.dynamic.partitions.pernode=100000;
SET hive.groupby.orderby.position.alias=TRUE;

use hdp_oc_team;

Insert Overwrite Table hdp_oc_team.shopperdelegation

Select
shopper_id, 
grantee_shopper_id, 
delegationlevelid,
isactive, 
createdate, 
modifydate 
from fortknoxdelegation.shopperdelegation_snap 
where regexp_extract(shopper_id,'[a-zA-Z]+', 0) = ''
;
