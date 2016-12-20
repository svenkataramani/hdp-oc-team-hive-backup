SET hive.support.concurrency=FALSE;
SET hive.cli.print.current.db=TRUE;
SET hive.exec.dynamic.partition = true;
SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.exec.max.dynamic.partitions=100000;
SET hive.exec.max.dynamic.partitions.pernode=100000;
SET hive.groupby.orderby.position.alias=TRUE;

use hdp_oc_team;

Insert Overwrite Table hdp_oc_team.help_lp

SELECT DISTINCT 
shopper_id
FROM godaddywebsitetraffic.visitpagerequest_snap 
where visitpageid = 214016 
and 
To_Date(VisitDAte) > DATE_SUB(FROM_UNIXTIME(UNIX_TIMESTAMP()), 60);


