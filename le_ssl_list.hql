SET hive.support.concurrency=FALSE;
SET hive.cli.print.current.db=TRUE;
SET hive.exec.dynamic.partition = true;
SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.exec.max.dynamic.partitions=100000;
SET hive.exec.max.dynamic.partitions.pernode=100000;
SET hive.groupby.orderby.position.alias=TRUE;

use hdp_oc_team;

Insert Overwrite Table hdp_oc_team.le_domain_match
Select
subdomain,
issuer_cn,
ct_exp_date,
ct_issue_date,
commonname,
gd_issue_date,
gd_exp_date,
domainname,
domain_createdate,
domain_exp_date
From bisandbox.le_domain_match
;