SET hive.support.concurrency=FALSE;
SET hive.cli.print.current.db=TRUE;
SET hive.exec.dynamic.partition = true;
SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.exec.max.dynamic.partitions=100000;
SET hive.exec.max.dynamic.partitions.pernode=100000;
SET hive.groupby.orderby.position.alias=TRUE;


use hdp_oc_team;

-- create table hdp_oc_team.ShopDomainRegNaics stored as textfile as
-- select distinct shopper_id, domainid, naics3d as naics, score3d as conf_score, case when naics3d in(621,622,623,624) then 'Regulated - Medical' else 'Regulated - Non-Medical' end as vertical
-- from bkamble.tmp_naics_classifier_3digit_NoKeyWords
-- where naics3d in(111,112,113,114,115,211,212,213,221,236,237,238,311,312,313,314,315,316,321,322,323,324,325,326,327,331,332,333,334,335,336,337,339,423,424,425,441,442,443,444,445,446,447,448,451,452,453,454,481,482,483,484,485,486,487,488,491,492,493,511,512,515,517,518,519,521,522,523,524,525,531,532,533,541,551,561,562,611,621,622,623,624,711,712,713,721,722,811,812,813,814,921,922,923,924,925,926,927,928)
-- and score3d >= 0.05;

insert overwrite table hdp_oc_team.ShopDomainRegNaics
select distinct shopper_id, domainid, naics3d as naics, score3d as conf_score, case when naics3d in(621,622,623,624) then 'Regulated - Medical' else 'Regulated - Non-Medical' end as vertical, rank3d
from bkamble.tmp_naics_classifier_3digit_NoKeyWords
where naics3d in(111,112,113,114,115,211,212,213,221,236,237,238,311,312,313,314,315,316,321,322,323,324,325,326,327,331,332,333,334,335,336,337,339,423,424,425,441,442,443,444,445,446,447,448,451,452,453,454,481,482,483,484,485,486,487,488,491,492,493,511,512,515,517,518,519,521,522,523,524,525,531,532,533,541,551,561,562,611,621,622,623,624,711,712,713,721,722,811,812,813,814,921,922,923,924,925,926,927,928)
and score3d >= 0.05;