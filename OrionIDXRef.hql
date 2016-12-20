--create table hdp_oc_team.do_resource_orion_xref ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' STORED AS TEXTFILE as
--select resource_id, external_resource_id as orion_id,common_name
--from dm_ecommerce.fact_billing_external_resource;

use hdp_oc_team;

insert overwrite table hdp_oc_team.do_resource_orion_xref
select resource_id, external_resource_id as orion_id, common_name
from dm_ecommerce.fact_billing_external_resource;