USE hdp_oc_team;

--Updated 5/19/2016 to include the new domain_workspace table
DROP TABLE IF EXISTS shopactvdomain;
CREATE TABLE shopactvdomain stored as textfile as
select shopper_id, domainid, domainName, case when nameserver_category_name = 'external' then 1 else 0 end as ExternalDNS, hostingdescription as HostingProduct, emaildescription as EmailProduct, domain_activation_flag as IsActv, has_workspace_flag as HasWorkspace
from dbmarketing.domain_activation_status;