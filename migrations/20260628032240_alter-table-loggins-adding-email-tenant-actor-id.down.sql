-- Add down migration script here
alter table loggings drop column email;
alter table loggings drop column tenant_id;
alter table loggings drop column actor_id;
