-- Add up migration script here
alter table loggings add column email varchar(255) default null;
alter table loggings add column tenant_id varchar(255) default null;
alter table loggings add column actor_id varchar(255) default null;
