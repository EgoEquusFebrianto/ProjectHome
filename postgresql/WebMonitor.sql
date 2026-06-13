show search_path;
set search_path to "machine_learning";

drop schema "machine_learning";

select * from "NetworkEvent_lr";
select * from "AttackSummary_lr";

select * from "NetworkEvent_rf";
select * from "AttackSummary_rf";

truncate table "NetworkEvent_lr";
truncate table "AttackSummary_lr";

truncate table "NetworkEvent_rf";
truncate table "AttackSummary_rf";

-- drop table "NetworkEvent_lr";
-- drop table "AttackSummary_lr";

-- drop table "NetworkEvent_rf";
-- drop table "AttackSummary_rf";