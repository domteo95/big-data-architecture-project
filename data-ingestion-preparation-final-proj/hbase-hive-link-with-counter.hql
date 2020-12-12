# CREATING HBASE TABLES LINKED TO HIVE

drop table if exists dominicteo_hbase_proj_team;
create table dominicteo_hbase_proj_team (
  team_year string, year int, num_players bigint, wages_money bigint,
  values_money bigint, overall_ratings bigint, potential_ratings bigint,
  pace_ratings bigint, shooting_ratings bigint, passing_ratings bigint, dribbling_ratings bigint, 
  defending_ratings bigint, physical_ratings bigint, num_european_players bigint, num_asian_players bigint,
  num_samerican_players bigint, num_namerican_players bigint, num_african_players bigint, 
  num_oceania_players bigint, country_ratings bigint, fifa_version string)
  STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES ('hbase.columns.mapping' = ':key,stats:year,stats:num_players,stats:wages_money,stats:values_money,stats:overall_ratings,stats:potential_ratings,stats:pace_ratings,stats:shooting_ratings,stats:passing_ratings,stats:dribbling_ratings,stats:defending_ratings,stats:physical_ratings,stats:num_european_players,stats:num_asian_players,stats:num_samerican_players,stats:num_namerican_players,stats:num_african_players,stats:num_oceania_players,stats:country_ratings,stats:fifa_version')
TBLPROPERTIES ('hbase.table.name' = 'dominicteo_hbase_proj_team');

insert overwrite table dominicteo_hbase_proj_team 
select * from dominicteo_hive_proj_teams;

drop table if exists dominicteo_hbase_proj_unique;
create table dominicteo_hbase_proj_unique (
  unique_team string, team string)
  STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES ('hbase.columns.mapping' = ':key,info:team')
TBLPROPERTIES ('hbase.table.name' = 'dominicteo_hbase_proj_unique');

insert overwrite table dominicteo_hbase_proj_unique
select * from dominicteo_hive_proj_unique;


# CREATING HBASE TABLE LINKED TO HIVE THAT CONTAINS COUNTER

drop table if exists dominicteo_hbase_proj_team_v2;
create table dominicteo_hbase_proj_team_v2 (
  team_year string, year int, num_players bigint, wages_money bigint,
  values_money bigint, overall_ratings bigint, potential_ratings bigint,
  pace_ratings bigint, shooting_ratings bigint, passing_ratings bigint, dribbling_ratings bigint, 
  defending_ratings bigint, physical_ratings bigint, num_european_players bigint, num_asian_players bigint,
  num_samerican_players bigint, num_namerican_players bigint, num_african_players bigint, 
  num_oceania_players bigint, country_ratings bigint, fifa_version string)
  STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES ('hbase.columns.mapping' = ':key,stats:year,stats:num_players#b,stats:wages_money#b,stats:values_money#b,stats:overall_ratings#b,stats:potential_ratings#b,stats:pace_ratings#b,stats:shooting_ratings#b,stats:passing_ratings#b,stats:dribbling_ratings#b,stats:defending_ratings#b,stats:physical_ratings#b,stats:num_european_players#b,stats:num_asian_players#b,stats:num_samerican_players#b,stats:num_namerican_players#b,stats:num_african_players#b,stats:num_oceania_players#b,stats:country_ratings#b,stats:fifa_version')
TBLPROPERTIES ('hbase.table.name' = 'dominicteo_hbase_proj_team_v2');

insert overwrite table dominicteo_hbase_proj_team_v2 
select * from dominicteo_hive_proj_teams;