drop table if exists dominicteo_proj_players_csv;
create external table dominicteo_proj_players_csv (
index INT,
id STRING,
name STRING,
age INT, 
nationality STRING,
overall INT, 
potential INT, 
club STRING, 
value FLOAT, 
wage INT, 
pac INT,
sho INT,
pas INT,
dri INT,
def INT,
phy INT,
fifa_version STRING,
year STRING)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
   "separatorChar" = ",",
   "quoteChar"     = "\""
)
STORED AS TEXTFILE
location 's3://dominicteo-mpcs53014/direct_from_python/'
TBLPROPERTIES ("skip.header.line.count"="1"); 


drop table if exists dominicteo_proj_players;
create table dominicteo_proj_players (
country_year STRING,
index INT,
id STRING,
name STRING,
age INT, 
nationality STRING,
overall INT, 
potential INT, 
club STRING, 
value FLOAT, 
wage INT, 
pac INT,
sho INT,
pas INT,
dri INT,
def INT,
phy INT,
fifa_version STRING,
year STRING)
stored as orc;

insert overwrite table dominicteo_proj_players select concat(nationality, year),
index, id, name, age, nationality, overall,potential,club,value,wage,pac,sho,pas,dri,def,phy,fifa_version,year
from dominicteo_proj_players_csv
where club != ""and club != "111648" and club != "113974" and club != "114912";

drop table if exists dominicteo_proj_country_csv;
create external table dominicteo_proj_country_csv (
id INT,
rank INT,
country_full STRING, 
country_abrv STRING,
total_points INT, 
previous_points INT, 
rank_change INT, 
confederation STRING, 
year STRING) 
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
   "separatorChar" = ",",
   "quoteChar"     = "\""
)
STORED AS TEXTFILE
location 's3://dominicteo-mpcs53014/final_proj_country_curl/'
TBLPROPERTIES ("skip.header.line.count"="1");

drop table if exists dominicteo_proj_country;
create table dominicteo_proj_country (
country_year STRING,
id INT,
rank INT,
nationality STRING, 
country_abrv STRING,
total_points INT, 
previous_points INT, 
rank_change INT, 
confederation STRING, 
year STRING)
stored as orc;

insert overwrite table dominicteo_proj_country select concat(country_full, substr(year, 1, 4)),
id, rank, country_full, country_abrv, total_points, previous_points,rank_change, confederation,
int(substr(year, 1, 4)) from dominicteo_proj_country_csv;