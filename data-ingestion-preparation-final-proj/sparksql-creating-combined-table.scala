

val country = spark.table("dominicteo_proj_country")
val players = spark.table("dominicteo_proj_players")
country.createOrReplaceTempView("country")

val country_clean = spark.sql(""" select distinct a.country_year as country_year, a.rank, b.confederation, b.year 
from (select country_year, min(rank) as rank from country group by country_year) a join country b
on a.country_year = b.country_year""")

country_clean.createOrReplaceTempView("country_clean")
players.createOrReplaceTempView("players")

val players_country = spark.sql("""select p.country_year, p.name, p.age, p.nationality, p.club, p.value, p.wage, 
p.overall, p.potential, p.pac, p.sho, p.pas, p.dri, p.def, p.phy, p.fifa_version, c.rank, c.confederation, c.year
from country_clean c join players p
on c.country_year = p.country_year""")

players_country.createOrReplaceTempView("players_country")

val team_year = spark.sql("""select concat(concat(club, "!"),year) as team_year, year, count(1) as num_players,
sum(wage) as sum_wages_thousands, bround(sum(value),0) as sum_player_values_millions, sum(overall) as overall_ratings, sum(potential) as potential_ratings, sum(pac) as pace_ratings, sum(sho) as shooting_ratings, sum(pas) as passing_ratings, sum(dri) as dribbling_ratings, sum(def) as defending_ratings, sum(phy) as physical_ratings, sum(case when confederation == "UEFA" then 1 else 0 end) as num_european_players, sum(case when confederation == "AFC" then 1 else 0 end) as num_asian_players, sum(case when confederation == "CONMEBOL" then 1 else 0 end) as num_samerican_players, sum(case when confederation == "CONCACAF" then 1 else 0 end) as num_namerican_players, sum(case when confederation == "CAF" then 1 else 0 end) as num_african_players, sum(case when confederation == "OFC" then 1 else 0 end) as num_oceania_players, sum(rank) as country_ratings, concat("FIFA ", year+1) as fifa_version
from players_country
group by club, year""")

import org.apache.spark.sql.SaveMode
team_year.write.mode(SaveMode.Overwrite).saveAsTable("dominicteo_hive_proj_teams")

val teams = spark.sql("""select distinct club as unique_team, club as team from players""")
teams.write.mode(SaveMode.Overwrite).saveAsTable("dominicteo_hive_proj_unique")
