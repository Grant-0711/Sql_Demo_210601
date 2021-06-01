with t1 as(
select 
  t.team_id,
  m.home_team_goals,
  m.away_team_goals goal_against,
  count(*)  matches_played_ashome,
    (case when m.home_team_goals > m.away_team_goals then 3
  when m.home_team_goals < m.away_team_goals then 0
  when m.home_team_goals = m.away_team_goals then 1 end) as score
from
Teams t left join Matches m 
on t.team_id = m.home_team_id
group by 
t.team_id
),
--客队得分统计
t2 as (
select 
  t.team_id,
  m. away_team_goals,
  m.home_team_goals goal_against,
  count(*)  matches_played_asaway ,
    (case when m.home_team_goals > m.away_team_goals then 1
  when m.home_team_goals < m.away_team_goals then -1
  when m.home_team_goals = m.away_team_goals then 0 end) as score
from
Teams t left join Matches m 
on t.team_id = m.away_team_id
group by 
t.team_id

)
--总得分以及次数统计
--t3 as (
select 
  t1.team_id,
  t1.matches_played_ashome + t2.matches_played_asaway matches_played,
  t1.score + t2.score points,
  t1.home_team_goals + t2.away_team_goals goal_for,
  t1.goal_against + t2.goal_against goal_against
from
t1 left join t2 
on t1.team_id = t2.team_id;