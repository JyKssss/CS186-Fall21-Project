-- Before running drop any existing views
DROP VIEW IF EXISTS q0;
DROP VIEW IF EXISTS q1i;
DROP VIEW IF EXISTS q1ii;
DROP VIEW IF EXISTS q1iii;
DROP VIEW IF EXISTS q1iv;
DROP VIEW IF EXISTS q2i;
DROP VIEW IF EXISTS q2ii;
DROP VIEW IF EXISTS q2iii;
DROP VIEW IF EXISTS q3i;
DROP VIEW IF EXISTS q3ii;
DROP VIEW IF EXISTS q3iii;
DROP VIEW IF EXISTS q4i;
DROP VIEW IF EXISTS q4ii;
DROP VIEW IF EXISTS q4iii;
DROP VIEW IF EXISTS q4iv;
DROP VIEW IF EXISTS q4v;

-- Question 0
CREATE VIEW q0(era) AS
 SELECT MAX(era)
 FROM pitching
;

-- Question 1i
CREATE VIEW q1i(namefirst, namelast, birthyear)
AS
  SELECT namefirst, namelast, birthyear
  FROM people
  WHERE weight>300
;

-- Question 1ii
CREATE VIEW q1ii(namefirst, namelast, birthyear)
AS
  SELECT namefirst, namelast, birthyear
  FROM people
  WHERE namefirst LIKE '% %'
  ORDER  BY namefirst asc, namelast asc
;

-- Question 1iii
CREATE VIEW q1iii(birthyear, avgheight, count)
AS
  SELECT birthyear, avg(height)as avgheight, count(*) as count
  FROM people
  GROUP BY birthyear
  ORDER BY birthyear asc
;

-- Question 1iv
CREATE VIEW q1iv(birthyear, avgheight, count)
AS
  SELECT birthyear, avg(height)as avgheight, count(*) as count
  FROM people
  GROUP BY birthyear
  HAVING avgheight>70
  ORDER BY birthyear asc
;

-- Question 2i
CREATE VIEW q2i(namefirst, namelast, playerid, yearid)
AS
  SELECT  p.namefirst, p.namelast, p.playerid, h.yearid
  FROM halloffame h left join people p on h.playerID=p.playerID
  WHERE h.inducted='Y'
  ORDER BY h.yearid desc, p.playerid asc
;

-- Question 2ii
CREATE VIEW q2ii(namefirst, namelast, playerid, schoolid, yearid)
AS
  SELECT p.namefirst, p.namelast, p.playerid, a.schoolid, h.yearid
  FROM halloffame h,people p,
      (SELECT  cp.playerid, cp.schoolid
      FROM collegeplaying cp, schools s
      WHERE  cp.schoolid = s.schoolid AND schoolstate='CA') as a
  WHERE h.playerID=p.playerID AND  p.playerid=a.playerid AND   h.inducted='Y'
  ORDER BY h.yearid desc,a.schoolid asc , p.playerid asc
;

-- Question 2iii
CREATE VIEW q2iii(playerid, namefirst, namelast, schoolid)
AS
  SELECT p.playerid, p.namefirst, p.namelast, a.schoolid
  FROM people p ,
    (SELECT distinct h.playerid, cp.schoolid
    FROM halloffame h left join collegeplaying cp on h.playerid=cp.playerid
    WHERE  h.inducted='Y') as a
  WHERE p.playerid=a.playerid
   ORDER BY p.playerid desc, a.schoolid asc

;

-- Question 3i
CREATE VIEW q3i(playerid, namefirst, namelast, yearid, slg)
AS
  SELECT p.playerid, p.namefirst, p.namelast, b.yearid,
  (CAST ((1* (H-H2B-H3B-HR)+2*H2B+ 3*H3B+ 4*HR) as FLOAT )/CAST(AB as FLOAT) ) as slg
  FROM people p, batting b
  WHERE p.playerid=b.playerid and b.ab>50
  ORDER  BY  slg desc, yearid asc, p.playerid asc
  LIMIT 10
;

-- Question 3ii
CREATE VIEW q3ii(playerid, namefirst, namelast, lslg)
AS
  SELECT p.playerid, p.namefirst, p.namelast,
  (CAST ((1* (H-H2B-H3B-HR)+2*H2B+ 3*H3B+ 4*HR) as FLOAT )/CAST(AB as FLOAT) ) as lslg
  FROM people p,
  (SELECT SUM(H)as H,SUM(H2B)as H2B,SUM(H3B)as H3B,SUM(HR)as HR,SUM(AB)as AB, playerid
  FROM batting
  GROUP BY playerid) as b
  WHERE  p.playerid=b.playerid and b.ab>50
  ORDER  BY  lslg desc, p.playerid asc
  LIMIT 10
;

-- Question 3iii
CREATE VIEW q3iii(namefirst, namelast, lslg)
AS
  SELECT  p.namefirst, p.namelast,
  (CAST ((1* (H-H2B-H3B-HR)+2*H2B+ 3*H3B+ 4*HR) as FLOAT )/CAST(AB as FLOAT) ) as lslg
  FROM people p,
  (SELECT SUM(H)as H,SUM(H2B)as H2B,SUM(H3B)as H3B,SUM(HR)as HR,SUM(AB)as AB, playerid
  FROM batting
  GROUP BY playerid) as b
  WHERE  p.playerid=b.playerid and b.ab>50 and
  lslg > (SELECT (CAST ((1* (d.H-d.H2B-d.H3B-d.HR)+2*d.H2B+ 3*d.H3B+ 4*d.HR) as FLOAT )/CAST(d.AB as FLOAT) ) as lslg  FROM people p1,
  (SELECT SUM(H)as H,SUM(H2B)as H2B,SUM(H3B)as H3B,SUM(HR)as HR,SUM(AB)as AB, playerid
  FROM batting
  GROUP BY playerid) as d
  WHERE  p1.playerid=d.playerid and p1.playerid='mayswi01'
  LIMIT 1 )
  ORDER  BY  lslg desc, p.playerid asc
;

-- Question 4i
CREATE VIEW q4i(yearid, min, max, avg)
AS
  SELECT yearid, min(salary), max(salary), avg(salary)
  FROM  salaries
  group  by yearid
  order by yearid asc
;

-- Question 4ii
CREATE VIEW q4ii(binid, low, high, count)
AS
   SELECT a.binid,
   (SELECT  min(salary)  FROM salaries WHERE yearid='2016')
   + binid
   * (SELECT CAST(MAX(salary) - MIN(salary) as float) / CAST(10 as float) FROM salaries WHERE yearid = 2016)
   as low,
   (SELECT  min(salary)  FROM salaries WHERE yearid='2016')
   + (binid+1)
   * (SELECT CAST(MAX(salary) - MIN(salary) as float) / CAST(10 as float) FROM salaries WHERE yearid = 2016)
   as high,
   count(*) as count
    FROM
     (SELECT  salary,
    CAST(((CAST (salary as float) - CAST ((SELECT  min(salary)  FROM salaries WHERE yearid='2016')as float)) /CAST((SELECT  max(salary)-min(salary)+1  FROM salaries WHERE yearid='2016' )as float) )*10 as INTEGER ) as binid
     FROM salaries WHERE yearid='2016') as a
    GROUP BY a.binid
    ORDER  BY a.binid
;
-- SELECT  salary,
--     CAST(((CAST (salary as float) - CAST ((SELECT  min(salary)  FROM salaries WHERE yearid='2016')as float)) /CAST((SELECT  max(salary)-min(salary)+1  FROM salaries WHERE yearid='2016' )as float) )*10 as INTEGER ) as binid
--      FROM salaries WHERE yearid='2016'
--
--      SELECT
--      CAST((SELECT  max(salary)-min(salary)+1  FROM salaries WHERE yearid='2016' )as float)
--      FROM salaries WHERE yearid='2016'
-- Question 4iii
CREATE VIEW q4iii(yearid, mindiff, maxdiff, avgdiff)
AS
  SELECT a.yearid,(a.min- b.min) as mindiff, (a.max- b.max) as maxdiff, (a.avg- b.avg) as avgdiff
 FROM
  (SELECT  yearid, MIN(salary) as min ,MAX(salary) as max ,AVG(salary) as avg
  FROM salaries
  GROUP BY(yearid)
  ORDER BY yearid ) as a ,
  (SELECT  yearid, MIN(salary) as min ,MAX(salary) as max ,AVG(salary) as avg
  FROM salaries
  GROUP BY(yearid)
  ORDER BY yearid ) as b
  WHERE a.yearid=b.yearid+1
  ORDER BY a.yearid
;

-- Question 4iv
CREATE VIEW q4iv(playerid, namefirst, namelast, salary, yearid)
AS
  SELECT p.playerid, namefirst, namelast, salary, yearid
  FROM people p LEFT JOIN  salaries s on p.playerid = s.playerid
  WHERE (yearid='2000' AND
  salary >= (SELECT MAX(salary) FROM salaries WHERE yearid='2000'))
  OR
  (yearid='2001' AND
  salary >= (SELECT MAX(salary) FROM salaries WHERE yearid='2001'))
;
-- Question 4v
CREATE VIEW q4v(team, diffAvg) AS
  SELECT a.teamid as team, MAX(salary)-MIN(salary) as diffAvg
  FROM  allstarfull a left join salaries s on a.playerid=s.playerid
  WHERE a.yearid='2016' and s.yearid='2016'
  GROUP BY a.teamid

;

