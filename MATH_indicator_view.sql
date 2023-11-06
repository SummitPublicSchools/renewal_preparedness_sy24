CREATE OR REPLACE VIEW state_dashboard_ca.math_historical AS

WITH cte_math_sy22 AS (
    SELECT cds,
           rtype,
           schoolname,
           districtname,
           countyname,
           charter_flag,
           coe_flag,
           dass_flag,
           studentgroup,
           currdenom,
           currstatus,
           statuslevel,
           hscutpoints,
           pairshare_method,
           prate_enrolled,
           prate_tested,
           prate,
           reportingyear
    FROM state_dashboard_ca.mathdownload2022
)

, cte_math_sy19 AS (
    SELECT down.cds,
           down.rtype,
           down.schoolname,
           down.districtname,
           down.countyname,
           down.charter_flag,
           down.coe_flag,
           down.dass_flag,
           down.studentgroup,
           down.currdenom,
           down.currstatus,
           down.statuslevel,
           down.hscutpoints,
           down.pairshare_method,
           prate.enrolled AS prate_enrolled,
           prate.tested AS prate_tested,
           prate.prate,
           down.reportingyear
    FROM state_dashboard_ca.mathdownload2019 down
    LEFT JOIN state_dashboard_ca.mathpratedownload2019 prate
        ON down.cds = prate.cds
        AND down.studentgroup = prate.studentgroup
)

, cte_math_sy18 AS (
    SELECT down.cds,
           down.rtype,
           down.schoolname,
           down.districtname,
           down.countyname,
           down.charter_flag,
           down.coe_flag,
           down.dass_flag,
           down.studentgroup,
           down.currdenom,
           down.currstatus,
           down.statuslevel,
           down.hscutpoints,
           down.pairshare_method,
           prate.enrolled AS prate_enrolled,
           prate.tested AS prate_tested,
           prate.prate,
           down.reportingyear
    FROM state_dashboard_ca.mathdownload2018 down
    LEFT JOIN state_dashboard_ca.mathpratedownload2018 prate
        ON down.cds = prate.cds
        AND down.studentgroup = prate.studentgroup
)


SELECT * FROM cte_math_sy22

UNION ALL

SELECT * FROM cte_math_sy19

UNION ALL

SELECT * FROM cte_math_sy18
