CREATE OR REPLACE VIEW state_dashboard_ca.ela_historical AS

WITH cte_ela_sy22 AS (
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
    FROM state_dashboard_ca.eladownload2022
)

, cte_ela_sy19 AS (
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
    FROM state_dashboard_ca.eladownload2019 down
    LEFT JOIN state_dashboard_ca.elapratedownload2019 prate
)

, cte_ela_sy18 AS (
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
    FROM state_dashboard_ca.eladownload2018 down
    LEFT JOIN state_dashboard_ca.elapratedownload2018 prate
)


SELECT * FROM cte_ela_sy22

UNION ALL

SELECT * FROM cte_ela_sy19

UNION ALL

SELECT * FROM cte_ela_sy18
