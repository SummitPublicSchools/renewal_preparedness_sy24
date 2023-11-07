CREATE OR REPLACE VIEW state_dashboard_ca.math_historical AS

WITH cte_math_nonstate_sy22 AS (
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
           TRY_TO_NUMBER(currstatus,10,1) AS currstatus,
           NULL AS priordenom,
           NULL AS priorstatus,
           NULL AS change,
           TRY_TO_NUMBER(statuslevel) AS statuslevel,
           NULL AS changelevel,
           NULL AS color,
           NULL AS box,
           hscutpoints,
           pairshare_method,
           prate_enrolled,
           prate_tested,
           prate,
           reportingyear
    FROM state_dashboard_ca.mathdownload2022
    WHERE rtype != 'X'
)
,cte_math_state_sy22 AS (
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
           TRY_TO_NUMBER(currstatus,10,1) AS currstatus,
           NULL AS priordenom,
           NULL AS priorstatus,
           NULL AS change,
           TRY_TO_NUMBER(statuslevel) AS statuslevel,
           NULL AS changelevel,
           NULL AS color,
           NULL AS box,
           hscutpoints,
           pairshare_method,
           prate_enrolled,
           prate_tested,
           prate,
           reportingyear
    FROM state_dashboard_ca.mathdownload2022
    WHERE rtype = 'X'
)
, cte_math_state_compare_sy22 AS (
    SELECT cte_math_nonstate_sy22.*
        , cte_math_state_sy22.currdenom AS currdenom_state
        , cte_math_state_sy22.currstatus AS currstatus_state
        , cte_math_state_sy22.priordenom AS priordenom_state
        , cte_math_state_sy22.priorstatus AS priorstatus_state
        , cte_math_state_sy22.change AS change_state
        , cte_math_state_sy22.statuslevel AS statuslevel_state
        , cte_math_state_sy22.changelevel AS changelevel_state
        , cte_math_state_sy22.color AS color_state
        , cte_math_state_sy22.box AS box_state
        , cte_math_state_sy22.hscutpoints AS hscutpoints_state
        , cte_math_state_sy22.pairshare_method AS pairshare_method_state
        , cte_math_state_sy22.prate_enrolled AS prate_enrolled_state
        , cte_math_state_sy22.prate_tested AS prate_tested_state
        , cte_math_state_sy22.prate AS prate_state
        , cte_math_nonstate_sy22.currstatus - cte_math_state_sy22.currstatus AS currstatus_comparison
        , cte_math_nonstate_sy22.statuslevel - cte_math_state_sy22.statuslevel AS statuslevel_comparison
        , cte_math_nonstate_sy22.change - cte_math_state_sy22.change AS change_comparison
        , cte_math_nonstate_sy22.changelevel - cte_math_state_sy22.changelevel AS changelevel_comparison
    FROM cte_math_nonstate_sy22
    LEFT JOIN cte_math_state_sy22
        ON cte_math_nonstate_sy22.studentgroup = cte_math_state_sy22.studentgroup
)
, cte_math_nonstate_sy19 AS (
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
           TRY_TO_NUMBER(down.currstatus,10,1) AS currstatus,
           down.priordenom,
           TRY_TO_NUMBER(down.priorstatus,10,1) AS priorstatus,
           TRY_TO_NUMBER(down.change,10,1) AS change,
           down.statuslevel,
           TRY_TO_NUMBER(down.changelevel) AS changelevel,
           down.color,
           down.box,
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
    WHERE down.rtype != 'X'
)
, cte_math_state_sy19 AS (
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
           TRY_TO_NUMBER(down.currstatus,10,1) AS currstatus,
           down.priordenom,
           TRY_TO_NUMBER(down.priorstatus,10,1) AS priorstatus,
           TRY_TO_NUMBER(down.change,10,1) AS change,
           down.statuslevel,
           TRY_TO_NUMBER(down.changelevel) AS changelevel,
           down.color,
           down.box,
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
    WHERE down.rtype = 'X'
)
, cte_math_state_compare_sy19 AS (
    SELECT cte_math_nonstate_sy19.*
        , cte_math_state_sy19.currdenom AS currdenom_state
        , cte_math_state_sy19.currstatus AS currstatus_state
        , cte_math_state_sy19.priordenom AS priordenom_state
        , cte_math_state_sy19.priorstatus AS priorstatus_state
        , cte_math_state_sy19.change AS change_state
        , cte_math_state_sy19.statuslevel AS statuslevel_state
        , cte_math_state_sy19.changelevel AS changelevel_state
        , cte_math_state_sy19.color AS color_state
        , cte_math_state_sy19.box AS box_state
        , cte_math_state_sy19.hscutpoints AS hscutpoints_state
        , cte_math_state_sy19.pairshare_method AS pairshare_method_state
        , cte_math_state_sy19.prate_enrolled AS prate_enrolled_state
        , cte_math_state_sy19.prate_tested AS prate_tested_state
        , cte_math_state_sy19.prate AS prate_state
        , cte_math_nonstate_sy19.currstatus - cte_math_state_sy19.currstatus AS currstatus_comparison
        , cte_math_nonstate_sy19.statuslevel - cte_math_state_sy19.statuslevel AS statuslevel_comparison
        , cte_math_nonstate_sy19.change - cte_math_state_sy19.change AS change_comparison
        , cte_math_nonstate_sy19.changelevel - cte_math_state_sy19.changelevel AS changelevel_comparison
    FROM cte_math_nonstate_sy19
    LEFT JOIN cte_math_state_sy19
        ON cte_math_nonstate_sy19.studentgroup = cte_math_state_sy19.studentgroup
)
, cte_math_nonstate_sy18 AS (
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
           TRY_TO_NUMBER(down.currstatus,10,1) AS currstatus,
           down.priordenom,
           TRY_TO_NUMBER(down.priorstatus,10,1) AS priorstatus,
           TRY_TO_NUMBER(down.change,10,1) AS change,
           down.statuslevel,
           TRY_TO_NUMBER(down.changelevel) AS changelevel,
           down.color,
           down.box,
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
    WHERE down.rtype != 'X'
)
, cte_math_state_sy18 AS (
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
           TRY_TO_NUMBER(down.currstatus,10,1) AS currstatus,
           down.priordenom,
           TRY_TO_NUMBER(down.priorstatus,10,1) AS priorstatus,
           TRY_TO_NUMBER(down.change,10,1) AS change,
           down.statuslevel,
           TRY_TO_NUMBER(down.changelevel) AS changelevel,
           down.color,
           down.box,
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
    WHERE down.rtype = 'X'
)
, cte_math_state_compare_sy18 AS (
    SELECT cte_math_nonstate_sy18.*
        , cte_math_state_sy18.currdenom AS currdenom_state
        , cte_math_state_sy18.currstatus AS currstatus_state
        , cte_math_state_sy18.priordenom AS priordenom_state
        , cte_math_state_sy18.priorstatus AS priorstatus_state
        , cte_math_state_sy18.change AS change_state
        , cte_math_state_sy18.statuslevel AS statuslevel_state
        , cte_math_state_sy18.changelevel AS changelevel_state
        , cte_math_state_sy18.color AS color_state
        , cte_math_state_sy18.box AS box_state
        , cte_math_state_sy18.hscutpoints AS hscutpoints_state
        , cte_math_state_sy18.pairshare_method AS pairshare_method_state
        , cte_math_state_sy18.prate_enrolled AS prate_enrolled_state
        , cte_math_state_sy18.prate_tested AS prate_tested_state
        , cte_math_state_sy18.prate AS prate_state
        , cte_math_nonstate_sy18.currstatus - cte_math_state_sy18.currstatus AS currstatus_comparison
        , cte_math_nonstate_sy18.statuslevel - cte_math_state_sy18.statuslevel AS statuslevel_comparison
        , cte_math_nonstate_sy18.change - cte_math_state_sy18.change AS change_comparison
        , cte_math_nonstate_sy18.changelevel - cte_math_state_sy18.changelevel AS changelevel_comparison
    FROM cte_math_nonstate_sy18
    LEFT JOIN cte_math_state_sy18
        ON cte_math_nonstate_sy18.studentgroup = cte_math_state_sy18.studentgroup
)

SELECT * FROM cte_math_state_compare_sy22

UNION ALL

SELECT * FROM cte_math_state_compare_sy19

UNION ALL

SELECT * FROM cte_math_state_compare_sy18