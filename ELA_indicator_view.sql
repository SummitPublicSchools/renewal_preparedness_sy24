CREATE OR REPLACE VIEW state_dashboard_ca.ela_historical AS

WITH cte_ela_nonstate_sy22 AS (
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
    FROM state_dashboard_ca.eladownload2022
    WHERE rtype != 'X'
)
,cte_ela_state_sy22 AS (
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
    FROM state_dashboard_ca.eladownload2022
    WHERE rtype = 'X'
)
,cte_ela_authorizer_sy22 AS (
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
    FROM state_dashboard_ca.eladownload2022
    WHERE rtype = 'D'
)
, cte_ela_compare_sy22 AS (
    SELECT cte_ela_nonstate_sy22.*
        , cte_ela_state_sy22.currdenom AS currdenom_state
        , cte_ela_state_sy22.currstatus AS currstatus_state
        , cte_ela_state_sy22.priordenom AS priordenom_state
        , cte_ela_state_sy22.priorstatus AS priorstatus_state
        , cte_ela_state_sy22.change AS change_state
        , cte_ela_state_sy22.statuslevel AS statuslevel_state
        , cte_ela_state_sy22.changelevel AS changelevel_state
        , cte_ela_state_sy22.color AS color_state
        , cte_ela_state_sy22.box AS box_state
        , cte_ela_state_sy22.hscutpoints AS hscutpoints_state
        , cte_ela_state_sy22.pairshare_method AS pairshare_method_state
        , cte_ela_state_sy22.prate_enrolled AS prate_enrolled_state
        , cte_ela_state_sy22.prate_tested AS prate_tested_state
        , cte_ela_state_sy22.prate AS prate_state
        , cte_ela_nonstate_sy22.currstatus - cte_ela_state_sy22.currstatus AS currstatus_comparison_state
        , cte_ela_nonstate_sy22.statuslevel - cte_ela_state_sy22.statuslevel AS statuslevel_comparison_state
        , cte_ela_nonstate_sy22.change - cte_ela_state_sy22.change AS change_comparison_state
        , cte_ela_nonstate_sy22.changelevel - cte_ela_state_sy22.changelevel AS changelevel_comparison_state
        , cte_ela_authorizer_sy22.currdenom AS currdenom_authorizer
        , cte_ela_authorizer_sy22.currstatus AS currstatus_authorizer
        , cte_ela_authorizer_sy22.priordenom AS priordenom_authorizer
        , cte_ela_authorizer_sy22.priorstatus AS priorstatus_authorizer
        , cte_ela_authorizer_sy22.change AS change_authorizer
        , cte_ela_authorizer_sy22.statuslevel AS statuslevel_authorizer
        , cte_ela_authorizer_sy22.changelevel AS changelevel_authorizer
        , cte_ela_authorizer_sy22.color AS color_authorizer
        , cte_ela_authorizer_sy22.box AS box_authorizer
        , cte_ela_authorizer_sy22.hscutpoints AS hscutpoints_authorizer
        , cte_ela_authorizer_sy22.pairshare_method AS pairshare_method_authorizer
        , cte_ela_authorizer_sy22.prate_enrolled AS prate_enrolled_authorizer
        , cte_ela_authorizer_sy22.prate_tested AS prate_tested_authorizer
        , cte_ela_authorizer_sy22.prate AS prate_authorizer
        , cte_ela_nonstate_sy22.currstatus - cte_ela_authorizer_sy22.currstatus AS currstatus_comparison_authorizer
        , cte_ela_nonstate_sy22.statuslevel - cte_ela_authorizer_sy22.statuslevel AS statuslevel_comparison_authorizer
        , cte_ela_nonstate_sy22.change - cte_ela_authorizer_sy22.change AS change_comparison_authorizer
        , cte_ela_nonstate_sy22.changelevel - cte_ela_authorizer_sy22.changelevel AS changelevel_comparison_authorizer
    FROM cte_ela_nonstate_sy22
    LEFT JOIN cte_ela_state_sy22
        ON cte_ela_nonstate_sy22.studentgroup = cte_ela_state_sy22.studentgroup
    LEFT JOIN cte_ela_authorizer_sy22
        ON cte_ela_nonstate_sy22.districtname = cte_ela_authorizer_sy22.districtname
        AND cte_ela_nonstate_sy22.studentgroup = cte_ela_authorizer_sy22.studentgroup
)
, cte_ela_nonstate_sy19 AS (
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
    FROM state_dashboard_ca.eladownload2019 down
    LEFT JOIN state_dashboard_ca.elapratedownload2019 prate
        ON down.cds = prate.cds
        AND down.studentgroup = prate.studentgroup
    WHERE down.rtype != 'X'
)
, cte_ela_state_sy19 AS (
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
    FROM state_dashboard_ca.eladownload2019 down
    LEFT JOIN state_dashboard_ca.elapratedownload2019 prate
        ON down.cds = prate.cds
        AND down.studentgroup = prate.studentgroup
    WHERE down.rtype = 'X'
)
, cte_ela_authorizer_sy19 AS (
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
    FROM state_dashboard_ca.eladownload2019 down
    LEFT JOIN state_dashboard_ca.elapratedownload2019 prate
        ON down.cds = prate.cds
        AND down.studentgroup = prate.studentgroup
    WHERE down.rtype = 'D'
)
, cte_ela_compare_sy19 AS (
    SELECT cte_ela_nonstate_sy19.*
        , cte_ela_state_sy19.currdenom AS currdenom_state
        , cte_ela_state_sy19.currstatus AS currstatus_state
        , cte_ela_state_sy19.priordenom AS priordenom_state
        , cte_ela_state_sy19.priorstatus AS priorstatus_state
        , cte_ela_state_sy19.change AS change_state
        , cte_ela_state_sy19.statuslevel AS statuslevel_state
        , cte_ela_state_sy19.changelevel AS changelevel_state
        , cte_ela_state_sy19.color AS color_state
        , cte_ela_state_sy19.box AS box_state
        , cte_ela_state_sy19.hscutpoints AS hscutpoints_state
        , cte_ela_state_sy19.pairshare_method AS pairshare_method_state
        , cte_ela_state_sy19.prate_enrolled AS prate_enrolled_state
        , cte_ela_state_sy19.prate_tested AS prate_tested_state
        , cte_ela_state_sy19.prate AS prate_state
        , cte_ela_nonstate_sy19.currstatus - cte_ela_state_sy19.currstatus AS currstatus_comparison_state
        , cte_ela_nonstate_sy19.statuslevel - cte_ela_state_sy19.statuslevel AS statuslevel_comparison_state
        , cte_ela_nonstate_sy19.change - cte_ela_state_sy19.change AS change_comparison_state
        , cte_ela_nonstate_sy19.changelevel - cte_ela_state_sy19.changelevel AS changelevel_comparison_state
        , cte_ela_authorizer_sy19.currdenom AS currdenom_authorizer
        , cte_ela_authorizer_sy19.currstatus AS currstatus_authorizer
        , cte_ela_authorizer_sy19.priordenom AS priordenom_authorizer
        , cte_ela_authorizer_sy19.priorstatus AS priorstatus_authorizer
        , cte_ela_authorizer_sy19.change AS change_authorizer
        , cte_ela_authorizer_sy19.statuslevel AS statuslevel_authorizer
        , cte_ela_authorizer_sy19.changelevel AS changelevel_authorizer
        , cte_ela_authorizer_sy19.color AS color_authorizer
        , cte_ela_authorizer_sy19.box AS box_authorizer
        , cte_ela_authorizer_sy19.hscutpoints AS hscutpoints_authorizer
        , cte_ela_authorizer_sy19.pairshare_method AS pairshare_method_authorizer
        , cte_ela_authorizer_sy19.prate_enrolled AS prate_enrolled_authorizer
        , cte_ela_authorizer_sy19.prate_tested AS prate_tested_authorizer
        , cte_ela_authorizer_sy19.prate AS prate_authorizer
        , cte_ela_nonstate_sy19.currstatus - cte_ela_authorizer_sy19.currstatus AS currstatus_comparison_authorizer
        , cte_ela_nonstate_sy19.statuslevel - cte_ela_authorizer_sy19.statuslevel AS statuslevel_comparison_authorizer
        , cte_ela_nonstate_sy19.change - cte_ela_authorizer_sy19.change AS change_comparison_authorizer
        , cte_ela_nonstate_sy19.changelevel - cte_ela_authorizer_sy19.changelevel AS changelevel_comparison_authorizer
    FROM cte_ela_nonstate_sy19
    LEFT JOIN cte_ela_state_sy19
        ON cte_ela_nonstate_sy19.studentgroup = cte_ela_state_sy19.studentgroup
    LEFT JOIN cte_ela_authorizer_sy19
        ON cte_ela_nonstate_sy19.districtname = cte_ela_authorizer_sy19.districtname
        AND cte_ela_nonstate_sy19.studentgroup = cte_ela_authorizer_sy19.studentgroup
)
, cte_ela_nonstate_sy18 AS (
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
    FROM state_dashboard_ca.eladownload2018 down
    LEFT JOIN state_dashboard_ca.elapratedownload2018 prate
        ON down.cds = prate.cds
        AND down.studentgroup = prate.studentgroup
    WHERE down.rtype != 'X'
)
, cte_ela_state_sy18 AS (
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
    FROM state_dashboard_ca.eladownload2018 down
    LEFT JOIN state_dashboard_ca.elapratedownload2018 prate
        ON down.cds = prate.cds
        AND down.studentgroup = prate.studentgroup
    WHERE down.rtype = 'X'
)
, cte_ela_authorizer_sy18 AS (
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
    FROM state_dashboard_ca.eladownload2018 down
    LEFT JOIN state_dashboard_ca.elapratedownload2018 prate
        ON down.cds = prate.cds
        AND down.studentgroup = prate.studentgroup
    WHERE down.rtype = 'D'
)
, cte_ela_compare_sy18 AS (
    SELECT cte_ela_nonstate_sy18.*
        , cte_ela_state_sy18.currdenom AS currdenom_state
        , cte_ela_state_sy18.currstatus AS currstatus_state
        , cte_ela_state_sy18.priordenom AS priordenom_state
        , cte_ela_state_sy18.priorstatus AS priorstatus_state
        , cte_ela_state_sy18.change AS change_state
        , cte_ela_state_sy18.statuslevel AS statuslevel_state
        , cte_ela_state_sy18.changelevel AS changelevel_state
        , cte_ela_state_sy18.color AS color_state
        , cte_ela_state_sy18.box AS box_state
        , cte_ela_state_sy18.hscutpoints AS hscutpoints_state
        , cte_ela_state_sy18.pairshare_method AS pairshare_method_state
        , cte_ela_state_sy18.prate_enrolled AS prate_enrolled_state
        , cte_ela_state_sy18.prate_tested AS prate_tested_state
        , cte_ela_state_sy18.prate AS prate_state
        , cte_ela_nonstate_sy18.currstatus - cte_ela_state_sy18.currstatus AS currstatus_comparison_state
        , cte_ela_nonstate_sy18.statuslevel - cte_ela_state_sy18.statuslevel AS statuslevel_comparison_state
        , cte_ela_nonstate_sy18.change - cte_ela_state_sy18.change AS change_comparison_state
        , cte_ela_nonstate_sy18.changelevel - cte_ela_state_sy18.changelevel AS changelevel_comparison_state
        , cte_ela_authorizer_sy18.currdenom AS currdenom_authorizer
        , cte_ela_authorizer_sy18.currstatus AS currstatus_authorizer
        , cte_ela_authorizer_sy18.priordenom AS priordenom_authorizer
        , cte_ela_authorizer_sy18.priorstatus AS priorstatus_authorizer
        , cte_ela_authorizer_sy18.change AS change_authorizer
        , cte_ela_authorizer_sy18.statuslevel AS statuslevel_authorizer
        , cte_ela_authorizer_sy18.changelevel AS changelevel_authorizer
        , cte_ela_authorizer_sy18.color AS color_authorizer
        , cte_ela_authorizer_sy18.box AS box_authorizer
        , cte_ela_authorizer_sy18.hscutpoints AS hscutpoints_authorizer
        , cte_ela_authorizer_sy18.pairshare_method AS pairshare_method_authorizer
        , cte_ela_authorizer_sy18.prate_enrolled AS prate_enrolled_authorizer
        , cte_ela_authorizer_sy18.prate_tested AS prate_tested_authorizer
        , cte_ela_authorizer_sy18.prate AS prate_authorizer
        , cte_ela_nonstate_sy18.currstatus - cte_ela_authorizer_sy18.currstatus AS currstatus_comparison_authorizer
        , cte_ela_nonstate_sy18.statuslevel - cte_ela_authorizer_sy18.statuslevel AS statuslevel_comparison_authorizer
        , cte_ela_nonstate_sy18.change - cte_ela_authorizer_sy18.change AS change_comparison_authorizer
        , cte_ela_nonstate_sy18.changelevel - cte_ela_authorizer_sy18.changelevel AS changelevel_comparison_authorizer
    FROM cte_ela_nonstate_sy18
    LEFT JOIN cte_ela_state_sy18
        ON cte_ela_nonstate_sy18.studentgroup = cte_ela_state_sy18.studentgroup
    LEFT JOIN cte_ela_authorizer_sy18
        ON cte_ela_nonstate_sy18.districtname = cte_ela_authorizer_sy18.districtname
        AND cte_ela_nonstate_sy18.studentgroup = cte_ela_authorizer_sy18.studentgroup
)

SELECT *
    , CASE WHEN schoolname LIKE '%Tahoma%' THEN 'Tahoma'
            WHEN schoolname LIKE '%Everest%' THEN 'Everest'
            WHEN schoolname LIKE '%Prep%' THEN 'Prep'
            WHEN schoolname LIKE '%Shasta%' THEN 'Shasta'
            WHEN schoolname LIKE '%K2%' THEN 'K2'
            WHEN schoolname LIKE '%Tam%' THEN 'Tam'
            ELSE NULL END AS reported_site_short_name
FROM cte_ela_compare_sy22

UNION ALL

SELECT *
    , CASE WHEN schoolname LIKE '%Tahoma%' THEN 'Tahoma'
            WHEN schoolname LIKE '%Everest%' THEN 'Everest'
            WHEN schoolname LIKE '%Prep%' THEN 'Prep'
            WHEN schoolname LIKE '%Shasta%' THEN 'Shasta'
            WHEN schoolname LIKE '%K2%' THEN 'K2'
            WHEN schoolname LIKE '%Tam%' THEN 'Tam'
            ELSE NULL END AS reported_site_short_name
FROM cte_ela_compare_sy19

UNION ALL

SELECT *
    , CASE WHEN schoolname LIKE '%Tahoma%' THEN 'Tahoma'
            WHEN schoolname LIKE '%Everest%' THEN 'Everest'
            WHEN schoolname LIKE '%Prep%' THEN 'Prep'
            WHEN schoolname LIKE '%Shasta%' THEN 'Shasta'
            WHEN schoolname LIKE '%K2%' THEN 'K2'
            WHEN schoolname LIKE '%Tam%' THEN 'Tam'
            ELSE NULL END AS reported_site_short_name
FROM cte_ela_compare_sy18