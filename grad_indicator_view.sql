CREATE OR REPLACE VIEW state_dashboard_ca.grad_historical AS

WITH cte_grad_nonstate_sy22 AS (
    SELECT cds,
        rtype,
        schoolname,
        districtname,
        countyname,
        charter_flag,
        coe_flag,
        dass_flag,
        studentgroup,
        currnumer,
        currdenom,
        TRY_TO_NUMBER(currstatus,10,1) AS currstatus,
        NULL AS priornumer,
        NULL AS priordenom,
        NULL AS priorstatus,
        fiveyrnumer,
        NULL AS change,
        TRY_TO_NUMBER(statuslevel) AS statuslevel,
        NULL AS changelevel,
        NULL AS color,
        NULL AS box,
        reportingyear
    FROM state_dashboard_ca.graddownload2022
    WHERE rtype = 'S'
)
, cte_grad_state_sy22 AS (
    SELECT cds,
        rtype,
        schoolname,
        districtname,
        countyname,
        charter_flag,
        coe_flag,
        dass_flag,
        studentgroup,
        currnumer,
        currdenom,
        TRY_TO_NUMBER(currstatus,10,1) AS currstatus,
        NULL AS priornumer,
        NULL AS priordenom,
        NULL AS priorstatus,
        fiveyrnumer,
        NULL AS change,
        TRY_TO_NUMBER(statuslevel) AS statuslevel,
        NULL AS changelevel,
        NULL AS color,
        NULL AS box,
        reportingyear
    FROM state_dashboard_ca.graddownload2022
    WHERE rtype = 'X'
)
, cte_grad_authorizer_sy22 AS (
    SELECT cds,
        rtype,
        schoolname,
        districtname,
        countyname,
        charter_flag,
        coe_flag,
        dass_flag,
        studentgroup,
        currnumer,
        currdenom,
        TRY_TO_NUMBER(currstatus,10,1) AS currstatus,
        NULL AS priornumer,
        NULL AS priordenom,
        NULL AS priorstatus,
        fiveyrnumer,
        NULL AS change,
        TRY_TO_NUMBER(statuslevel) AS statuslevel,
        NULL AS changelevel,
        NULL AS color,
        NULL AS box,
        reportingyear
    FROM state_dashboard_ca.graddownload2022
    WHERE rtype = 'D'
)
, cte_grad_compare_sy22 AS (
    SELECT cte_grad_nonstate_sy22.*
        , cte_grad_state_sy22.currdenom AS currdenom_state
        , cte_grad_state_sy22.currstatus AS currstatus_state
        , cte_grad_state_sy22.priordenom AS priordenom_state
        , cte_grad_state_sy22.priorstatus AS priorstatus_state
        , cte_grad_state_sy22.change AS change_state
        , cte_grad_state_sy22.statuslevel AS statuslevel_state
        , cte_grad_state_sy22.changelevel AS changelevel_state
        , cte_grad_state_sy22.color AS color_state
        , cte_grad_state_sy22.box AS box_state
        , cte_grad_nonstate_sy22.currstatus - cte_grad_state_sy22.currstatus AS currstatus_comparison_state
        , cte_grad_nonstate_sy22.statuslevel - cte_grad_state_sy22.statuslevel AS statuslevel_comparison_state
        , cte_grad_nonstate_sy22.change - cte_grad_state_sy22.change AS change_comparison_state
        , cte_grad_nonstate_sy22.changelevel - cte_grad_state_sy22.changelevel AS changelevel_comparison_state
        , cte_grad_authorizer_sy22.currdenom AS currdenom_authorizer
        , cte_grad_authorizer_sy22.currstatus AS currstatus_authorizer
        , cte_grad_authorizer_sy22.priordenom AS priordenom_authorizer
        , cte_grad_authorizer_sy22.priorstatus AS priorstatus_authorizer
        , cte_grad_authorizer_sy22.change AS change_authorizer
        , cte_grad_authorizer_sy22.statuslevel AS statuslevel_authorizer
        , cte_grad_authorizer_sy22.changelevel AS changelevel_authorizer
        , cte_grad_authorizer_sy22.color AS color_authorizer
        , cte_grad_authorizer_sy22.box AS box_authorizer
        , cte_grad_nonstate_sy22.currstatus - cte_grad_authorizer_sy22.currstatus AS currstatus_comparison_authorizer
        , cte_grad_nonstate_sy22.statuslevel - cte_grad_authorizer_sy22.statuslevel AS statuslevel_comparison_authorizer
        , cte_grad_nonstate_sy22.change - cte_grad_authorizer_sy22.change AS change_comparison_authorizer
        , cte_grad_nonstate_sy22.changelevel - cte_grad_authorizer_sy22.changelevel AS changelevel_comparison_authorizer
    FROM cte_grad_nonstate_sy22
    LEFT JOIN cte_grad_state_sy22
        ON cte_grad_nonstate_sy22.studentgroup = cte_grad_state_sy22.studentgroup
    LEFT JOIN cte_grad_authorizer_sy22
        ON cte_grad_nonstate_sy22.studentgroup = cte_grad_authorizer_sy22.studentgroup
        AND cte_grad_nonstate_sy22.districtname = cte_grad_authorizer_sy22.districtname
)
, cte_grad_nonstate_sy21 AS (
    SELECT cds,
        rtype,
        schoolname,
        districtname,
        countyname,
        charter_flag,
        coe_flag,
        dass_flag,
        studentgroup,
        currnumer,
        currdenom,
        TRY_TO_NUMBER(currstatus,10,1) AS currstatus,
        NULL AS priornumer,
        NULL AS priordenom,
        NULL AS priorstatus,
        fiveyrnumer,
        NULL AS change,
        NULL AS statuslevel,
        NULL AS changelevel,
        NULL AS color,
        NULL AS box,
        reportingyear
    FROM state_dashboard_ca.graddownload2021
    WHERE rtype = 'S'
)
, cte_grad_state_sy21 AS (
    SELECT cds,
        rtype,
        schoolname,
        districtname,
        countyname,
        charter_flag,
        coe_flag,
        dass_flag,
        studentgroup,
        currnumer,
        currdenom,
        TRY_TO_NUMBER(currstatus,10,1) AS currstatus,
        NULL AS priornumer,
        NULL AS priordenom,
        NULL AS priorstatus,
        fiveyrnumer,
        NULL AS change,
        NULL AS statuslevel,
        NULL AS changelevel,
        NULL AS color,
        NULL AS box,
        reportingyear
    FROM state_dashboard_ca.graddownload2021
    WHERE rtype = 'X'
)
, cte_grad_authorizer_sy21 AS (
    SELECT cds,
        rtype,
        schoolname,
        districtname,
        countyname,
        charter_flag,
        coe_flag,
        dass_flag,
        studentgroup,
        currnumer,
        currdenom,
        TRY_TO_NUMBER(currstatus,10,1) AS currstatus,
        NULL AS priornumer,
        NULL AS priordenom,
        NULL AS priorstatus,
        fiveyrnumer,
        NULL AS change,
        NULL AS statuslevel,
        NULL AS changelevel,
        NULL AS color,
        NULL AS box,
        reportingyear
    FROM state_dashboard_ca.graddownload2021
    WHERE rtype = 'D'
)
, cte_grad_compare_sy21 AS (
    SELECT cte_grad_nonstate_sy21.*
        , cte_grad_state_sy21.currdenom AS currdenom_state
        , cte_grad_state_sy21.currstatus AS currstatus_state
        , cte_grad_state_sy21.priordenom AS priordenom_state
        , cte_grad_state_sy21.priorstatus AS priorstatus_state
        , cte_grad_state_sy21.change AS change_state
        , cte_grad_state_sy21.statuslevel AS statuslevel_state
        , cte_grad_state_sy21.changelevel AS changelevel_state
        , cte_grad_state_sy21.color AS color_state
        , cte_grad_state_sy21.box AS box_state
        , cte_grad_nonstate_sy21.currstatus - cte_grad_state_sy21.currstatus AS currstatus_comparison_state
        , cte_grad_nonstate_sy21.statuslevel - cte_grad_state_sy21.statuslevel AS statuslevel_comparison_state
        , cte_grad_nonstate_sy21.change - cte_grad_state_sy21.change AS change_comparison_state
        , cte_grad_nonstate_sy21.changelevel - cte_grad_state_sy21.changelevel AS changelevel_comparison_state
        , cte_grad_authorizer_sy21.currdenom AS currdenom_authorizer
        , cte_grad_authorizer_sy21.currstatus AS currstatus_authorizer
        , cte_grad_authorizer_sy21.priordenom AS priordenom_authorizer
        , cte_grad_authorizer_sy21.priorstatus AS priorstatus_authorizer
        , cte_grad_authorizer_sy21.change AS change_authorizer
        , cte_grad_authorizer_sy21.statuslevel AS statuslevel_authorizer
        , cte_grad_authorizer_sy21.changelevel AS changelevel_authorizer
        , cte_grad_authorizer_sy21.color AS color_authorizer
        , cte_grad_authorizer_sy21.box AS box_authorizer
        , cte_grad_nonstate_sy21.currstatus - cte_grad_authorizer_sy21.currstatus AS currstatus_comparison_authorizer
        , cte_grad_nonstate_sy21.statuslevel - cte_grad_authorizer_sy21.statuslevel AS statuslevel_comparison_authorizer
        , cte_grad_nonstate_sy21.change - cte_grad_authorizer_sy21.change AS change_comparison_authorizer
        , cte_grad_nonstate_sy21.changelevel - cte_grad_authorizer_sy21.changelevel AS changelevel_comparison_authorizer
    FROM cte_grad_nonstate_sy21
    LEFT JOIN cte_grad_state_sy21
        ON cte_grad_nonstate_sy21.studentgroup = cte_grad_state_sy21.studentgroup
    LEFT JOIN cte_grad_authorizer_sy21
        ON cte_grad_nonstate_sy21.studentgroup = cte_grad_authorizer_sy21.studentgroup
        AND cte_grad_nonstate_sy21.districtname = cte_grad_authorizer_sy21.districtname
)
, cte_grad_nonstate_sy20 AS (
    SELECT cds,
        rtype,
        schoolname,
        districtname,
        countyname,
        charter_flag,
        coe_flag,
        dass_flag,
        studentgroup,
        currnumer,
        currdenom,
        TRY_TO_NUMBER(currstatus,10,1) AS currstatus,
        NULL AS priornumer,
        NULL AS priordenom,
        NULL AS priorstatus,
        fiveyrnumer,
        NULL AS change,
        NULL AS statuslevel,
        NULL AS changelevel,
        NULL AS color,
        NULL AS box,
        reportingyear
    FROM state_dashboard_ca.graddownload2020
    WHERE rtype = 'S'
)
, cte_grad_state_sy20 AS (
    SELECT cds,
        rtype,
        schoolname,
        districtname,
        countyname,
        charter_flag,
        coe_flag,
        dass_flag,
        studentgroup,
        currnumer,
        currdenom,
        TRY_TO_NUMBER(currstatus,10,1) AS currstatus,
        NULL AS priornumer,
        NULL AS priordenom,
        NULL AS priorstatus,
        fiveyrnumer,
        NULL AS change,
        NULL AS statuslevel,
        NULL AS changelevel,
        NULL AS color,
        NULL AS box,
        reportingyear
    FROM state_dashboard_ca.graddownload2020
    WHERE rtype = 'X'
)
, cte_grad_authorizer_sy20 AS (
    SELECT cds,
        rtype,
        schoolname,
        districtname,
        countyname,
        charter_flag,
        coe_flag,
        dass_flag,
        studentgroup,
        currnumer,
        currdenom,
        TRY_TO_NUMBER(currstatus,10,1) AS currstatus,
        NULL AS priornumer,
        NULL AS priordenom,
        NULL AS priorstatus,
        fiveyrnumer,
        NULL AS change,
        NULL AS statuslevel,
        NULL AS changelevel,
        NULL AS color,
        NULL AS box,
        reportingyear
    FROM state_dashboard_ca.graddownload2020
    WHERE rtype = 'D'
)
, cte_grad_compare_sy20 AS (
    SELECT cte_grad_nonstate_sy20.*
        , cte_grad_state_sy20.currdenom AS currdenom_state
        , cte_grad_state_sy20.currstatus AS currstatus_state
        , cte_grad_state_sy20.priordenom AS priordenom_state
        , cte_grad_state_sy20.priorstatus AS priorstatus_state
        , cte_grad_state_sy20.change AS change_state
        , cte_grad_state_sy20.statuslevel AS statuslevel_state
        , cte_grad_state_sy20.changelevel AS changelevel_state
        , cte_grad_state_sy20.color AS color_state
        , cte_grad_state_sy20.box AS box_state
        , cte_grad_nonstate_sy20.currstatus - cte_grad_state_sy20.currstatus AS currstatus_comparison_state
        , cte_grad_nonstate_sy20.statuslevel - cte_grad_state_sy20.statuslevel AS statuslevel_comparison_state
        , cte_grad_nonstate_sy20.change - cte_grad_state_sy20.change AS change_comparison_state
        , cte_grad_nonstate_sy20.changelevel - cte_grad_state_sy20.changelevel AS changelevel_comparison_state
        , cte_grad_authorizer_sy20.currdenom AS currdenom_authorizer
        , cte_grad_authorizer_sy20.currstatus AS currstatus_authorizer
        , cte_grad_authorizer_sy20.priordenom AS priordenom_authorizer
        , cte_grad_authorizer_sy20.priorstatus AS priorstatus_authorizer
        , cte_grad_authorizer_sy20.change AS change_authorizer
        , cte_grad_authorizer_sy20.statuslevel AS statuslevel_authorizer
        , cte_grad_authorizer_sy20.changelevel AS changelevel_authorizer
        , cte_grad_authorizer_sy20.color AS color_authorizer
        , cte_grad_authorizer_sy20.box AS box_authorizer
        , cte_grad_nonstate_sy20.currstatus - cte_grad_authorizer_sy20.currstatus AS currstatus_comparison_authorizer
        , cte_grad_nonstate_sy20.statuslevel - cte_grad_authorizer_sy20.statuslevel AS statuslevel_comparison_authorizer
        , cte_grad_nonstate_sy20.change - cte_grad_authorizer_sy20.change AS change_comparison_authorizer
        , cte_grad_nonstate_sy20.changelevel - cte_grad_authorizer_sy20.changelevel AS changelevel_comparison_authorizer
    FROM cte_grad_nonstate_sy20
    LEFT JOIN cte_grad_state_sy20
        ON cte_grad_nonstate_sy20.studentgroup = cte_grad_state_sy20.studentgroup
    LEFT JOIN cte_grad_authorizer_sy20
        ON cte_grad_nonstate_sy20.studentgroup = cte_grad_authorizer_sy20.studentgroup
        AND cte_grad_nonstate_sy20.districtname = cte_grad_authorizer_sy20.districtname
)
, cte_grad_nonstate_sy19 AS (
    SELECT cds,
        rtype,
        schoolname,
        districtname,
        countyname,
        charter_flag,
        coe_flag,
        dass_flag,
        studentgroup,
        currnumer,
        currdenom,
        TRY_TO_NUMBER(currstatus,10,1) AS currstatus,
        priornumer,
        priordenom,
        priorstatus,
        fiveyrnumer,
        TRY_TO_NUMBER(change,10,1) AS change,
        TRY_TO_NUMBER(statuslevel) AS statuslevel,
        TRY_TO_NUMBER(changelevel) AS changelevel,
        TRY_TO_NUMBER(color) AS color,
        TRY_TO_NUMBER(box) AS box,
        reportingyear
    FROM state_dashboard_ca.graddownload2019
    WHERE rtype = 'S'
)
, cte_grad_state_sy19 AS (
    SELECT cds,
        rtype,
        schoolname,
        districtname,
        countyname,
        charter_flag,
        coe_flag,
        dass_flag,
        studentgroup,
        currnumer,
        currdenom,
        TRY_TO_NUMBER(currstatus,10,1) AS currstatus,
        priornumer,
        priordenom,
        priorstatus,
        fiveyrnumer,
        TRY_TO_NUMBER(change,10,1) AS change,
        TRY_TO_NUMBER(statuslevel) AS statuslevel,
        TRY_TO_NUMBER(changelevel) AS changelevel,
        TRY_TO_NUMBER(color) AS color,
        TRY_TO_NUMBER(box) AS box,
        reportingyear
    FROM state_dashboard_ca.graddownload2019
    WHERE rtype = 'X'
)
, cte_grad_authorizer_sy19 AS (
    SELECT cds,
        rtype,
        schoolname,
        districtname,
        countyname,
        charter_flag,
        coe_flag,
        dass_flag,
        studentgroup,
        currnumer,
        currdenom,
        TRY_TO_NUMBER(currstatus,10,1) AS currstatus,
        priornumer,
        priordenom,
        priorstatus,
        fiveyrnumer,
        TRY_TO_NUMBER(change,10,1) AS change,
        TRY_TO_NUMBER(statuslevel) AS statuslevel,
        TRY_TO_NUMBER(changelevel) AS changelevel,
        TRY_TO_NUMBER(color) AS color,
        TRY_TO_NUMBER(box) AS box,
        reportingyear
    FROM state_dashboard_ca.graddownload2019
    WHERE rtype = 'D'
)
, cte_grad_compare_sy19 AS (
    SELECT cte_grad_nonstate_sy19.*
        , cte_grad_state_sy19.currdenom AS currdenom_state
        , cte_grad_state_sy19.currstatus AS currstatus_state
        , cte_grad_state_sy19.priordenom AS priordenom_state
        , cte_grad_state_sy19.priorstatus AS priorstatus_state
        , cte_grad_state_sy19.change AS change_state
        , cte_grad_state_sy19.statuslevel AS statuslevel_state
        , cte_grad_state_sy19.changelevel AS changelevel_state
        , cte_grad_state_sy19.color AS color_state
        , cte_grad_state_sy19.box AS box_state
        , cte_grad_nonstate_sy19.currstatus - cte_grad_state_sy19.currstatus AS currstatus_comparison_state
        , cte_grad_nonstate_sy19.statuslevel - cte_grad_state_sy19.statuslevel AS statuslevel_comparison_state
        , cte_grad_nonstate_sy19.change - cte_grad_state_sy19.change AS change_comparison_state
        , cte_grad_nonstate_sy19.changelevel - cte_grad_state_sy19.changelevel AS changelevel_comparison_state
        , cte_grad_authorizer_sy19.currdenom AS currdenom_authorizer
        , cte_grad_authorizer_sy19.currstatus AS currstatus_authorizer
        , cte_grad_authorizer_sy19.priordenom AS priordenom_authorizer
        , cte_grad_authorizer_sy19.priorstatus AS priorstatus_authorizer
        , cte_grad_authorizer_sy19.change AS change_authorizer
        , cte_grad_authorizer_sy19.statuslevel AS statuslevel_authorizer
        , cte_grad_authorizer_sy19.changelevel AS changelevel_authorizer
        , cte_grad_authorizer_sy19.color AS color_authorizer
        , cte_grad_authorizer_sy19.box AS box_authorizer
        , cte_grad_nonstate_sy19.currstatus - cte_grad_authorizer_sy19.currstatus AS currstatus_comparison_authorizer
        , cte_grad_nonstate_sy19.statuslevel - cte_grad_authorizer_sy19.statuslevel AS statuslevel_comparison_authorizer
        , cte_grad_nonstate_sy19.change - cte_grad_authorizer_sy19.change AS change_comparison_authorizer
        , cte_grad_nonstate_sy19.changelevel - cte_grad_authorizer_sy19.changelevel AS changelevel_comparison_authorizer
    FROM cte_grad_nonstate_sy19
    LEFT JOIN cte_grad_state_sy19
        ON cte_grad_nonstate_sy19.studentgroup = cte_grad_state_sy19.studentgroup
    LEFT JOIN cte_grad_authorizer_sy19
        ON cte_grad_nonstate_sy19.studentgroup = cte_grad_authorizer_sy19.studentgroup
        AND cte_grad_nonstate_sy19.districtname = cte_grad_authorizer_sy19.districtname
)
, cte_grad_nonstate_sy18 AS (
    SELECT cds,
        rtype,
        schoolname,
        districtname,
        countyname,
        charter_flag,
        coe_flag,
        dass_flag,
        studentgroup,
        currnumer,
        currdenom,
        TRY_TO_NUMBER(currstatus,10,1) AS currstatus,
        priornumer,
        priordenom,
        priorstatus,
        NULL AS fiveyrnumer,
        TRY_TO_NUMBER(change,10,1) AS change,
        TRY_TO_NUMBER(statuslevel) AS statuslevel,
        TRY_TO_NUMBER(changelevel) AS changelevel,
        TRY_TO_NUMBER(color) AS color,
        TRY_TO_NUMBER(box) AS box,
        reportingyear
    FROM state_dashboard_ca.graddownload2018
    WHERE rtype = 'S'
)
, cte_grad_state_sy18 AS (
    SELECT cds,
        rtype,
        schoolname,
        districtname,
        countyname,
        charter_flag,
        coe_flag,
        dass_flag,
        studentgroup,
        currnumer,
        currdenom,
        TRY_TO_NUMBER(currstatus,10,1) AS currstatus,
        priornumer,
        priordenom,
        priorstatus,
        NULL AS fiveyrnumer,
        TRY_TO_NUMBER(change,10,1) AS change,
        TRY_TO_NUMBER(statuslevel) AS statuslevel,
        TRY_TO_NUMBER(changelevel) AS changelevel,
        TRY_TO_NUMBER(color) AS color,
        TRY_TO_NUMBER(box) AS box,
        reportingyear
    FROM state_dashboard_ca.graddownload2018
    WHERE rtype = 'X'
)
, cte_grad_authorizer_sy18 AS (
    SELECT cds,
        rtype,
        schoolname,
        districtname,
        countyname,
        charter_flag,
        coe_flag,
        dass_flag,
        studentgroup,
        currnumer,
        currdenom,
        TRY_TO_NUMBER(currstatus,10,1) AS currstatus,
        priornumer,
        priordenom,
        priorstatus,
        NULL AS fiveyrnumer,
        TRY_TO_NUMBER(change,10,1) AS change,
        TRY_TO_NUMBER(statuslevel) AS statuslevel,
        TRY_TO_NUMBER(changelevel) AS changelevel,
        TRY_TO_NUMBER(color) AS color,
        TRY_TO_NUMBER(box) AS box,
        reportingyear
    FROM state_dashboard_ca.graddownload2018
    WHERE rtype = 'D'
)
, cte_grad_compare_sy18 AS (
    SELECT cte_grad_nonstate_sy18.*
        , cte_grad_state_sy18.currdenom AS currdenom_state
        , cte_grad_state_sy18.currstatus AS currstatus_state
        , cte_grad_state_sy18.priordenom AS priordenom_state
        , cte_grad_state_sy18.priorstatus AS priorstatus_state
        , cte_grad_state_sy18.change AS change_state
        , cte_grad_state_sy18.statuslevel AS statuslevel_state
        , cte_grad_state_sy18.changelevel AS changelevel_state
        , cte_grad_state_sy18.color AS color_state
        , cte_grad_state_sy18.box AS box_state
        , cte_grad_nonstate_sy18.currstatus - cte_grad_state_sy18.currstatus AS currstatus_comparison_state
        , cte_grad_nonstate_sy18.statuslevel - cte_grad_state_sy18.statuslevel AS statuslevel_comparison_state
        , cte_grad_nonstate_sy18.change - cte_grad_state_sy18.change AS change_comparison_state
        , cte_grad_nonstate_sy18.changelevel - cte_grad_state_sy18.changelevel AS changelevel_comparison_state
        , cte_grad_authorizer_sy18.currdenom AS currdenom_authorizer
        , cte_grad_authorizer_sy18.currstatus AS currstatus_authorizer
        , cte_grad_authorizer_sy18.priordenom AS priordenom_authorizer
        , cte_grad_authorizer_sy18.priorstatus AS priorstatus_authorizer
        , cte_grad_authorizer_sy18.change AS change_authorizer
        , cte_grad_authorizer_sy18.statuslevel AS statuslevel_authorizer
        , cte_grad_authorizer_sy18.changelevel AS changelevel_authorizer
        , cte_grad_authorizer_sy18.color AS color_authorizer
        , cte_grad_authorizer_sy18.box AS box_authorizer
        , cte_grad_nonstate_sy18.currstatus - cte_grad_authorizer_sy18.currstatus AS currstatus_comparison_authorizer
        , cte_grad_nonstate_sy18.statuslevel - cte_grad_authorizer_sy18.statuslevel AS statuslevel_comparison_authorizer
        , cte_grad_nonstate_sy18.change - cte_grad_authorizer_sy18.change AS change_comparison_authorizer
        , cte_grad_nonstate_sy18.changelevel - cte_grad_authorizer_sy18.changelevel AS changelevel_comparison_authorizer
    FROM cte_grad_nonstate_sy18
    LEFT JOIN cte_grad_state_sy18
        ON cte_grad_nonstate_sy18.studentgroup = cte_grad_state_sy18.studentgroup
    LEFT JOIN cte_grad_authorizer_sy18
        ON cte_grad_nonstate_sy18.studentgroup = cte_grad_authorizer_sy18.studentgroup
        AND cte_grad_nonstate_sy18.districtname = cte_grad_authorizer_sy18.districtname
)

SELECT *
    , CASE WHEN schoolname LIKE '%Tahoma%' THEN 'Tahoma'
            WHEN schoolname LIKE '%Everest%' THEN 'Everest'
            WHEN schoolname LIKE '%Prep%' THEN 'Prep'
            WHEN schoolname LIKE '%Shasta%' THEN 'Shasta'
            WHEN schoolname LIKE '%K2%' THEN 'K2'
            WHEN schoolname LIKE '%Tam%' THEN 'Tam'
            ELSE NULL END AS reported_site_short_name
FROM cte_grad_compare_sy22

UNION ALL

SELECT *
    , CASE WHEN schoolname LIKE '%Tahoma%' THEN 'Tahoma'
            WHEN schoolname LIKE '%Everest%' THEN 'Everest'
            WHEN schoolname LIKE '%Prep%' THEN 'Prep'
            WHEN schoolname LIKE '%Shasta%' THEN 'Shasta'
            WHEN schoolname LIKE '%K2%' THEN 'K2'
            WHEN schoolname LIKE '%Tam%' THEN 'Tam'
            ELSE NULL END AS reported_site_short_name
FROM cte_grad_compare_sy21

UNION ALL

SELECT *
    , CASE WHEN schoolname LIKE '%Tahoma%' THEN 'Tahoma'
            WHEN schoolname LIKE '%Everest%' THEN 'Everest'
            WHEN schoolname LIKE '%Prep%' THEN 'Prep'
            WHEN schoolname LIKE '%Shasta%' THEN 'Shasta'
            WHEN schoolname LIKE '%K2%' THEN 'K2'
            WHEN schoolname LIKE '%Tam%' THEN 'Tam'
            ELSE NULL END AS reported_site_short_name
FROM cte_grad_compare_sy20

UNION ALL

SELECT *
    , CASE WHEN schoolname LIKE '%Tahoma%' THEN 'Tahoma'
            WHEN schoolname LIKE '%Everest%' THEN 'Everest'
            WHEN schoolname LIKE '%Prep%' THEN 'Prep'
            WHEN schoolname LIKE '%Shasta%' THEN 'Shasta'
            WHEN schoolname LIKE '%K2%' THEN 'K2'
            WHEN schoolname LIKE '%Tam%' THEN 'Tam'
            ELSE NULL END AS reported_site_short_name
FROM cte_grad_compare_sy19

UNION ALL

SELECT *
    , CASE WHEN schoolname LIKE '%Tahoma%' THEN 'Tahoma'
            WHEN schoolname LIKE '%Everest%' THEN 'Everest'
            WHEN schoolname LIKE '%Prep%' THEN 'Prep'
            WHEN schoolname LIKE '%Shasta%' THEN 'Shasta'
            WHEN schoolname LIKE '%K2%' THEN 'K2'
            WHEN schoolname LIKE '%Tam%' THEN 'Tam'
            ELSE NULL END AS reported_site_short_name
FROM cte_grad_compare_sy18



