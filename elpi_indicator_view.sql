CREATE OR REPLACE VIEW state_dashboard_ca.elpi_historical AS

WITH cte_elpi_nonstate_sy22 AS (
    SELECT cds,
        rtype,
        schoolname,
        districtname,
        countyname,
        charter_flag,
        coe_flag,
        dass_flag,
        currdenom,
        currnumer,
        currprogressed,
        TRY_TO_NUMBER(pctcurrprogressed,10,1) AS pctcurrprogressed,
        currmaintainpl4,
        TRY_TO_NUMBER(pctcurrmaintainpl4,10,1) AS pctcurrmaintainpl4,
        currmaintainoth,
        TRY_TO_NUMBER(pctcurrmaintainoth,10,1) AS pctcurrmaintainoth,
        currdeclined,
        TRY_TO_NUMBER(currstatus,10,1) AS currstatus,
        statuslevel,
        flag95pct,
        reportingyear
    FROM state_dashboard_ca.elpidownload2022
    WHERE rtype = 'S'
)
, cte_elpi_state_sy22 AS (
    SELECT cds,
        rtype,
        schoolname,
        districtname,
        countyname,
        charter_flag,
        coe_flag,
        dass_flag,
        currdenom,
        currnumer,
        currprogressed,
        TRY_TO_NUMBER(pctcurrprogressed,10,1) AS pctcurrprogressed,
        currmaintainpl4,
        TRY_TO_NUMBER(pctcurrmaintainpl4,10,1) AS pctcurrmaintainpl4,
        currmaintainoth,
        TRY_TO_NUMBER(pctcurrmaintainoth,10,1) AS pctcurrmaintainoth,
        currdeclined,
        TRY_TO_NUMBER(currstatus,10,1) AS currstatus,
        statuslevel,
        flag95pct,
        reportingyear
    FROM state_dashboard_ca.elpidownload2022
    WHERE rtype = 'X'
)
, cte_elpi_authorizer_sy22 AS (
    SELECT cds,
        rtype,
        schoolname,
        districtname,
        countyname,
        charter_flag,
        coe_flag,
        dass_flag,
        currdenom,
        currnumer,
        currprogressed,
        TRY_TO_NUMBER(pctcurrprogressed,10,1) AS pctcurrprogressed,
        currmaintainpl4,
        TRY_TO_NUMBER(pctcurrmaintainpl4,10,1) AS pctcurrmaintainpl4,
        currmaintainoth,
        TRY_TO_NUMBER(pctcurrmaintainoth,10,1) AS pctcurrmaintainoth,
        currdeclined,
        TRY_TO_NUMBER(currstatus,10,1) AS currstatus,
        statuslevel,
        flag95pct,
        reportingyear
    FROM state_dashboard_ca.elpidownload2022
    WHERE rtype = 'D'
)
, cte_elpi_compare_sy22 AS (
    SELECT cte_elpi_nonstate_sy22.*
        , cte_elpi_state_sy22.currstatus AS currstatus_state
        , cte_elpi_state_sy22.statuslevel AS statuslevel_state
        , cte_elpi_state_sy22.pctcurrprogressed AS pctcurrprogressed_state
        , cte_elpi_state_sy22.pctcurrmaintainpl4 AS pctcurrmaintainpl4_state
        , cte_elpi_state_sy22.pctcurrmaintainoth AS pctcurrmaintainoth_state
        , cte_elpi_nonstate_sy22.pctcurrprogressed - cte_elpi_state_sy22.pctcurrprogressed AS pctcurrprogressed_comparison_state
        , cte_elpi_nonstate_sy22.pctcurrmaintainpl4 - cte_elpi_state_sy22.pctcurrmaintainpl4 AS pctcurrmaintainpl4_comparison_state
        , cte_elpi_nonstate_sy22.pctcurrmaintainoth - cte_elpi_state_sy22.pctcurrmaintainoth AS pctcurrmaintainoth_comparison_state
        , cte_elpi_nonstate_sy22.currstatus - cte_elpi_state_sy22.currstatus AS currstatus_comparison_state
        , cte_elpi_nonstate_sy22.statuslevel - cte_elpi_state_sy22.statuslevel AS statuslevel_comparison_state
        , cte_elpi_authorizer_sy22.currstatus AS currstatus_authorizer
        , cte_elpi_authorizer_sy22.statuslevel AS statuslevel_authorizer
        , cte_elpi_authorizer_sy22.pctcurrprogressed AS pctcurrprogressed_authorizer
        , cte_elpi_authorizer_sy22.pctcurrmaintainpl4 AS pctcurrmaintainpl4_authorizer
        , cte_elpi_authorizer_sy22.pctcurrmaintainoth AS pctcurrmaintainoth_authorizer
        , cte_elpi_nonstate_sy22.pctcurrprogressed - cte_elpi_authorizer_sy22.pctcurrprogressed AS pctcurrprogressed_comparison_authorizer
        , cte_elpi_nonstate_sy22.pctcurrmaintainpl4 - cte_elpi_authorizer_sy22.pctcurrmaintainpl4 AS pctcurrmaintainpl4_comparison_authorizer
        , cte_elpi_nonstate_sy22.pctcurrmaintainoth - cte_elpi_authorizer_sy22.pctcurrmaintainoth AS pctcurrmaintainoth_comparison_authorizer
        , cte_elpi_nonstate_sy22.currstatus - cte_elpi_authorizer_sy22.currstatus AS currstatus_comparison_authorizer
        , cte_elpi_nonstate_sy22.statuslevel - cte_elpi_authorizer_sy22.statuslevel AS statuslevel_comparison_authorizer
    FROM cte_elpi_nonstate_sy22
    LEFT JOIN cte_elpi_state_sy22
    LEFT JOIN cte_elpi_authorizer_sy22
        ON cte_elpi_nonstate_sy22.districtname = cte_elpi_authorizer_sy22.districtname
)
, cte_elpi_nonstate_sy19 AS (
    SELECT cds,
        rtype,
        schoolname,
        districtname,
        countyname,
        charter_flag,
        coe_flag,
        dass_flag,
        currdenom,
        currnumer,
        currprogressed,
        ROUND((currprogressed / currdenom)*100,1) AS pctcurrprogressed,
        currmaintainpl4,
        ROUND((currmaintainpl4 / currdenom)*100,1) AS pctcurrmaintainpl4,
        currmaintainoth,
        ROUND((currmaintainoth / currdenom)*100,1) AS pctcurrmaintainoth,
        currdeclined,
        currstatus,
        statuslevel,
        flag95pct,
        reportingyear
    FROM state_dashboard_ca.elpidownload2019
    WHERE rtype = 'S'
)
, cte_elpi_state_sy19 AS (
    SELECT cds,
        rtype,
        schoolname,
        districtname,
        countyname,
        charter_flag,
        coe_flag,
        dass_flag,
        currdenom,
        currnumer,
        currprogressed,
        ROUND((currprogressed / currdenom)*100,1) AS pctcurrprogressed,
        currmaintainpl4,
        ROUND((currmaintainpl4 / currdenom)*100,1) AS pctcurrmaintainpl4,
        currmaintainoth,
        ROUND((currmaintainoth / currdenom)*100,1) AS pctcurrmaintainoth,
        currdeclined,
        currstatus,
        statuslevel,
        flag95pct,
        reportingyear
    FROM state_dashboard_ca.elpidownload2019
    WHERE rtype = 'X'
)
, cte_elpi_authorizer_sy19 AS (
    SELECT cds,
        rtype,
        schoolname,
        districtname,
        countyname,
        charter_flag,
        coe_flag,
        dass_flag,
        currdenom,
        currnumer,
        currprogressed,
        ROUND((currprogressed / currdenom)*100,1) AS pctcurrprogressed,
        currmaintainpl4,
        ROUND((currmaintainpl4 / currdenom)*100,1) AS pctcurrmaintainpl4,
        currmaintainoth,
        ROUND((currmaintainoth / currdenom)*100,1) AS pctcurrmaintainoth,
        currdeclined,
        currstatus,
        statuslevel,
        flag95pct,
        reportingyear
    FROM state_dashboard_ca.elpidownload2019
    WHERE rtype = 'D'
)
, cte_elpi_compare_sy19 AS (
    SELECT cte_elpi_nonstate_sy19.*
        , cte_elpi_state_sy19.currstatus AS currstatus_state
        , cte_elpi_state_sy19.statuslevel AS statuslevel_state
        , cte_elpi_state_sy19.pctcurrprogressed AS pctcurrprogressed_state
        , cte_elpi_state_sy19.pctcurrmaintainpl4 AS pctcurrmaintainpl4_state
        , cte_elpi_state_sy19.pctcurrmaintainoth AS pctcurrmaintainoth_state
        , cte_elpi_nonstate_sy19.pctcurrprogressed - cte_elpi_state_sy19.pctcurrprogressed AS pctcurrprogressed_comparison_state
        , cte_elpi_nonstate_sy19.pctcurrmaintainpl4 - cte_elpi_state_sy19.pctcurrmaintainpl4 AS pctcurrmaintainpl4_comparison_state
        , cte_elpi_nonstate_sy19.pctcurrmaintainoth - cte_elpi_state_sy19.pctcurrmaintainoth AS pctcurrmaintainoth_comparison_state
        , cte_elpi_nonstate_sy19.currstatus - cte_elpi_state_sy19.currstatus AS currstatus_comparison_state
        , cte_elpi_nonstate_sy19.statuslevel - cte_elpi_state_sy19.statuslevel AS statuslevel_comparison_state
        , cte_elpi_authorizer_sy19.currstatus AS currstatus_authorizer
        , cte_elpi_authorizer_sy19.statuslevel AS statuslevel_authorizer
        , cte_elpi_authorizer_sy19.pctcurrprogressed AS pctcurrprogressed_authorizer
        , cte_elpi_authorizer_sy19.pctcurrmaintainpl4 AS pctcurrmaintainpl4_authorizer
        , cte_elpi_authorizer_sy19.pctcurrmaintainoth AS pctcurrmaintainoth_authorizer
        , cte_elpi_nonstate_sy19.pctcurrprogressed - cte_elpi_authorizer_sy19.pctcurrprogressed AS pctcurrprogressed_comparison_authorizer
        , cte_elpi_nonstate_sy19.pctcurrmaintainpl4 - cte_elpi_authorizer_sy19.pctcurrmaintainpl4 AS pctcurrmaintainpl4_comparison_authorizer
        , cte_elpi_nonstate_sy19.pctcurrmaintainoth - cte_elpi_authorizer_sy19.pctcurrmaintainoth AS pctcurrmaintainoth_comparison_authorizer
        , cte_elpi_nonstate_sy19.currstatus - cte_elpi_authorizer_sy19.currstatus AS currstatus_comparison_authorizer
        , cte_elpi_nonstate_sy19.statuslevel - cte_elpi_authorizer_sy19.statuslevel AS statuslevel_comparison_authorizer
    FROM cte_elpi_nonstate_sy19
    LEFT JOIN cte_elpi_state_sy19
    LEFT JOIN cte_elpi_authorizer_sy19
        ON cte_elpi_nonstate_sy19.districtname = cte_elpi_authorizer_sy19.districtname
)

SELECT *
    , CASE WHEN schoolname LIKE '%Tahoma%' THEN 'Tahoma'
            WHEN schoolname LIKE '%Everest%' THEN 'Everest'
            WHEN schoolname LIKE '%Prep%' THEN 'Prep'
            WHEN schoolname LIKE '%Shasta%' THEN 'Shasta'
            WHEN schoolname LIKE '%K2%' THEN 'K2'
            WHEN schoolname LIKE '%Tam%' THEN 'Tam'
            ELSE NULL END AS reported_site_short_name
FROM cte_elpi_compare_sy22

UNION ALL

SELECT *
    , CASE WHEN schoolname LIKE '%Tahoma%' THEN 'Tahoma'
            WHEN schoolname LIKE '%Everest%' THEN 'Everest'
            WHEN schoolname LIKE '%Prep%' THEN 'Prep'
            WHEN schoolname LIKE '%Shasta%' THEN 'Shasta'
            WHEN schoolname LIKE '%K2%' THEN 'K2'
            WHEN schoolname LIKE '%Tam%' THEN 'Tam'
            ELSE NULL END AS reported_site_short_name
FROM cte_elpi_compare_sy19
