CREATE OR REPLACE VIEW state_dashboard_ca.absenteeism_historical AS
    
WITH cte_absenteeism_summit_sy22 AS (
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
           TRY_TO_NUMBER(currstatus) AS currstatus,
           null AS priornumer,
           null AS priordenom,
           null AS priorstatus,
           null AS change,
           null AS safetynet,
           statuslevel::INT as statuslevel,
           null AS changelevel,
           null AS color,
           null AS box,
           certifyflag,
           dataerrorflag,
           reportingyear::INT as reportingyear
    FROM state_dashboard_ca.chronicdownload2022
    WHERE rtype = 'S'
      AND cds != 'nan'
)

, cte_absenteeism_authorizer_sy22 AS (
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
           TRY_TO_NUMBER(currstatus) AS currstatus,
           null AS priornumer,
           null AS priordenom,
           null AS priorstatus,
           null AS change,
           null AS safetynet,
           statuslevel::INT as statuslevel,
           null AS changelevel,
           null AS color,
           null AS box,
           certifyflag,
           dataerrorflag,
           reportingyear::INT as reportingyear
    FROM state_dashboard_ca.chronicdownload2022
    WHERE rtype = 'D'
      AND cds != 'nan'
)

, cte_absenteeism_state_sy22 AS (
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
           TRY_TO_NUMBER(currstatus) AS currstatus,
           null AS priornumer,
           null AS priordenom,
           null AS priorstatus,
           null AS change,
           null AS safetynet,
           statuslevel::INT as statuslevel,
           null AS changelevel,
           null AS color,
           null AS box,
           certifyflag,
           dataerrorflag,
           reportingyear::INT as reportingyear
    FROM state_dashboard_ca.chronicdownload2022
    WHERE rtype = 'X' and cds != 'nan'
)

, cte_absenteeism_compare_sy22 AS (
    SELECT summit.*,
           state.currnumer AS currnumer_state,
           state.currdenom AS currdenom_state,
           state.currstatus AS currstatus_state,
           state.priornumer AS priornumer_state,
           state.priordenom AS priordenom_state,
           state.priorstatus AS priorstatus_state,
           state.change AS change_state,
           state.safetynet AS safetynet_state,
           state.statuslevel AS statuslevel_state,
           state.changelevel AS changelevel_state,
           state.color AS color_state,
           state.box AS box_state,
           state.certifyflag AS certifyflag_state,
           state.dataerrorflag as dataerrorflag_state,
           authorizer.currnumer AS currnumer_authorizer,
           authorizer.currdenom AS currdenom_authorizer,
           authorizer.currstatus AS currstatus_authorizer,
           authorizer.priornumer AS priornumer_authorizer,
           authorizer.priordenom AS priordenom_authorizer,
           authorizer.priorstatus AS priorstatus_authorizer,
           authorizer.change AS change_authorizer,
           authorizer.safetynet AS safetynet_authorizer,
           authorizer.statuslevel AS statuslevel_authorizer,
           authorizer.changelevel AS changelevel_authorizer,
           authorizer.color AS color_authorizer,
           authorizer.box AS box_authorizer,
           authorizer.certifyflag AS certifyflag_authorizer,
           authorizer.dataerrorflag as dataerrorflag_authorizer,
           summit.currstatus - state.currstatus AS currstatus_comparison_state,
           summit.statuslevel - state.statuslevel AS statuslevel_comparison_state,
           summit.change - state.change AS change_comparison_state,
           summit.changelevel - state.changelevel AS changelevel_comparison_state,
           summit.currstatus - authorizer.currstatus AS currstatus_comparison_authorizer,
           summit.statuslevel - authorizer.statuslevel AS statuslevel_comparison_authorizer,
           summit.change - authorizer.change AS change_comparison_authorizer,
           summit.changelevel - authorizer.changelevel AS changelevel_comparison_authorizer
    FROM cte_absenteeism_summit_sy22 summit
    LEFT JOIN cte_absenteeism_state_sy22 state
        ON summit.studentgroup = state.studentgroup
    LEFT JOIN cte_absenteeism_authorizer_sy22 authorizer
        ON summit.districtname = authorizer.districtname
        AND summit.studentgroup = authorizer.studentgroup
)

, cte_absenteeism_summit_sy19 AS (
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
           TRY_TO_NUMBER(currstatus) AS currstatus,
           priornumer,
           priordenom,
           priorstatus,
           TRY_TO_NUMBER(change) AS change,
           safetynet,
           statuslevel::INT as statuslevel,
           changelevel::INT as changelevel,
           color,
           box,
           certifyflag,
           dataerrorflag,
           reportingyear::INT as reportingyear
    FROM state_dashboard_ca.chronicdownload2019
    WHERE rtype = 'S'
      AND cds != 'nan'
)

, cte_absenteeism_authorizer_sy19 AS (
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
           TRY_TO_NUMBER(currstatus) AS currstatus,
           priornumer,
           priordenom,
           priorstatus,
           TRY_TO_NUMBER(change) AS change,
           safetynet,
           statuslevel::INT as statuslevel,
           changelevel::INT as changelevel,
           color,
           box,
           certifyflag,
           dataerrorflag,
           reportingyear::INT as reportingyear
    FROM state_dashboard_ca.chronicdownload2019
    WHERE rtype = 'D'
      AND cds != 'nan'
)

, cte_absenteeism_state_sy19 AS (
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
           TRY_TO_NUMBER(currstatus) AS currstatus,
           priornumer,
           priordenom,
           priorstatus,
           TRY_TO_NUMBER(change) AS change,
           safetynet,
           statuslevel::INT as statuslevel,
           changelevel::INT as changelevel,
           color,
           box,
           certifyflag,
           dataerrorflag,
           reportingyear::INT as reportingyear
    FROM state_dashboard_ca.chronicdownload2019
    WHERE rtype = 'X' and cds != 'nan'
)

, cte_absenteeism_compare_sy19 AS (
    SELECT summit.*,
           state.currnumer AS currnumer_state,
           state.currdenom AS currdenom_state,
           state.currstatus AS currstatus_state,
           state.priornumer AS priornumer_state,
           state.priordenom AS priordenom_state,
           state.priorstatus AS priorstatus_state,
           state.change AS change_state,
           state.safetynet AS safetynet_state,
           state.statuslevel AS statuslevel_state,
           state.changelevel AS changelevel_state,
           state.color AS color_state,
           state.box AS box_state,
           state.certifyflag AS certifyflag_state,
           state.dataerrorflag as dataerrorflag_state,
           authorizer.currnumer AS currnumer_authorizer,
           authorizer.currdenom AS currdenom_authorizer,
           authorizer.currstatus AS currstatus_authorizer,
           authorizer.priornumer AS priornumer_authorizer,
           authorizer.priordenom AS priordenom_authorizer,
           authorizer.priorstatus AS priorstatus_authorizer,
           authorizer.change AS change_authorizer,
           authorizer.safetynet AS safetynet_authorizer,
           authorizer.statuslevel AS statuslevel_authorizer,
           authorizer.changelevel AS changelevel_authorizer,
           authorizer.color AS color_authorizer,
           authorizer.box AS box_authorizer,
           authorizer.certifyflag AS certifyflag_authorizer,
           authorizer.dataerrorflag as dataerrorflag_authorizer,
           summit.currstatus - state.currstatus AS currstatus_comparison_state,
           summit.statuslevel - state.statuslevel AS statuslevel_comparison_state,
           summit.change - state.change AS change_comparison_state,
           summit.changelevel - state.changelevel AS changelevel_comparison_state,
           summit.currstatus - authorizer.currstatus AS currstatus_comparison_authorizer,
           summit.statuslevel - authorizer.statuslevel AS statuslevel_comparison_authorizer,
           summit.change - authorizer.change AS change_comparison_authorizer,
           summit.changelevel - authorizer.changelevel AS changelevel_comparison_authorizer
    FROM cte_absenteeism_summit_sy19 summit
    LEFT JOIN cte_absenteeism_state_sy19 state
        ON summit.studentgroup = state.studentgroup
    LEFT JOIN cte_absenteeism_authorizer_sy19 authorizer
        ON summit.districtname = authorizer.districtname
        AND summit.studentgroup = authorizer.studentgroup
)


, cte_absenteeism_summit_sy18 AS (
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
           TRY_TO_NUMBER(currstatus) AS currstatus,
           priornumer,
           priordenom,
           priorstatus,
           TRY_TO_NUMBER(change) AS change,
           safetynet,
           statuslevel::INT as statuslevel,
           changelevel::INT as changelevel,
           color,
           box,
           certifyflag,
           null AS dataerrorflag,
           reportingyear::INT as reportingyear
    FROM state_dashboard_ca.chronicdownload2018
    WHERE rtype = 'S'
      AND cds != 'nan'
)

, cte_absenteeism_authorizer_sy18 AS (
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
           TRY_TO_NUMBER(currstatus) AS currstatus,
           priornumer,
           priordenom,
           priorstatus,
           TRY_TO_NUMBER(change) AS change,
           safetynet,
           statuslevel::INT as statuslevel,
           changelevel::INT as changelevel,
           color,
           box,
           certifyflag,
           null AS dataerrorflag,
           reportingyear::INT as reportingyear
    FROM state_dashboard_ca.chronicdownload2018
    WHERE rtype = 'D'
      AND cds != 'nan'
)

, cte_absenteeism_state_sy18 AS (
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
           TRY_TO_NUMBER(currstatus) AS currstatus,
           priornumer,
           priordenom,
           priorstatus,
           TRY_TO_NUMBER(change) AS change,
           safetynet,
           statuslevel::INT as statuslevel,
           changelevel::INT as changelevel,
           color,
           box,
           certifyflag,
           null AS dataerrorflag,
           reportingyear::INT as reportingyear
    FROM state_dashboard_ca.chronicdownload2018
    WHERE rtype = 'X' and cds != 'nan'
)

, cte_absenteeism_compare_sy18 AS (
    SELECT summit.*,
           state.currnumer AS currnumer_state,
           state.currdenom AS currdenom_state,
           state.currstatus AS currstatus_state,
           state.priornumer AS priornumer_state,
           state.priordenom AS priordenom_state,
           state.priorstatus AS priorstatus_state,
           state.change AS change_state,
           state.safetynet AS safetynet_state,
           state.statuslevel AS statuslevel_state,
           state.changelevel AS changelevel_state,
           state.color AS color_state,
           state.box AS box_state,
           state.certifyflag AS certifyflag_state,
           state.dataerrorflag as dataerrorflag_state,
           authorizer.currnumer AS currnumer_authorizer,
           authorizer.currdenom AS currdenom_authorizer,
           authorizer.currstatus AS currstatus_authorizer,
           authorizer.priornumer AS priornumer_authorizer,
           authorizer.priordenom AS priordenom_authorizer,
           authorizer.priorstatus AS priorstatus_authorizer,
           authorizer.change AS change_authorizer,
           authorizer.safetynet AS safetynet_authorizer,
           authorizer.statuslevel AS statuslevel_authorizer,
           authorizer.changelevel AS changelevel_authorizer,
           authorizer.color AS color_authorizer,
           authorizer.box AS box_authorizer,
           authorizer.certifyflag AS certifyflag_authorizer,
           authorizer.dataerrorflag as dataerrorflag_authorizer,
           summit.currstatus - state.currstatus AS currstatus_comparison_state,
           summit.statuslevel - state.statuslevel AS statuslevel_comparison_state,
           summit.change - state.change AS change_comparison_state,
           summit.changelevel - state.changelevel AS changelevel_comparison_state,
           summit.currstatus - authorizer.currstatus AS currstatus_comparison_authorizer,
           summit.statuslevel - authorizer.statuslevel AS statuslevel_comparison_authorizer,
           summit.change - authorizer.change AS change_comparison_authorizer,
           summit.changelevel - authorizer.changelevel AS changelevel_comparison_authorizer
    FROM cte_absenteeism_summit_sy18 summit
    LEFT JOIN cte_absenteeism_state_sy18 state
        ON summit.studentgroup = state.studentgroup
    LEFT JOIN cte_absenteeism_authorizer_sy18 authorizer
        ON summit.districtname = authorizer.districtname
        AND summit.studentgroup = authorizer.studentgroup
)

SELECT * ,
       CASE WHEN schoolname LIKE '%Tahoma%' THEN 'Tahoma'
            WHEN schoolname LIKE '%Everest%' THEN 'Everest'
            WHEN schoolname LIKE '%Prep%' THEN 'Prep'
            WHEN schoolname LIKE '%Shasta%' THEN 'Shasta'
            WHEN schoolname LIKE '%K2%' THEN 'K2'
            WHEN schoolname LIKE '%Tam%' THEN 'Tam'
            ELSE NULL END AS reported_site_short_name
FROM cte_absenteeism_compare_sy22

UNION ALL

SELECT * ,
       CASE WHEN schoolname LIKE '%Tahoma%' THEN 'Tahoma'
            WHEN schoolname LIKE '%Everest%' THEN 'Everest'
            WHEN schoolname LIKE '%Prep%' THEN 'Prep'
            WHEN schoolname LIKE '%Shasta%' THEN 'Shasta'
            WHEN schoolname LIKE '%K2%' THEN 'K2'
            WHEN schoolname LIKE '%Tam%' THEN 'Tam'
            ELSE NULL END AS reported_site_short_name
FROM cte_absenteeism_compare_sy19

UNION ALL

SELECT * ,
       CASE WHEN schoolname LIKE '%Tahoma%' THEN 'Tahoma'
            WHEN schoolname LIKE '%Everest%' THEN 'Everest'
            WHEN schoolname LIKE '%Prep%' THEN 'Prep'
            WHEN schoolname LIKE '%Shasta%' THEN 'Shasta'
            WHEN schoolname LIKE '%K2%' THEN 'K2'
            WHEN schoolname LIKE '%Tam%' THEN 'Tam'
            ELSE NULL END AS reported_site_short_name
FROM cte_absenteeism_compare_sy18
