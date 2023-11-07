CREATE OR REPLACE VIEW state_dashboard_ca.cci_historical AS

WITH cte_cci_nonstate_sy22 AS (
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
        NULL AS currstatus,
        NULL AS priordenom,
        NULL AS priorstatus,
        NULL AS change,
        NULL AS statuslevel,
        NULL AS changelevel,
        NULL AS color,
        NULL AS box,
        NULL AS prep,
        NULL AS prep_pct,
        NULL AS prep_summative,
        NULL AS prep_summative_pct,
        ap AS prep_ap,
        TRY_TO_NUMBER(ap_pct,10,1) AS prep_ap_pct,
        ib AS prep_ib,
        TRY_TO_NUMBER(ib_pct,10,1) AS prep_ib_pct,
        ag AS prep_ag,
        TRY_TO_NUMBER(ag_pct,10,1) AS prep_ag_pct,
        cte AS prep_cte,
        TRY_TO_NUMBER(cte_pct,10,1) AS prep_cte_pct,
        ag_cte AS prep_ag_cte,
        TRY_TO_NUMBER(ag_cte_pct,10,1) AS prep_ag_cte_pct,
        one_college_course AS prep_one_college_course,
        TRY_TO_NUMBER(one_college_course_pct,10,1) AS prep_one_college_course_pct,
        two_college_course AS prep_two_college_course,
        TRY_TO_NUMBER(two_college_course_pct,10,1) AS prep_two_college_course_pct,
        one_milsci_course AS prep_one_milsci_course,
        TRY_TO_NUMBER(one_milsci_course_pct,10,1) AS prep_one_milsci_course_pct,
        two_milsci_course AS prep_two_milsci_course,
        TRY_TO_NUMBER(two_milsci_course_pct,10,1) AS prep_two_milsci_course_pct,
        non_reg_pre AS prep_non_reg_pre,
        TRY_TO_NUMBER(non_reg_pre_pct,10,1) AS prep_non_reg_pre_pct,
        reg_pre AS prep_reg_pre,
        TRY_TO_NUMBER(reg_pre_pct,10,1) AS prep_reg_pre_pct,
        statefedjobs AS prep_statefedjobs,
        TRY_TO_NUMBER(statefedjobs_pct,10,1) AS prep_statefedjobs_pct,
        ssb AS prep_ssb,
        TRY_TO_NUMBER(ssb_pct,10,1) AS prep_ssb_pct,
        tcbwe AS prep_tcbwe,
        TRY_TO_NUMBER(tcbwe_pct,10,1) AS prep_tcbwe_pct,
        twbe AS prep_twbe,
        TRY_TO_NUMBER(twbe_pct,10,1) AS prep_twbe_pct,
        tcbwe_and_twbe AS prep_tcbwe_and_twbe,
        TRY_TO_NUMBER(tcbwe_and_twbe_pct,10,1) AS prep_tcbwe_and_twbe_pct,
        NULL AS aprep,
        NULL AS aprep_pct,
        NULL AS aprep_summative,
        NULL AS aprep_summative_pct,
        NULL AS aprep_ap,
        NULL AS aprep_ap_pct,
        NULL AS aprep_ib,
        NULL AS aprep_ib_pct,
        NULL AS aprep_ag,
        NULL AS aprep_ag_pct,
        NULL AS aprep_cte,
        NULL AS aprep_cte_pct,
        NULL AS aprep_ag_cte,
        NULL AS aprep_ag_cte_pct,
        NULL AS aprep_one_college_course,
        NULL AS aprep_one_college_course_pct,
        NULL AS aprep_two_college_course,
        NULL AS aprep_two_college_course_pct,
        NULL AS aprep_one_milsci_course,
        NULL AS aprep_one_milsci_course_pct,
        NULL AS aprep_two_milsci_course,
        NULL AS aprep_two_milsci_course_pct,
        NULL AS aprep_non_reg_pre,
        NULL AS aprep_non_reg_pre_pct,
        NULL AS aprep_reg_pre,
        NULL AS aprep_reg_pre_pct,
        NULL AS aprep_statefedjobs,
        NULL AS aprep_statefedjobs_pct,
        NULL AS aprep_ssb,
        NULL AS aprep_ssb_pct,
        NULL AS aprep_tcbwe,
        NULL AS aprep_tcbwe_pct,
        NULL AS aprep_twbe,
        NULL AS aprep_twbe_pct,
        NULL AS aprep_tcbwe_and_twbe,
        NULL AS aprep_tcbwe_and_twbe_pct,
        reportingyear
    FROM state_dashboard_ca.ccidownload2022
    WHERE rtype != 'X'
)
,cte_cci_state_sy22 AS (
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
        NULL AS currstatus,
        NULL AS priordenom,
        NULL AS priorstatus,
        NULL AS change,
        NULL AS statuslevel,
        NULL AS changelevel,
        NULL AS color,
        NULL AS box,
        NULL AS prep,
        NULL AS prep_pct,
        NULL AS prep_summative,
        NULL AS prep_summative_pct,
        ap AS prep_ap,
        TRY_TO_NUMBER(ap_pct,10,1) AS prep_ap_pct,
        ib AS prep_ib,
        TRY_TO_NUMBER(ib_pct,10,1) AS prep_ib_pct,
        ag AS prep_ag,
        TRY_TO_NUMBER(ag_pct,10,1) AS prep_ag_pct,
        cte AS prep_cte,
        TRY_TO_NUMBER(cte_pct,10,1) AS prep_cte_pct,
        ag_cte AS prep_ag_cte,
        TRY_TO_NUMBER(ag_cte_pct,10,1) AS prep_ag_cte_pct,
        one_college_course AS prep_one_college_course,
        TRY_TO_NUMBER(one_college_course_pct,10,1) AS prep_one_college_course_pct,
        two_college_course AS prep_two_college_course,
        TRY_TO_NUMBER(two_college_course_pct,10,1) AS prep_two_college_course_pct,
        one_milsci_course AS prep_one_milsci_course,
        TRY_TO_NUMBER(one_milsci_course_pct,10,1) AS prep_one_milsci_course_pct,
        two_milsci_course AS prep_two_milsci_course,
        TRY_TO_NUMBER(two_milsci_course_pct,10,1) AS prep_two_milsci_course_pct,
        non_reg_pre AS prep_non_reg_pre,
        TRY_TO_NUMBER(non_reg_pre_pct,10,1) AS prep_non_reg_pre_pct,
        reg_pre AS prep_reg_pre,
        TRY_TO_NUMBER(reg_pre_pct,10,1) AS prep_reg_pre_pct,
        statefedjobs AS prep_statefedjobs,
        TRY_TO_NUMBER(statefedjobs_pct,10,1) AS prep_statefedjobs_pct,
        ssb AS prep_ssb,
        TRY_TO_NUMBER(ssb_pct,10,1) AS prep_ssb_pct,
        tcbwe AS prep_tcbwe,
        TRY_TO_NUMBER(tcbwe_pct,10,1) AS prep_tcbwe_pct,
        twbe AS prep_twbe,
        TRY_TO_NUMBER(twbe_pct,10,1) AS prep_twbe_pct,
        tcbwe_and_twbe AS prep_tcbwe_and_twbe,
        TRY_TO_NUMBER(tcbwe_and_twbe_pct,10,1) AS prep_tcbwe_and_twbe_pct,
        NULL AS aprep,
        NULL AS aprep_pct,
        NULL AS aprep_summative,
        NULL AS aprep_summative_pct,
        NULL AS aprep_ap,
        NULL AS aprep_ap_pct,
        NULL AS aprep_ib,
        NULL AS aprep_ib_pct,
        NULL AS aprep_ag,
        NULL AS aprep_ag_pct,
        NULL AS aprep_cte,
        NULL AS aprep_cte_pct,
        NULL AS aprep_ag_cte,
        NULL AS aprep_ag_cte_pct,
        NULL AS aprep_one_college_course,
        NULL AS aprep_one_college_course_pct,
        NULL AS aprep_two_college_course,
        NULL AS aprep_two_college_course_pct,
        NULL AS aprep_one_milsci_course,
        NULL AS aprep_one_milsci_course_pct,
        NULL AS aprep_two_milsci_course,
        NULL AS aprep_two_milsci_course_pct,
        NULL AS aprep_non_reg_pre,
        NULL AS aprep_non_reg_pre_pct,
        NULL AS aprep_reg_pre,
        NULL AS aprep_reg_pre_pct,
        NULL AS aprep_statefedjobs,
        NULL AS aprep_statefedjobs_pct,
        NULL AS aprep_ssb,
        NULL AS aprep_ssb_pct,
        NULL AS aprep_tcbwe,
        NULL AS aprep_tcbwe_pct,
        NULL AS aprep_twbe,
        NULL AS aprep_twbe_pct,
        NULL AS aprep_tcbwe_and_twbe,
        NULL AS aprep_tcbwe_and_twbe_pct,
        reportingyear
    FROM state_dashboard_ca.ccidownload2022
    WHERE rtype = 'X'
)
, cte_cci_state_compare_sy22 AS (
    SELECT cte_cci_nonstate_sy22.*
        , cte_cci_state_sy22.prep_pct AS prep_pct_state
        , cte_cci_state_sy22.prep_ap_pct AS prep_ap_pct_state
        , cte_cci_state_sy22.prep_ib_pct AS prep_ib_pct_state
        , cte_cci_state_sy22.prep_ag_pct AS prep_ag_pct_state
        , cte_cci_state_sy22.prep_cte_pct AS prep_cte_pct_state
        , cte_cci_state_sy22.prep_ag_cte_pct AS prep_ag_cte_pct_state
        , cte_cci_state_sy22.prep_one_college_course_pct AS prep_one_college_course_pct_state
        , cte_cci_state_sy22.prep_two_college_course_pct AS prep_two_college_course_pct_state
        , cte_cci_state_sy22.prep_one_milsci_course_pct AS prep_one_milsci_course_pct_state
        , cte_cci_state_sy22.prep_two_milsci_course_pct AS prep_two_milsci_course_pct_state
        , cte_cci_state_sy22.prep_non_reg_pre_pct AS prep_non_reg_pre_pct_state
        , cte_cci_state_sy22.prep_reg_pre_pct AS prep_reg_pre_pct_state
        , cte_cci_state_sy22.prep_statefedjobs_pct AS prep_statefedjobs_pct_state
        , cte_cci_state_sy22.prep_ssb_pct AS prep_ssb_pct_state
        , cte_cci_state_sy22.prep_tcbwe_pct AS prep_tcbwe_pct_state
        , cte_cci_state_sy22.prep_twbe_pct AS prep_twbe_pct_state
        , cte_cci_state_sy22.prep_tcbwe_and_twbe_pct AS prep_tcbwe_and_twbe_pct_state
        , cte_cci_nonstate_sy22.prep_pct - cte_cci_state_sy22.prep_pct AS prep_comparison
        , cte_cci_nonstate_sy22.prep_ap_pct - cte_cci_state_sy22.prep_ap_pct AS prep_ap_comparison
        , cte_cci_nonstate_sy22.prep_ib_pct - cte_cci_state_sy22.prep_ib_pct AS prep_ib_comparison
        , cte_cci_nonstate_sy22.prep_ag_pct - cte_cci_state_sy22.prep_ag_pct AS prep_ag_comparison
        , cte_cci_nonstate_sy22.prep_cte_pct - cte_cci_state_sy22.prep_cte_pct AS prep_cte_comparison
        , cte_cci_nonstate_sy22.prep_ag_cte_pct - cte_cci_state_sy22.prep_ag_cte_pct AS prep_ag_cte_comparison
        , cte_cci_nonstate_sy22.prep_one_college_course_pct - cte_cci_state_sy22.prep_one_college_course_pct AS prep_one_college_course_comparison
        , cte_cci_nonstate_sy22.prep_two_college_course_pct - cte_cci_state_sy22.prep_two_college_course_pct AS prep_two_college_course_comparison
        , cte_cci_nonstate_sy22.prep_one_milsci_course_pct - cte_cci_state_sy22.prep_one_milsci_course_pct AS prep_one_milsci_course_comparison
        , cte_cci_nonstate_sy22.prep_two_milsci_course_pct - cte_cci_state_sy22.prep_two_milsci_course_pct AS prep_two_milsci_course_comparison
        , cte_cci_nonstate_sy22.prep_non_reg_pre_pct - cte_cci_state_sy22.prep_non_reg_pre_pct AS prep_non_reg_pre_comparison
        , cte_cci_nonstate_sy22.prep_reg_pre_pct - cte_cci_state_sy22.prep_reg_pre_pct AS prep_reg_pre_comparison
        , cte_cci_nonstate_sy22.prep_statefedjobs_pct - cte_cci_state_sy22.prep_statefedjobs_pct AS prep_statefedjobs_comparison
        , cte_cci_nonstate_sy22.prep_ssb_pct - cte_cci_state_sy22.prep_ssb_pct AS prep_ssb_comparison
        , cte_cci_nonstate_sy22.prep_tcbwe_pct - cte_cci_state_sy22.prep_tcbwe_pct AS prep_tcbwe_comparison
        , cte_cci_nonstate_sy22.prep_twbe_pct - cte_cci_state_sy22.prep_twbe_pct AS prep_twbe_comparison
        , cte_cci_nonstate_sy22.prep_tcbwe_and_twbe_pct - cte_cci_state_sy22.prep_tcbwe_and_twbe_pct AS prep_tcbwe_and_twbe_comparison
        , cte_cci_nonstate_sy22.aprep_pct - cte_cci_state_sy22.aprep_pct AS aprep_comparison
        , cte_cci_nonstate_sy22.aprep_ap_pct - cte_cci_state_sy22.aprep_ap_pct AS aprep_ap_comparison
        , cte_cci_nonstate_sy22.aprep_ib_pct - cte_cci_state_sy22.aprep_ib_pct AS aprep_ib_comparison
        , cte_cci_nonstate_sy22.aprep_ag_pct - cte_cci_state_sy22.aprep_ag_pct AS aprep_ag_comparison
        , cte_cci_nonstate_sy22.aprep_cte_pct - cte_cci_state_sy22.aprep_cte_pct AS aprep_cte_comparison
        , cte_cci_nonstate_sy22.aprep_ag_cte_pct - cte_cci_state_sy22.aprep_ag_cte_pct AS aprep_ag_cte_comparison
        , cte_cci_nonstate_sy22.aprep_one_college_course_pct - cte_cci_state_sy22.aprep_one_college_course_pct AS parep_one_college_course_comparison
        , cte_cci_nonstate_sy22.aprep_two_college_course_pct - cte_cci_state_sy22.aprep_two_college_course_pct AS aprep_two_college_course_comparison
        , cte_cci_nonstate_sy22.aprep_one_milsci_course_pct - cte_cci_state_sy22.aprep_one_milsci_course_pct AS aprep_one_milsci_course_comparison
        , cte_cci_nonstate_sy22.aprep_two_milsci_course_pct - cte_cci_state_sy22.aprep_two_milsci_course_pct AS aprep_two_milsci_course_comparison
        , cte_cci_nonstate_sy22.aprep_non_reg_pre_pct - cte_cci_state_sy22.aprep_non_reg_pre_pct AS aprep_non_reg_pre_comparison
        , cte_cci_nonstate_sy22.aprep_reg_pre_pct - cte_cci_state_sy22.aprep_reg_pre_pct AS aprep_reg_pre_comparison
        , cte_cci_nonstate_sy22.aprep_statefedjobs_pct - cte_cci_state_sy22.aprep_statefedjobs_pct AS aprep_statefedjobs_comparison
        , cte_cci_nonstate_sy22.aprep_ssb_pct - cte_cci_state_sy22.aprep_ssb_pct AS aprep_ssb_comparison
        , cte_cci_nonstate_sy22.aprep_tcbwe_pct - cte_cci_state_sy22.aprep_tcbwe_pct AS aprep_tcbwe_comparison
        , cte_cci_nonstate_sy22.aprep_twbe_pct - cte_cci_state_sy22.aprep_twbe_pct AS aprep_twbe_comparison
        , cte_cci_nonstate_sy22.aprep_tcbwe_and_twbe_pct - cte_cci_state_sy22.aprep_tcbwe_and_twbe_pct AS aprep_tcbwe_and_twbe_comparison
        , cte_cci_nonstate_sy22.currstatus - cte_cci_state_sy22.currstatus AS currstatus_comparison
        , cte_cci_nonstate_sy22.statuslevel - cte_cci_state_sy22.statuslevel AS statuslevel_comparison
        , cte_cci_nonstate_sy22.change - cte_cci_state_sy22.change AS change_comparison
        , cte_cci_nonstate_sy22.changelevel - cte_cci_state_sy22.changelevel AS changelevel_comparison
    FROM cte_cci_nonstate_sy22
    LEFT JOIN cte_cci_state_sy22
        ON cte_cci_nonstate_sy22.studentgroup = cte_cci_state_sy22.studentgroup
)
, cte_cci_nonstate_sy21 AS (
    SELECT cds,
        rtype,
        schoolname,
        districtname,
        countyname,
        charter_flag,
        coe_flag,
        dass_flag,
        studentgroup,
        currdenom_4Y_5Y_1Y AS currdenom,
        NULL AS currstatus,
        NULL AS priordenom,
        NULL AS priorstatus,
        NULL AS change,
        NULL AS statuslevel,
        NULL AS changelevel,
        NULL AS color,
        NULL AS box,
        NULL AS prep,
        NULL AS prep_pct,
        NULL AS prep_summative,
        NULL AS prep_summative_pct,
        ap AS prep_ap,
        TRY_TO_NUMBER(ap_pct,10,1) AS prep_ap_pct,
        ib AS prep_ib,
        TRY_TO_NUMBER(ib_pct,10,1) AS prep_ib_pct,
        ag AS prep_ag,
        TRY_TO_NUMBER(ag_pct,10,1) AS prep_ag_pct,
        cte AS prep_cte,
        TRY_TO_NUMBER(cte_pct,10,1) AS prep_cte_pct,
        ag_cte AS prep_ag_cte,
        TRY_TO_NUMBER(ag_cte_pct,10,1) AS prep_ag_cte_pct,
        one_college_course AS prep_one_college_course,
        TRY_TO_NUMBER(one_college_course_pct,10,1) AS prep_one_college_course_pct,
        two_college_courses AS prep_two_college_course,
        TRY_TO_NUMBER(two_college_courses_pct,10,1) AS prep_two_college_course_pct,
        NULL AS prep_one_milsci_course,
        NULL AS prep_one_milsci_course_pct,
        NULL AS prep_two_milsci_course,
        NULL AS prep_two_milsci_course_pct,
        NULL AS prep_non_reg_pre,
        NULL AS prep_non_reg_pre_pct,
        NULL AS prep_reg_pre,
        NULL AS prep_reg_pre_pct,
        NULL AS prep_statefedjobs,
        NULL AS prep_statefedjobs_pct,
        NULL AS prep_ssb,
        NULL AS prep_ssb_pct,
        NULL AS prep_tcbwe,
        NULL AS prep_tcbwe_pct,
        NULL AS prep_twbe,
        NULL AS prep_twbe_pct,
        NULL AS prep_tcbwe_and_twbe,
        NULL AS prep_tcbwe_and_twbe_pct,
        NULL AS aprep,
        NULL AS aprep_pct,
        NULL AS aprep_summative,
        NULL AS aprep_summative_pct,
        NULL AS aprep_ap,
        NULL AS aprep_ap_pct,
        NULL AS aprep_ib,
        NULL AS aprep_ib_pct,
        NULL AS aprep_ag,
        NULL AS aprep_ag_pct,
        NULL AS aprep_cte,
        NULL AS aprep_cte_pct,
        NULL AS aprep_ag_cte,
        NULL AS aprep_ag_cte_pct,
        NULL AS aprep_one_college_course,
        NULL AS aprep_one_college_course_pct,
        NULL AS aprep_two_college_course,
        NULL AS aprep_two_college_course_pct,
        NULL AS aprep_one_milsci_course,
        NULL AS aprep_one_milsci_course_pct,
        NULL AS aprep_two_milsci_course,
        NULL AS aprep_two_milsci_course_pct,
        NULL AS aprep_non_reg_pre,
        NULL AS aprep_non_reg_pre_pct,
        NULL AS aprep_reg_pre,
        NULL AS aprep_reg_pre_pct,
        NULL AS aprep_statefedjobs,
        NULL AS aprep_statefedjobs_pct,
        NULL AS aprep_ssb,
        NULL AS aprep_ssb_pct,
        NULL AS aprep_tcbwe,
        NULL AS aprep_tcbwe_pct,
        NULL AS aprep_twbe,
        NULL AS aprep_twbe_pct,
        NULL AS aprep_tcbwe_and_twbe,
        NULL AS aprep_tcbwe_and_twbe_pct,
        reportingyear
    FROM state_dashboard_ca.ccidownload2021
    WHERE rtype != 'X'
)
, cte_cci_state_sy21 AS (
    SELECT cds,
        rtype,
        schoolname,
        districtname,
        countyname,
        charter_flag,
        coe_flag,
        dass_flag,
        studentgroup,
        currdenom_4Y_5Y_1Y AS currdenom,
        NULL AS currstatus,
        NULL AS priordenom,
        NULL AS priorstatus,
        NULL AS change,
        NULL AS statuslevel,
        NULL AS changelevel,
        NULL AS color,
        NULL AS box,
        NULL AS prep,
        NULL AS prep_pct,
        NULL AS prep_summative,
        NULL AS prep_summative_pct,
        ap AS prep_ap,
        TRY_TO_NUMBER(ap_pct,10,1) AS prep_ap_pct,
        ib AS prep_ib,
        TRY_TO_NUMBER(ib_pct,10,1) AS prep_ib_pct,
        ag AS prep_ag,
        TRY_TO_NUMBER(ag_pct,10,1) AS prep_ag_pct,
        cte AS prep_cte,
        TRY_TO_NUMBER(cte_pct,10,1) AS prep_cte_pct,
        ag_cte AS prep_ag_cte,
        TRY_TO_NUMBER(ag_cte_pct,10,1) AS prep_ag_cte_pct,
        one_college_course AS prep_one_college_course,
        TRY_TO_NUMBER(one_college_course_pct,10,1) AS prep_one_college_course_pct,
        two_college_courses AS prep_two_college_course,
        TRY_TO_NUMBER(two_college_courses_pct,10,1) AS prep_two_college_course_pct,
        NULL AS prep_one_milsci_course,
        NULL AS prep_one_milsci_course_pct,
        NULL AS prep_two_milsci_course,
        NULL AS prep_two_milsci_course_pct,
        NULL AS prep_non_reg_pre,
        NULL AS prep_non_reg_pre_pct,
        NULL AS prep_reg_pre,
        NULL AS prep_reg_pre_pct,
        NULL AS prep_statefedjobs,
        NULL AS prep_statefedjobs_pct,
        NULL AS prep_ssb,
        NULL AS prep_ssb_pct,
        NULL AS prep_tcbwe,
        NULL AS prep_tcbwe_pct,
        NULL AS prep_twbe,
        NULL AS prep_twbe_pct,
        NULL AS prep_tcbwe_and_twbe,
        NULL AS prep_tcbwe_and_twbe_pct,
        NULL AS aprep,
        NULL AS aprep_pct,
        NULL AS aprep_summative,
        NULL AS aprep_summative_pct,
        NULL AS aprep_ap,
        NULL AS aprep_ap_pct,
        NULL AS aprep_ib,
        NULL AS aprep_ib_pct,
        NULL AS aprep_ag,
        NULL AS aprep_ag_pct,
        NULL AS aprep_cte,
        NULL AS aprep_cte_pct,
        NULL AS aprep_ag_cte,
        NULL AS aprep_ag_cte_pct,
        NULL AS aprep_one_college_course,
        NULL AS aprep_one_college_course_pct,
        NULL AS aprep_two_college_course,
        NULL AS aprep_two_college_course_pct,
        NULL AS aprep_one_milsci_course,
        NULL AS aprep_one_milsci_course_pct,
        NULL AS aprep_two_milsci_course,
        NULL AS aprep_two_milsci_course_pct,
        NULL AS aprep_non_reg_pre,
        NULL AS aprep_non_reg_pre_pct,
        NULL AS aprep_reg_pre,
        NULL AS aprep_reg_pre_pct,
        NULL AS aprep_statefedjobs,
        NULL AS aprep_statefedjobs_pct,
        NULL AS aprep_ssb,
        NULL AS aprep_ssb_pct,
        NULL AS aprep_tcbwe,
        NULL AS aprep_tcbwe_pct,
        NULL AS aprep_twbe,
        NULL AS aprep_twbe_pct,
        NULL AS aprep_tcbwe_and_twbe,
        NULL AS aprep_tcbwe_and_twbe_pct,
        reportingyear
    FROM state_dashboard_ca.ccidownload2021
    WHERE rtype = 'X'
)
, cte_cci_state_compare_sy21 AS (
    SELECT cte_cci_nonstate_sy21.*
        , cte_cci_state_sy21.prep_pct AS prep_pct_state
        , cte_cci_state_sy21.prep_ap_pct AS prep_ap_pct_state
        , cte_cci_state_sy21.prep_ib_pct AS prep_ib_pct_state
        , cte_cci_state_sy21.prep_ag_pct AS prep_ag_pct_state
        , cte_cci_state_sy21.prep_cte_pct AS prep_cte_pct_state
        , cte_cci_state_sy21.prep_ag_cte_pct AS prep_ag_cte_pct_state
        , cte_cci_state_sy21.prep_one_college_course_pct AS prep_one_college_course_pct_state
        , cte_cci_state_sy21.prep_two_college_course_pct AS prep_two_college_course_pct_state
        , cte_cci_state_sy21.prep_one_milsci_course_pct AS prep_one_milsci_course_pct_state
        , cte_cci_state_sy21.prep_two_milsci_course_pct AS prep_two_milsci_course_pct_state
        , cte_cci_state_sy21.prep_non_reg_pre_pct AS prep_non_reg_pre_pct_state
        , cte_cci_state_sy21.prep_reg_pre_pct AS prep_reg_pre_pct_state
        , cte_cci_state_sy21.prep_statefedjobs_pct AS prep_statefedjobs_pct_state
        , cte_cci_state_sy21.prep_ssb_pct AS prep_ssb_pct_state
        , cte_cci_state_sy21.prep_tcbwe_pct AS prep_tcbwe_pct_state
        , cte_cci_state_sy21.prep_twbe_pct AS prep_twbe_pct_state
        , cte_cci_state_sy21.prep_tcbwe_and_twbe_pct AS prep_tcbwe_and_twbe_pct_state
        , cte_cci_nonstate_sy21.prep_pct - cte_cci_state_sy21.prep_pct AS prep_comparison
        , cte_cci_nonstate_sy21.prep_ap_pct - cte_cci_state_sy21.prep_ap_pct AS prep_ap_comparison
        , cte_cci_nonstate_sy21.prep_ib_pct - cte_cci_state_sy21.prep_ib_pct AS prep_ib_comparison
        , cte_cci_nonstate_sy21.prep_ag_pct - cte_cci_state_sy21.prep_ag_pct AS prep_ag_comparison
        , cte_cci_nonstate_sy21.prep_cte_pct - cte_cci_state_sy21.prep_cte_pct AS prep_cte_comparison
        , cte_cci_nonstate_sy21.prep_ag_cte_pct - cte_cci_state_sy21.prep_ag_cte_pct AS prep_ag_cte_comparison
        , cte_cci_nonstate_sy21.prep_one_college_course_pct - cte_cci_state_sy21.prep_one_college_course_pct AS prep_one_college_course_comparison
        , cte_cci_nonstate_sy21.prep_two_college_course_pct - cte_cci_state_sy21.prep_two_college_course_pct AS prep_two_college_course_comparison
        , cte_cci_nonstate_sy21.prep_one_milsci_course_pct - cte_cci_state_sy21.prep_one_milsci_course_pct AS prep_one_milsci_course_comparison
        , cte_cci_nonstate_sy21.prep_two_milsci_course_pct - cte_cci_state_sy21.prep_two_milsci_course_pct AS prep_two_milsci_course_comparison
        , cte_cci_nonstate_sy21.prep_non_reg_pre_pct - cte_cci_state_sy21.prep_non_reg_pre_pct AS prep_non_reg_pre_comparison
        , cte_cci_nonstate_sy21.prep_reg_pre_pct - cte_cci_state_sy21.prep_reg_pre_pct AS prep_reg_pre_comparison
        , cte_cci_nonstate_sy21.prep_statefedjobs_pct - cte_cci_state_sy21.prep_statefedjobs_pct AS prep_statefedjobs_comparison
        , cte_cci_nonstate_sy21.prep_ssb_pct - cte_cci_state_sy21.prep_ssb_pct AS prep_ssb_comparison
        , cte_cci_nonstate_sy21.prep_tcbwe_pct - cte_cci_state_sy21.prep_tcbwe_pct AS prep_tcbwe_comparison
        , cte_cci_nonstate_sy21.prep_twbe_pct - cte_cci_state_sy21.prep_twbe_pct AS prep_twbe_comparison
        , cte_cci_nonstate_sy21.prep_tcbwe_and_twbe_pct - cte_cci_state_sy21.prep_tcbwe_and_twbe_pct AS prep_tcbwe_and_twbe_comparison
        , cte_cci_nonstate_sy21.aprep_pct - cte_cci_state_sy21.aprep_pct AS aprep_comparison
        , cte_cci_nonstate_sy21.aprep_ap_pct - cte_cci_state_sy21.aprep_ap_pct AS aprep_ap_comparison
        , cte_cci_nonstate_sy21.aprep_ib_pct - cte_cci_state_sy21.aprep_ib_pct AS aprep_ib_comparison
        , cte_cci_nonstate_sy21.aprep_ag_pct - cte_cci_state_sy21.aprep_ag_pct AS aprep_ag_comparison
        , cte_cci_nonstate_sy21.aprep_cte_pct - cte_cci_state_sy21.aprep_cte_pct AS aprep_cte_comparison
        , cte_cci_nonstate_sy21.aprep_ag_cte_pct - cte_cci_state_sy21.aprep_ag_cte_pct AS aprep_ag_cte_comparison
        , cte_cci_nonstate_sy21.aprep_one_college_course_pct - cte_cci_state_sy21.aprep_one_college_course_pct AS parep_one_college_course_comparison
        , cte_cci_nonstate_sy21.aprep_two_college_course_pct - cte_cci_state_sy21.aprep_two_college_course_pct AS aprep_two_college_course_comparison
        , cte_cci_nonstate_sy21.aprep_one_milsci_course_pct - cte_cci_state_sy21.aprep_one_milsci_course_pct AS aprep_one_milsci_course_comparison
        , cte_cci_nonstate_sy21.aprep_two_milsci_course_pct - cte_cci_state_sy21.aprep_two_milsci_course_pct AS aprep_two_milsci_course_comparison
        , cte_cci_nonstate_sy21.aprep_non_reg_pre_pct - cte_cci_state_sy21.aprep_non_reg_pre_pct AS aprep_non_reg_pre_comparison
        , cte_cci_nonstate_sy21.aprep_reg_pre_pct - cte_cci_state_sy21.aprep_reg_pre_pct AS aprep_reg_pre_comparison
        , cte_cci_nonstate_sy21.aprep_statefedjobs_pct - cte_cci_state_sy21.aprep_statefedjobs_pct AS aprep_statefedjobs_comparison
        , cte_cci_nonstate_sy21.aprep_ssb_pct - cte_cci_state_sy21.aprep_ssb_pct AS aprep_ssb_comparison
        , cte_cci_nonstate_sy21.aprep_tcbwe_pct - cte_cci_state_sy21.aprep_tcbwe_pct AS aprep_tcbwe_comparison
        , cte_cci_nonstate_sy21.aprep_twbe_pct - cte_cci_state_sy21.aprep_twbe_pct AS aprep_twbe_comparison
        , cte_cci_nonstate_sy21.aprep_tcbwe_and_twbe_pct - cte_cci_state_sy21.aprep_tcbwe_and_twbe_pct AS aprep_tcbwe_and_twbe_comparison
        , cte_cci_nonstate_sy21.currstatus - cte_cci_state_sy21.currstatus AS currstatus_comparison
        , cte_cci_nonstate_sy21.statuslevel - cte_cci_state_sy21.statuslevel AS statuslevel_comparison
        , cte_cci_nonstate_sy21.change - cte_cci_state_sy21.change AS change_comparison
        , cte_cci_nonstate_sy21.changelevel - cte_cci_state_sy21.changelevel AS changelevel_comparison
    FROM cte_cci_nonstate_sy21
    LEFT JOIN cte_cci_state_sy21
        ON cte_cci_nonstate_sy21.studentgroup = cte_cci_state_sy21.studentgroup
)
, cte_cci_nonstate_sy20 AS (
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
        NULL AS currstatus,
        NULL AS priordenom,
        NULL AS priorstatus,
        NULL AS change,
        NULL AS statuslevel,
        NULL AS changelevel,
        NULL AS color,
        NULL AS box,
        curr_prep AS prep,
        TRY_TO_NUMBER(curr_prep_pct,10,1) AS prep_pct,
        curr_prep_summative AS prep_summative,
        TRY_TO_NUMBER(curr_prep_summative_pct,10,1) AS prep_summative_pct,
        curr_prep_apexam AS prep_ap,
        TRY_TO_NUMBER(curr_prep_apexam_pct,10,1) AS prep_ap_pct,
        curr_prep_ibexam AS prep_ib,
        TRY_TO_NUMBER(curr_prep_ibexam_pct,10,1) AS prep_ib_pct,
        curr_prep_agplus AS prep_ag,
        TRY_TO_NUMBER(curr_prep_agplus_pct,10,1) AS prep_ag_pct,
        curr_prep_cteplus AS prep_cte,
        TRY_TO_NUMBER(curr_prep_cteplus_pct,10,1) AS prep_cte_pct,
        NULL AS prep_ag_cte,
        NULL AS prep_ag_cte_pct,
        curr_prep_collegecredit AS prep_one_college_course,
        TRY_TO_NUMBER(curr_prep_collegecredit_pct,10,1) AS prep_one_college_course_pct,
        NULL AS prep_two_college_course,
        NULL AS prep_two_college_course_pct,
        curr_prep_milsci AS prep_one_milsci_course,
        TRY_TO_NUMBER(curr_prep_milsci_pct,10,1) AS prep_one_milsci_course_pct,
        NULL AS prep_two_milsci_course,
        NULL AS prep_two_milsci_course_pct,
        curr_prep_non_reg_pre AS prep_non_reg_pre,
        TRY_TO_NUMBER(curr_prep_non_reg_pre_pct,10,1) AS prep_non_reg_pre_pct,
        curr_prep_reg_pre AS prep_reg_pre,
        TRY_TO_NUMBER(curr_prep_reg_pre_pct,10,1) AS prep_reg_pre_pct,
        curr_prep_statefedjobs_dass AS prep_statefedjobs,
        TRY_TO_NUMBER(curr_prep_statefedjobs_dass_pct,10,1) AS prep_statefedjobs_pct,
        curr_prep_ssb AS prep_ssb,
        TRY_TO_NUMBER(curr_prep_ssb_pct,10,1) AS prep_ssb_pct,
        NULL AS prep_tcbwe,
        NULL AS prep_tcbwe_pct,
        NULL AS prep_twbe,
        NULL AS prep_twbe_pct,
        curr_prep_trans_classwk AS prep_tcbwe_and_twbe,
        TRY_TO_NUMBER(curr_prep_trans_classwk_pct,10,1) AS prep_tcbwe_and_twbe_pct,
        curr_aprep AS aprep,
        TRY_TO_NUMBER(curr_aprep_pct,10,1) AS aprep_pct,
        curr_aprep_summative AS aprep_summative,
        TRY_TO_NUMBER(curr_aprep_summative_pct,10,1) AS aprep_summative_pct,
        NULL AS aprep_ap,
        NULL AS aprep_ap_pct,
        NULL AS aprep_ib,
        NULL AS aprep_ib_pct,
        curr_aprep_ag AS aprep_ag,
        TRY_TO_NUMBER(curr_aprep_ag_pct,10,1) AS aprep_ag_pct,
        curr_aprep_cte AS aprep_cte,
        TRY_TO_NUMBER(curr_aprep_cte_pct,10,1) AS aprep_cte_pct,
        NULL AS aprep_ag_cte,
        NULL AS aprep_ag_cte_pct,
        curr_aprep_collegecredit AS aprep_one_college_course,
        TRY_TO_NUMBER(curr_aprep_collegecredit_pct,10,1) AS aprep_one_college_course_pct,
        NULL AS aprep_two_college_course,
        NULL AS aprep_two_college_course_pct,
        curr_aprep_milsci AS aprep_one_milsci_course,
        TRY_TO_NUMBER(curr_aprep_milsci_pct,10,1) AS aprep_one_milsci_course_pct,
        NULL AS aprep_two_milsci_course,
        NULL AS aprep_two_milsci_course_pct,
        curr_aprep_non_reg_pre AS aprep_non_reg_pre,
        TRY_TO_NUMBER(curr_aprep_non_reg_pre_pct,10,1) AS aprep_non_reg_pre_pct,
        NULL AS aprep_reg_pre,
        NULL AS aprep_reg_pre_pct,
        curr_aprep_statefedjobs_dass AS aprep_statefedjobs,
        TRY_TO_NUMBER(curr_aprep_statefedjobs_dass_pct,10,1) AS aprep_statefedjobs_pct,
        NULL AS aprep_ssb,
        NULL AS aprep_ssb_pct,
        NULL AS aprep_tcbwe,
        NULL AS aprep_tcbwe_pct,
        NULL AS aprep_twbe,
        NULL AS aprep_twbe_pct,
        curr_aprep_trans_classwk AS aprep_tcbwe_and_twbe,
        TRY_TO_NUMBER(curr_aprep_trans_classwk_pct,10,1) AS aprep_tcbwe_and_twbe_pct,
        reportingyear
    FROM state_dashboard_ca.ccidownload2020
    WHERE rtype != 'X'
)
, cte_cci_state_sy20 AS (
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
        NULL AS currstatus,
        NULL AS priordenom,
        NULL AS priorstatus,
        NULL AS change,
        NULL AS statuslevel,
        NULL AS changelevel,
        NULL AS color,
        NULL AS box,
        curr_prep AS prep,
        TRY_TO_NUMBER(curr_prep_pct,10,1) AS prep_pct,
        curr_prep_summative AS prep_summative,
        TRY_TO_NUMBER(curr_prep_summative_pct,10,1) AS prep_summative_pct,
        curr_prep_apexam AS prep_ap,
        TRY_TO_NUMBER(curr_prep_apexam_pct,10,1) AS prep_ap_pct,
        curr_prep_ibexam AS prep_ib,
        TRY_TO_NUMBER(curr_prep_ibexam_pct,10,1) AS prep_ib_pct,
        curr_prep_agplus AS prep_ag,
        TRY_TO_NUMBER(curr_prep_agplus_pct,10,1) AS prep_ag_pct,
        curr_prep_cteplus AS prep_cte,
        TRY_TO_NUMBER(curr_prep_cteplus_pct,10,1) AS prep_cte_pct,
        NULL AS prep_ag_cte,
        NULL AS prep_ag_cte_pct,
        curr_prep_collegecredit AS prep_one_college_course,
        TRY_TO_NUMBER(curr_prep_collegecredit_pct,10,1) AS prep_one_college_course_pct,
        NULL AS prep_two_college_course,
        NULL AS prep_two_college_course_pct,
        curr_prep_milsci AS prep_one_milsci_course,
        TRY_TO_NUMBER(curr_prep_milsci_pct,10,1) AS prep_one_milsci_course_pct,
        NULL AS prep_two_milsci_course,
        NULL AS prep_two_milsci_course_pct,
        curr_prep_non_reg_pre AS prep_non_reg_pre,
        TRY_TO_NUMBER(curr_prep_non_reg_pre_pct,10,1) AS prep_non_reg_pre_pct,
        curr_prep_reg_pre AS prep_reg_pre,
        TRY_TO_NUMBER(curr_prep_reg_pre_pct,10,1) AS prep_reg_pre_pct,
        curr_prep_statefedjobs_dass AS prep_statefedjobs,
        TRY_TO_NUMBER(curr_prep_statefedjobs_dass_pct,10,1) AS prep_statefedjobs_pct,
        curr_prep_ssb AS prep_ssb,
        TRY_TO_NUMBER(curr_prep_ssb_pct,10,1) AS prep_ssb_pct,
        NULL AS prep_tcbwe,
        NULL AS prep_tcbwe_pct,
        NULL AS prep_twbe,
        NULL AS prep_twbe_pct,
        curr_prep_trans_classwk AS prep_tcbwe_and_twbe,
        TRY_TO_NUMBER(curr_prep_trans_classwk_pct,10,1) AS prep_tcbwe_and_twbe_pct,
        curr_aprep AS aprep,
        TRY_TO_NUMBER(curr_aprep_pct,10,1) AS aprep_pct,
        curr_aprep_summative AS aprep_summative,
        TRY_TO_NUMBER(curr_aprep_summative_pct,10,1) AS aprep_summative_pct,
        NULL AS aprep_ap,
        NULL AS aprep_ap_pct,
        NULL AS aprep_ib,
        NULL AS aprep_ib_pct,
        curr_aprep_ag AS aprep_ag,
        TRY_TO_NUMBER(curr_aprep_ag_pct,10,1) AS aprep_ag_pct,
        curr_aprep_cte AS aprep_cte,
        TRY_TO_NUMBER(curr_aprep_cte_pct,10,1) AS aprep_cte_pct,
        NULL AS aprep_ag_cte,
        NULL AS aprep_ag_cte_pct,
        curr_aprep_collegecredit AS aprep_one_college_course,
        TRY_TO_NUMBER(curr_aprep_collegecredit_pct,10,1) AS aprep_one_college_course_pct,
        NULL AS aprep_two_college_course,
        NULL AS aprep_two_college_course_pct,
        curr_aprep_milsci AS aprep_one_milsci_course,
        TRY_TO_NUMBER(curr_aprep_milsci_pct,10,1) AS aprep_one_milsci_course_pct,
        NULL AS aprep_two_milsci_course,
        NULL AS aprep_two_milsci_course_pct,
        curr_aprep_non_reg_pre AS aprep_non_reg_pre,
        TRY_TO_NUMBER(curr_aprep_non_reg_pre_pct,10,1) AS aprep_non_reg_pre_pct,
        NULL AS aprep_reg_pre,
        NULL AS aprep_reg_pre_pct,
        curr_aprep_statefedjobs_dass AS aprep_statefedjobs,
        TRY_TO_NUMBER(curr_aprep_statefedjobs_dass_pct,10,1) AS aprep_statefedjobs_pct,
        NULL AS aprep_ssb,
        NULL AS aprep_ssb_pct,
        NULL AS aprep_tcbwe,
        NULL AS aprep_tcbwe_pct,
        NULL AS aprep_twbe,
        NULL AS aprep_twbe_pct,
        curr_aprep_trans_classwk AS aprep_tcbwe_and_twbe,
        TRY_TO_NUMBER(curr_aprep_trans_classwk_pct,10,1) AS aprep_tcbwe_and_twbe_pct,
        reportingyear
    FROM state_dashboard_ca.ccidownload2020
    WHERE rtype = 'X'
)
, cte_cci_state_compare_sy20 AS (
    SELECT cte_cci_nonstate_sy20.*
        , cte_cci_state_sy20.prep_pct AS prep_pct_state
        , cte_cci_state_sy20.prep_ap_pct AS prep_ap_pct_state
        , cte_cci_state_sy20.prep_ib_pct AS prep_ib_pct_state
        , cte_cci_state_sy20.prep_ag_pct AS prep_ag_pct_state
        , cte_cci_state_sy20.prep_cte_pct AS prep_cte_pct_state
        , cte_cci_state_sy20.prep_ag_cte_pct AS prep_ag_cte_pct_state
        , cte_cci_state_sy20.prep_one_college_course_pct AS prep_one_college_course_pct_state
        , cte_cci_state_sy20.prep_two_college_course_pct AS prep_two_college_course_pct_state
        , cte_cci_state_sy20.prep_one_milsci_course_pct AS prep_one_milsci_course_pct_state
        , cte_cci_state_sy20.prep_two_milsci_course_pct AS prep_two_milsci_course_pct_state
        , cte_cci_state_sy20.prep_non_reg_pre_pct AS prep_non_reg_pre_pct_state
        , cte_cci_state_sy20.prep_reg_pre_pct AS prep_reg_pre_pct_state
        , cte_cci_state_sy20.prep_statefedjobs_pct AS prep_statefedjobs_pct_state
        , cte_cci_state_sy20.prep_ssb_pct AS prep_ssb_pct_state
        , cte_cci_state_sy20.prep_tcbwe_pct AS prep_tcbwe_pct_state
        , cte_cci_state_sy20.prep_twbe_pct AS prep_twbe_pct_state
        , cte_cci_state_sy20.prep_tcbwe_and_twbe_pct AS prep_tcbwe_and_twbe_pct_state
        , cte_cci_nonstate_sy20.prep_pct - cte_cci_state_sy20.prep_pct AS prep_comparison
        , cte_cci_nonstate_sy20.prep_ap_pct - cte_cci_state_sy20.prep_ap_pct AS prep_ap_comparison
        , cte_cci_nonstate_sy20.prep_ib_pct - cte_cci_state_sy20.prep_ib_pct AS prep_ib_comparison
        , cte_cci_nonstate_sy20.prep_ag_pct - cte_cci_state_sy20.prep_ag_pct AS prep_ag_comparison
        , cte_cci_nonstate_sy20.prep_cte_pct - cte_cci_state_sy20.prep_cte_pct AS prep_cte_comparison
        , cte_cci_nonstate_sy20.prep_ag_cte_pct - cte_cci_state_sy20.prep_ag_cte_pct AS prep_ag_cte_comparison
        , cte_cci_nonstate_sy20.prep_one_college_course_pct - cte_cci_state_sy20.prep_one_college_course_pct AS prep_one_college_course_comparison
        , cte_cci_nonstate_sy20.prep_two_college_course_pct - cte_cci_state_sy20.prep_two_college_course_pct AS prep_two_college_course_comparison
        , cte_cci_nonstate_sy20.prep_one_milsci_course_pct - cte_cci_state_sy20.prep_one_milsci_course_pct AS prep_one_milsci_course_comparison
        , cte_cci_nonstate_sy20.prep_two_milsci_course_pct - cte_cci_state_sy20.prep_two_milsci_course_pct AS prep_two_milsci_course_comparison
        , cte_cci_nonstate_sy20.prep_non_reg_pre_pct - cte_cci_state_sy20.prep_non_reg_pre_pct AS prep_non_reg_pre_comparison
        , cte_cci_nonstate_sy20.prep_reg_pre_pct - cte_cci_state_sy20.prep_reg_pre_pct AS prep_reg_pre_comparison
        , cte_cci_nonstate_sy20.prep_statefedjobs_pct - cte_cci_state_sy20.prep_statefedjobs_pct AS prep_statefedjobs_comparison
        , cte_cci_nonstate_sy20.prep_ssb_pct - cte_cci_state_sy20.prep_ssb_pct AS prep_ssb_comparison
        , cte_cci_nonstate_sy20.prep_tcbwe_pct - cte_cci_state_sy20.prep_tcbwe_pct AS prep_tcbwe_comparison
        , cte_cci_nonstate_sy20.prep_twbe_pct - cte_cci_state_sy20.prep_twbe_pct AS prep_twbe_comparison
        , cte_cci_nonstate_sy20.prep_tcbwe_and_twbe_pct - cte_cci_state_sy20.prep_tcbwe_and_twbe_pct AS prep_tcbwe_and_twbe_comparison
        , cte_cci_nonstate_sy20.aprep_pct - cte_cci_state_sy20.aprep_pct AS aprep_comparison
        , cte_cci_nonstate_sy20.aprep_ap_pct - cte_cci_state_sy20.aprep_ap_pct AS aprep_ap_comparison
        , cte_cci_nonstate_sy20.aprep_ib_pct - cte_cci_state_sy20.aprep_ib_pct AS aprep_ib_comparison
        , cte_cci_nonstate_sy20.aprep_ag_pct - cte_cci_state_sy20.aprep_ag_pct AS aprep_ag_comparison
        , cte_cci_nonstate_sy20.aprep_cte_pct - cte_cci_state_sy20.aprep_cte_pct AS aprep_cte_comparison
        , cte_cci_nonstate_sy20.aprep_ag_cte_pct - cte_cci_state_sy20.aprep_ag_cte_pct AS aprep_ag_cte_comparison
        , cte_cci_nonstate_sy20.aprep_one_college_course_pct - cte_cci_state_sy20.aprep_one_college_course_pct AS parep_one_college_course_comparison
        , cte_cci_nonstate_sy20.aprep_two_college_course_pct - cte_cci_state_sy20.aprep_two_college_course_pct AS aprep_two_college_course_comparison
        , cte_cci_nonstate_sy20.aprep_one_milsci_course_pct - cte_cci_state_sy20.aprep_one_milsci_course_pct AS aprep_one_milsci_course_comparison
        , cte_cci_nonstate_sy20.aprep_two_milsci_course_pct - cte_cci_state_sy20.aprep_two_milsci_course_pct AS aprep_two_milsci_course_comparison
        , cte_cci_nonstate_sy20.aprep_non_reg_pre_pct - cte_cci_state_sy20.aprep_non_reg_pre_pct AS aprep_non_reg_pre_comparison
        , cte_cci_nonstate_sy20.aprep_reg_pre_pct - cte_cci_state_sy20.aprep_reg_pre_pct AS aprep_reg_pre_comparison
        , cte_cci_nonstate_sy20.aprep_statefedjobs_pct - cte_cci_state_sy20.aprep_statefedjobs_pct AS aprep_statefedjobs_comparison
        , cte_cci_nonstate_sy20.aprep_ssb_pct - cte_cci_state_sy20.aprep_ssb_pct AS aprep_ssb_comparison
        , cte_cci_nonstate_sy20.aprep_tcbwe_pct - cte_cci_state_sy20.aprep_tcbwe_pct AS aprep_tcbwe_comparison
        , cte_cci_nonstate_sy20.aprep_twbe_pct - cte_cci_state_sy20.aprep_twbe_pct AS aprep_twbe_comparison
        , cte_cci_nonstate_sy20.aprep_tcbwe_and_twbe_pct - cte_cci_state_sy20.aprep_tcbwe_and_twbe_pct AS aprep_tcbwe_and_twbe_comparison
        , cte_cci_nonstate_sy20.currstatus - cte_cci_state_sy20.currstatus AS currstatus_comparison
        , cte_cci_nonstate_sy20.statuslevel - cte_cci_state_sy20.statuslevel AS statuslevel_comparison
        , cte_cci_nonstate_sy20.change - cte_cci_state_sy20.change AS change_comparison
        , cte_cci_nonstate_sy20.changelevel - cte_cci_state_sy20.changelevel AS changelevel_comparison
    FROM cte_cci_nonstate_sy20
    LEFT JOIN cte_cci_state_sy20
        ON cte_cci_nonstate_sy20.studentgroup = cte_cci_state_sy20.studentgroup
)
, cte_cci_nonstate_sy19 AS (
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
        priordenom,
        TRY_TO_NUMBER(priorstatus,10,1) AS priorstatus,
        TRY_TO_NUMBER(change,10,1) AS change,
        statuslevel,
        changelevel,
        color,
        box,
        curr_prep AS prep,
        TRY_TO_NUMBER(curr_prep_pct,10,1) AS prep_pct,
        curr_prep_summative AS prep_summative,
        TRY_TO_NUMBER(curr_prep_summative_pct,10,1) AS prep_summative_pct,
        curr_prep_apexam AS prep_ap,
        TRY_TO_NUMBER(curr_prep_apexam_pct,10,1) AS prep_ap_pct,
        curr_prep_ibexam AS prep_ib,
        TRY_TO_NUMBER(curr_prep_ibexam_pct,10,1) AS prep_ib_pct,
        curr_prep_agplus AS prep_ag,
        TRY_TO_NUMBER(curr_prep_agplus_pct,10,1) AS prep_ag_pct,
        curr_prep_cteplus AS prep_cte,
        TRY_TO_NUMBER(curr_prep_cteplus_pct,10,1) AS prep_cte_pct,
        NULL AS prep_ag_cte,
        NULL AS prep_ag_cte_pct,
        curr_prep_collegecredit AS prep_one_college_course,
        TRY_TO_NUMBER(curr_prep_collegecredit_pct,10,1) AS prep_one_college_course_pct,
        NULL AS prep_two_college_course,
        NULL AS prep_two_college_course_pct,
        curr_prep_milsci AS prep_one_milsci_course,
        TRY_TO_NUMBER(curr_prep_milsci_pct,10,1) AS prep_one_milsci_course_pct,
        NULL AS prep_two_milsci_course,
        NULL AS prep_two_milsci_course_pct,
        NULL AS prep_non_reg_pre,
        NULL AS prep_non_reg_pre_pct,
        NULL AS prep_reg_pre,
        NULL AS prep_reg_pre_pct,
        NULL AS prep_statefedjobs,
        NULL AS prep_statefedjobs_pct,
        NULL AS prep_ssb,
        NULL AS prep_ssb_pct,
        NULL AS prep_tcbwe,
        NULL AS prep_tcbwe_pct,
        NULL AS prep_twbe,
        NULL AS prep_twbe_pct,
        NULL AS prep_tcbwe_and_twbe,
        NULL AS prep_tcbwe_and_twbe_pct,
        NULL AS aprep,
        NULL AS aprep_pct,
        NULL AS aprep_summative,
        NULL AS aprep_summative_pct,
        NULL AS aprep_ap,
        NULL AS aprep_ap_pct,
        NULL AS aprep_ib,
        NULL AS aprep_ib_pct,
        curr_aprep_ag AS aprep_ag,
        TRY_TO_NUMBER(curr_aprep_ag_pct,10,1) AS aprep_ag_pct,
        curr_aprep_cte AS aprep_cte,
        TRY_TO_NUMBER(curr_aprep_cte_pct,10,1) AS aprep_cte_pct,
        NULL AS aprep_ag_cte,
        NULL AS aprep_ag_cte_pct,
        curr_aprep_collegecredit AS aprep_one_college_course,
        TRY_TO_NUMBER(curr_aprep_collegecredit_pct,10,1) AS aprep_one_college_course_pct,
        NULL AS aprep_two_college_course,
        NULL AS aprep_two_college_course_pct,
        curr_aprep_milsci AS aprep_one_milsci_course,
        TRY_TO_NUMBER(curr_aprep_milsci_pct,10,1) AS aprep_one_milsci_course_pct,
        NULL AS aprep_two_milsci_course,
        NULL AS aprep_two_milsci_course_pct,
        NULL AS aprep_non_reg_pre,
        NULL AS aprep_non_reg_pre_pct,
        NULL AS aprep_reg_pre,
        NULL AS aprep_reg_pre_pct,
        NULL AS aprep_statefedjobs,
        NULL AS aprep_statefedjobs_pct,
        NULL AS aprep_ssb,
        NULL AS aprep_ssb_pct,
        NULL AS aprep_tcbwe,
        NULL AS aprep_tcbwe_pct,
        NULL AS aprep_twbe,
        NULL AS aprep_twbe_pct,
        NULL AS aprep_tcbwe_and_twbe,
        NULL AS aprep_tcbwe_and_twbe_pct,
        reportingyear
    FROM state_dashboard_ca.ccidownload2019
    WHERE rtype != 'X'
)
, cte_cci_state_sy19 AS (
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
        priordenom,
        TRY_TO_NUMBER(priorstatus,10,1) AS priorstatus,
        TRY_TO_NUMBER(change,10,1) AS change,
        statuslevel,
        changelevel,
        color,
        box,
        curr_prep AS prep,
        TRY_TO_NUMBER(curr_prep_pct,10,1) AS prep_pct,
        curr_prep_summative AS prep_summative,
        TRY_TO_NUMBER(curr_prep_summative_pct,10,1) AS prep_summative_pct,
        curr_prep_apexam AS prep_ap,
        TRY_TO_NUMBER(curr_prep_apexam_pct,10,1) AS prep_ap_pct,
        curr_prep_ibexam AS prep_ib,
        TRY_TO_NUMBER(curr_prep_ibexam_pct,10,1) AS prep_ib_pct,
        curr_prep_agplus AS prep_ag,
        TRY_TO_NUMBER(curr_prep_agplus_pct,10,1) AS prep_ag_pct,
        curr_prep_cteplus AS prep_cte,
        TRY_TO_NUMBER(curr_prep_cteplus_pct,10,1) AS prep_cte_pct,
        NULL AS prep_ag_cte,
        NULL AS prep_ag_cte_pct,
        curr_prep_collegecredit AS prep_one_college_course,
        TRY_TO_NUMBER(curr_prep_collegecredit_pct,10,1) AS prep_one_college_course_pct,
        NULL AS prep_two_college_course,
        NULL AS prep_two_college_course_pct,
        curr_prep_milsci AS prep_one_milsci_course,
        TRY_TO_NUMBER(curr_prep_milsci_pct,10,1) AS prep_one_milsci_course_pct,
        NULL AS prep_two_milsci_course,
        NULL AS prep_two_milsci_course_pct,
        NULL AS prep_non_reg_pre,
        NULL AS prep_non_reg_pre_pct,
        NULL AS prep_reg_pre,
        NULL AS prep_reg_pre_pct,
        NULL AS prep_statefedjobs,
        NULL AS prep_statefedjobs_pct,
        NULL AS prep_ssb,
        NULL AS prep_ssb_pct,
        NULL AS prep_tcbwe,
        NULL AS prep_tcbwe_pct,
        NULL AS prep_twbe,
        NULL AS prep_twbe_pct,
        NULL AS prep_tcbwe_and_twbe,
        NULL AS prep_tcbwe_and_twbe_pct,
        NULL AS aprep,
        NULL AS aprep_pct,
        NULL AS aprep_summative,
        NULL AS aprep_summative_pct,
        NULL AS aprep_ap,
        NULL AS aprep_ap_pct,
        NULL AS aprep_ib,
        NULL AS aprep_ib_pct,
        curr_aprep_ag AS aprep_ag,
        TRY_TO_NUMBER(curr_aprep_ag_pct,10,1) AS aprep_ag_pct,
        curr_aprep_cte AS aprep_cte,
        TRY_TO_NUMBER(curr_aprep_cte_pct,10,1) AS aprep_cte_pct,
        NULL AS aprep_ag_cte,
        NULL AS aprep_ag_cte_pct,
        curr_aprep_collegecredit AS aprep_one_college_course,
        TRY_TO_NUMBER(curr_aprep_collegecredit_pct,10,1) AS aprep_one_college_course_pct,
        NULL AS aprep_two_college_course,
        NULL AS aprep_two_college_course_pct,
        curr_aprep_milsci AS aprep_one_milsci_course,
        TRY_TO_NUMBER(curr_aprep_milsci_pct,10,1) AS aprep_one_milsci_course_pct,
        NULL AS aprep_two_milsci_course,
        NULL AS aprep_two_milsci_course_pct,
        NULL AS aprep_non_reg_pre,
        NULL AS aprep_non_reg_pre_pct,
        NULL AS aprep_reg_pre,
        NULL AS aprep_reg_pre_pct,
        NULL AS aprep_statefedjobs,
        NULL AS aprep_statefedjobs_pct,
        NULL AS aprep_ssb,
        NULL AS aprep_ssb_pct,
        NULL AS aprep_tcbwe,
        NULL AS aprep_tcbwe_pct,
        NULL AS aprep_twbe,
        NULL AS aprep_twbe_pct,
        NULL AS aprep_tcbwe_and_twbe,
        NULL AS aprep_tcbwe_and_twbe_pct,
        reportingyear
    FROM state_dashboard_ca.ccidownload2019
    WHERE rtype = 'X'
)
, cte_cci_state_compare_sy19 AS (
    SELECT cte_cci_nonstate_sy19.*
        , cte_cci_state_sy19.prep_pct AS prep_pct_state
        , cte_cci_state_sy19.prep_ap_pct AS prep_ap_pct_state
        , cte_cci_state_sy19.prep_ib_pct AS prep_ib_pct_state
        , cte_cci_state_sy19.prep_ag_pct AS prep_ag_pct_state
        , cte_cci_state_sy19.prep_cte_pct AS prep_cte_pct_state
        , cte_cci_state_sy19.prep_ag_cte_pct AS prep_ag_cte_pct_state
        , cte_cci_state_sy19.prep_one_college_course_pct AS prep_one_college_course_pct_state
        , cte_cci_state_sy19.prep_two_college_course_pct AS prep_two_college_course_pct_state
        , cte_cci_state_sy19.prep_one_milsci_course_pct AS prep_one_milsci_course_pct_state
        , cte_cci_state_sy19.prep_two_milsci_course_pct AS prep_two_milsci_course_pct_state
        , cte_cci_state_sy19.prep_non_reg_pre_pct AS prep_non_reg_pre_pct_state
        , cte_cci_state_sy19.prep_reg_pre_pct AS prep_reg_pre_pct_state
        , cte_cci_state_sy19.prep_statefedjobs_pct AS prep_statefedjobs_pct_state
        , cte_cci_state_sy19.prep_ssb_pct AS prep_ssb_pct_state
        , cte_cci_state_sy19.prep_tcbwe_pct AS prep_tcbwe_pct_state
        , cte_cci_state_sy19.prep_twbe_pct AS prep_twbe_pct_state
        , cte_cci_state_sy19.prep_tcbwe_and_twbe_pct AS prep_tcbwe_and_twbe_pct_state
        , cte_cci_nonstate_sy19.prep_pct - cte_cci_state_sy19.prep_pct AS prep_comparison
        , cte_cci_nonstate_sy19.prep_ap_pct - cte_cci_state_sy19.prep_ap_pct AS prep_ap_comparison
        , cte_cci_nonstate_sy19.prep_ib_pct - cte_cci_state_sy19.prep_ib_pct AS prep_ib_comparison
        , cte_cci_nonstate_sy19.prep_ag_pct - cte_cci_state_sy19.prep_ag_pct AS prep_ag_comparison
        , cte_cci_nonstate_sy19.prep_cte_pct - cte_cci_state_sy19.prep_cte_pct AS prep_cte_comparison
        , cte_cci_nonstate_sy19.prep_ag_cte_pct - cte_cci_state_sy19.prep_ag_cte_pct AS prep_ag_cte_comparison
        , cte_cci_nonstate_sy19.prep_one_college_course_pct - cte_cci_state_sy19.prep_one_college_course_pct AS prep_one_college_course_comparison
        , cte_cci_nonstate_sy19.prep_two_college_course_pct - cte_cci_state_sy19.prep_two_college_course_pct AS prep_two_college_course_comparison
        , cte_cci_nonstate_sy19.prep_one_milsci_course_pct - cte_cci_state_sy19.prep_one_milsci_course_pct AS prep_one_milsci_course_comparison
        , cte_cci_nonstate_sy19.prep_two_milsci_course_pct - cte_cci_state_sy19.prep_two_milsci_course_pct AS prep_two_milsci_course_comparison
        , cte_cci_nonstate_sy19.prep_non_reg_pre_pct - cte_cci_state_sy19.prep_non_reg_pre_pct AS prep_non_reg_pre_comparison
        , cte_cci_nonstate_sy19.prep_reg_pre_pct - cte_cci_state_sy19.prep_reg_pre_pct AS prep_reg_pre_comparison
        , cte_cci_nonstate_sy19.prep_statefedjobs_pct - cte_cci_state_sy19.prep_statefedjobs_pct AS prep_statefedjobs_comparison
        , cte_cci_nonstate_sy19.prep_ssb_pct - cte_cci_state_sy19.prep_ssb_pct AS prep_ssb_comparison
        , cte_cci_nonstate_sy19.prep_tcbwe_pct - cte_cci_state_sy19.prep_tcbwe_pct AS prep_tcbwe_comparison
        , cte_cci_nonstate_sy19.prep_twbe_pct - cte_cci_state_sy19.prep_twbe_pct AS prep_twbe_comparison
        , cte_cci_nonstate_sy19.prep_tcbwe_and_twbe_pct - cte_cci_state_sy19.prep_tcbwe_and_twbe_pct AS prep_tcbwe_and_twbe_comparison
        , cte_cci_nonstate_sy19.aprep_pct - cte_cci_state_sy19.aprep_pct AS aprep_comparison
        , cte_cci_nonstate_sy19.aprep_ap_pct - cte_cci_state_sy19.aprep_ap_pct AS aprep_ap_comparison
        , cte_cci_nonstate_sy19.aprep_ib_pct - cte_cci_state_sy19.aprep_ib_pct AS aprep_ib_comparison
        , cte_cci_nonstate_sy19.aprep_ag_pct - cte_cci_state_sy19.aprep_ag_pct AS aprep_ag_comparison
        , cte_cci_nonstate_sy19.aprep_cte_pct - cte_cci_state_sy19.aprep_cte_pct AS aprep_cte_comparison
        , cte_cci_nonstate_sy19.aprep_ag_cte_pct - cte_cci_state_sy19.aprep_ag_cte_pct AS aprep_ag_cte_comparison
        , cte_cci_nonstate_sy19.aprep_one_college_course_pct - cte_cci_state_sy19.aprep_one_college_course_pct AS parep_one_college_course_comparison
        , cte_cci_nonstate_sy19.aprep_two_college_course_pct - cte_cci_state_sy19.aprep_two_college_course_pct AS aprep_two_college_course_comparison
        , cte_cci_nonstate_sy19.aprep_one_milsci_course_pct - cte_cci_state_sy19.aprep_one_milsci_course_pct AS aprep_one_milsci_course_comparison
        , cte_cci_nonstate_sy19.aprep_two_milsci_course_pct - cte_cci_state_sy19.aprep_two_milsci_course_pct AS aprep_two_milsci_course_comparison
        , cte_cci_nonstate_sy19.aprep_non_reg_pre_pct - cte_cci_state_sy19.aprep_non_reg_pre_pct AS aprep_non_reg_pre_comparison
        , cte_cci_nonstate_sy19.aprep_reg_pre_pct - cte_cci_state_sy19.aprep_reg_pre_pct AS aprep_reg_pre_comparison
        , cte_cci_nonstate_sy19.aprep_statefedjobs_pct - cte_cci_state_sy19.aprep_statefedjobs_pct AS aprep_statefedjobs_comparison
        , cte_cci_nonstate_sy19.aprep_ssb_pct - cte_cci_state_sy19.aprep_ssb_pct AS aprep_ssb_comparison
        , cte_cci_nonstate_sy19.aprep_tcbwe_pct - cte_cci_state_sy19.aprep_tcbwe_pct AS aprep_tcbwe_comparison
        , cte_cci_nonstate_sy19.aprep_twbe_pct - cte_cci_state_sy19.aprep_twbe_pct AS aprep_twbe_comparison
        , cte_cci_nonstate_sy19.aprep_tcbwe_and_twbe_pct - cte_cci_state_sy19.aprep_tcbwe_and_twbe_pct AS aprep_tcbwe_and_twbe_comparison
        , cte_cci_nonstate_sy19.currstatus - cte_cci_state_sy19.currstatus AS currstatus_comparison
        , cte_cci_nonstate_sy19.statuslevel - cte_cci_state_sy19.statuslevel AS statuslevel_comparison
        , cte_cci_nonstate_sy19.change - cte_cci_state_sy19.change AS change_comparison
        , cte_cci_nonstate_sy19.changelevel - cte_cci_state_sy19.changelevel AS changelevel_comparison
    FROM cte_cci_nonstate_sy19
    LEFT JOIN cte_cci_state_sy19
        ON cte_cci_nonstate_sy19.studentgroup = cte_cci_state_sy19.studentgroup
)
, cte_cci_nonstate_sy18 AS (
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
        priordenom,
        TRY_TO_NUMBER(priorstatus,10,1) AS priorstatus,
        TRY_TO_NUMBER(change,10,1) AS change,
        statuslevel,
        changelevel,
        color,
        box,
        curr_prep AS prep,
        TRY_TO_NUMBER(curr_prep_pct,10,1) AS prep_pct,
        curr_prep_summative AS prep_summative,
        TRY_TO_NUMBER(curr_prep_summative_pct,10,1) AS prep_summative_pct,
        curr_prep_apexam AS prep_ap,
        TRY_TO_NUMBER(curr_prep_apexam_pct,10,1) AS prep_ap_pct,
        curr_prep_ibexam AS prep_ib,
        TRY_TO_NUMBER(curr_prep_ibexam_pct,10,1) AS prep_ib_pct,
        curr_prep_agplus AS prep_ag,
        TRY_TO_NUMBER(curr_prep_agplus_pct,10,1) AS prep_ag_pct,
        curr_prep_cteplus AS prep_cte,
        TRY_TO_NUMBER(curr_prep_cteplus_pct,10,1) AS prep_cte_pct,
        NULL AS prep_ag_cte,
        NULL AS prep_ag_cte_pct,
        curr_prep_collegecredit AS prep_one_college_course,
        TRY_TO_NUMBER(curr_prep_collegecredit_pct,10,1) AS prep_one_college_course_pct,
        NULL AS prep_two_college_course,
        NULL AS prep_two_college_course_pct,
        curr_prep_milsci AS prep_one_milsci_course,
        TRY_TO_NUMBER(curr_prep_milsci_pct,10,1) AS prep_one_milsci_course_pct,
        NULL AS prep_two_milsci_course,
        NULL AS prep_two_milsci_course_pct,
        NULL AS prep_non_reg_pre,
        NULL AS prep_non_reg_pre_pct,
        NULL AS prep_reg_pre,
        NULL AS prep_reg_pre_pct,
        NULL AS prep_statefedjobs,
        NULL AS prep_statefedjobs_pct,
        NULL AS prep_ssb,
        NULL AS prep_ssb_pct,
        NULL AS prep_tcbwe,
        NULL AS prep_tcbwe_pct,
        NULL AS prep_twbe,
        NULL AS prep_twbe_pct,
        NULL AS prep_tcbwe_and_twbe,
        NULL AS prep_tcbwe_and_twbe_pct,
        NULL AS aprep,
        NULL AS aprep_pct,
        NULL AS aprep_summative,
        NULL AS aprep_summative_pct,
        NULL AS aprep_ap,
        NULL AS aprep_ap_pct,
        NULL AS aprep_ib,
        NULL AS aprep_ib_pct,
        curr_aprep_ag AS aprep_ag,
        TRY_TO_NUMBER(curr_aprep_ag_pct,10,1) AS aprep_ag_pct,
        curr_aprep_cte AS aprep_cte,
        TRY_TO_NUMBER(curr_aprep_cte_pct,10,1) AS aprep_cte_pct,
        NULL AS aprep_ag_cte,
        NULL AS aprep_ag_cte_pct,
        curr_aprep_collegecredit AS aprep_one_college_course,
        TRY_TO_NUMBER(curr_aprep_collegecredit_pct,10,1) AS aprep_one_college_course_pct,
        NULL AS aprep_two_college_course,
        NULL AS aprep_two_college_course_pct,
        curr_aprep_milsci AS aprep_one_milsci_course,
        TRY_TO_NUMBER(curr_aprep_milsci_pct,10,1) AS aprep_one_milsci_course_pct,
        NULL AS aprep_two_milsci_course,
        NULL AS aprep_two_milsci_course_pct,
        NULL AS aprep_non_reg_pre,
        NULL AS aprep_non_reg_pre_pct,
        NULL AS aprep_reg_pre,
        NULL AS aprep_reg_pre_pct,
        NULL AS aprep_statefedjobs,
        NULL AS aprep_statefedjobs_pct,
        NULL AS aprep_ssb,
        NULL AS aprep_ssb_pct,
        NULL AS aprep_tcbwe,
        NULL AS aprep_tcbwe_pct,
        NULL AS aprep_twbe,
        NULL AS aprep_twbe_pct,
        NULL AS aprep_tcbwe_and_twbe,
        NULL AS aprep_tcbwe_and_twbe_pct,
        reportingyear
    FROM state_dashboard_ca.ccidownload2018
    WHERE rtype != 'X'
)
, cte_cci_state_sy18 AS (
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
        priordenom,
        TRY_TO_NUMBER(priorstatus,10,1) AS priorstatus,
        TRY_TO_NUMBER(change,10,1) AS change,
        statuslevel,
        changelevel,
        color,
        box,
        curr_prep AS prep,
        TRY_TO_NUMBER(curr_prep_pct,10,1) AS prep_pct,
        curr_prep_summative AS prep_summative,
        TRY_TO_NUMBER(curr_prep_summative_pct,10,1) AS prep_summative_pct,
        curr_prep_apexam AS prep_ap,
        TRY_TO_NUMBER(curr_prep_apexam_pct,10,1) AS prep_ap_pct,
        curr_prep_ibexam AS prep_ib,
        TRY_TO_NUMBER(curr_prep_ibexam_pct,10,1) AS prep_ib_pct,
        curr_prep_agplus AS prep_ag,
        TRY_TO_NUMBER(curr_prep_agplus_pct,10,1) AS prep_ag_pct,
        curr_prep_cteplus AS prep_cte,
        TRY_TO_NUMBER(curr_prep_cteplus_pct,10,1) AS prep_cte_pct,
        NULL AS prep_ag_cte,
        NULL AS prep_ag_cte_pct,
        curr_prep_collegecredit AS prep_one_college_course,
        TRY_TO_NUMBER(curr_prep_collegecredit_pct,10,1) AS prep_one_college_course_pct,
        NULL AS prep_two_college_course,
        NULL AS prep_two_college_course_pct,
        curr_prep_milsci AS prep_one_milsci_course,
        TRY_TO_NUMBER(curr_prep_milsci_pct,10,1) AS prep_one_milsci_course_pct,
        NULL AS prep_two_milsci_course,
        NULL AS prep_two_milsci_course_pct,
        NULL AS prep_non_reg_pre,
        NULL AS prep_non_reg_pre_pct,
        NULL AS prep_reg_pre,
        NULL AS prep_reg_pre_pct,
        NULL AS prep_statefedjobs,
        NULL AS prep_statefedjobs_pct,
        NULL AS prep_ssb,
        NULL AS prep_ssb_pct,
        NULL AS prep_tcbwe,
        NULL AS prep_tcbwe_pct,
        NULL AS prep_twbe,
        NULL AS prep_twbe_pct,
        NULL AS prep_tcbwe_and_twbe,
        NULL AS prep_tcbwe_and_twbe_pct,
        NULL AS aprep,
        NULL AS aprep_pct,
        NULL AS aprep_summative,
        NULL AS aprep_summative_pct,
        NULL AS aprep_ap,
        NULL AS aprep_ap_pct,
        NULL AS aprep_ib,
        NULL AS aprep_ib_pct,
        curr_aprep_ag AS aprep_ag,
        TRY_TO_NUMBER(curr_aprep_ag_pct,10,1) AS aprep_ag_pct,
        curr_aprep_cte AS aprep_cte,
        TRY_TO_NUMBER(curr_aprep_cte_pct,10,1) AS aprep_cte_pct,
        NULL AS aprep_ag_cte,
        NULL AS aprep_ag_cte_pct,
        curr_aprep_collegecredit AS aprep_one_college_course,
        TRY_TO_NUMBER(curr_aprep_collegecredit_pct,10,1) AS aprep_one_college_course_pct,
        NULL AS aprep_two_college_course,
        NULL AS aprep_two_college_course_pct,
        curr_aprep_milsci AS aprep_one_milsci_course,
        TRY_TO_NUMBER(curr_aprep_milsci_pct,10,1) AS aprep_one_milsci_course_pct,
        NULL AS aprep_two_milsci_course,
        NULL AS aprep_two_milsci_course_pct,
        NULL AS aprep_non_reg_pre,
        NULL AS aprep_non_reg_pre_pct,
        NULL AS aprep_reg_pre,
        NULL AS aprep_reg_pre_pct,
        NULL AS aprep_statefedjobs,
        NULL AS aprep_statefedjobs_pct,
        NULL AS aprep_ssb,
        NULL AS aprep_ssb_pct,
        NULL AS aprep_tcbwe,
        NULL AS aprep_tcbwe_pct,
        NULL AS aprep_twbe,
        NULL AS aprep_twbe_pct,
        NULL AS aprep_tcbwe_and_twbe,
        NULL AS aprep_tcbwe_and_twbe_pct,
        reportingyear
    FROM state_dashboard_ca.ccidownload2018
    WHERE rtype = 'X'
)
, cte_cci_state_compare_sy18 AS (
    SELECT cte_cci_nonstate_sy18.*
        , cte_cci_state_sy18.prep_pct AS prep_pct_state
        , cte_cci_state_sy18.prep_ap_pct AS prep_ap_pct_state
        , cte_cci_state_sy18.prep_ib_pct AS prep_ib_pct_state
        , cte_cci_state_sy18.prep_ag_pct AS prep_ag_pct_state
        , cte_cci_state_sy18.prep_cte_pct AS prep_cte_pct_state
        , cte_cci_state_sy18.prep_ag_cte_pct AS prep_ag_cte_pct_state
        , cte_cci_state_sy18.prep_one_college_course_pct AS prep_one_college_course_pct_state
        , cte_cci_state_sy18.prep_two_college_course_pct AS prep_two_college_course_pct_state
        , cte_cci_state_sy18.prep_one_milsci_course_pct AS prep_one_milsci_course_pct_state
        , cte_cci_state_sy18.prep_two_milsci_course_pct AS prep_two_milsci_course_pct_state
        , cte_cci_state_sy18.prep_non_reg_pre_pct AS prep_non_reg_pre_pct_state
        , cte_cci_state_sy18.prep_reg_pre_pct AS prep_reg_pre_pct_state
        , cte_cci_state_sy18.prep_statefedjobs_pct AS prep_statefedjobs_pct_state
        , cte_cci_state_sy18.prep_ssb_pct AS prep_ssb_pct_state
        , cte_cci_state_sy18.prep_tcbwe_pct AS prep_tcbwe_pct_state
        , cte_cci_state_sy18.prep_twbe_pct AS prep_twbe_pct_state
        , cte_cci_state_sy18.prep_tcbwe_and_twbe_pct AS prep_tcbwe_and_twbe_pct_state
        , cte_cci_nonstate_sy18.prep_pct - cte_cci_state_sy18.prep_pct AS prep_comparison
        , cte_cci_nonstate_sy18.prep_ap_pct - cte_cci_state_sy18.prep_ap_pct AS prep_ap_comparison
        , cte_cci_nonstate_sy18.prep_ib_pct - cte_cci_state_sy18.prep_ib_pct AS prep_ib_comparison
        , cte_cci_nonstate_sy18.prep_ag_pct - cte_cci_state_sy18.prep_ag_pct AS prep_ag_comparison
        , cte_cci_nonstate_sy18.prep_cte_pct - cte_cci_state_sy18.prep_cte_pct AS prep_cte_comparison
        , cte_cci_nonstate_sy18.prep_ag_cte_pct - cte_cci_state_sy18.prep_ag_cte_pct AS prep_ag_cte_comparison
        , cte_cci_nonstate_sy18.prep_one_college_course_pct - cte_cci_state_sy18.prep_one_college_course_pct AS prep_one_college_course_comparison
        , cte_cci_nonstate_sy18.prep_two_college_course_pct - cte_cci_state_sy18.prep_two_college_course_pct AS prep_two_college_course_comparison
        , cte_cci_nonstate_sy18.prep_one_milsci_course_pct - cte_cci_state_sy18.prep_one_milsci_course_pct AS prep_one_milsci_course_comparison
        , cte_cci_nonstate_sy18.prep_two_milsci_course_pct - cte_cci_state_sy18.prep_two_milsci_course_pct AS prep_two_milsci_course_comparison
        , cte_cci_nonstate_sy18.prep_non_reg_pre_pct - cte_cci_state_sy18.prep_non_reg_pre_pct AS prep_non_reg_pre_comparison
        , cte_cci_nonstate_sy18.prep_reg_pre_pct - cte_cci_state_sy18.prep_reg_pre_pct AS prep_reg_pre_comparison
        , cte_cci_nonstate_sy18.prep_statefedjobs_pct - cte_cci_state_sy18.prep_statefedjobs_pct AS prep_statefedjobs_comparison
        , cte_cci_nonstate_sy18.prep_ssb_pct - cte_cci_state_sy18.prep_ssb_pct AS prep_ssb_comparison
        , cte_cci_nonstate_sy18.prep_tcbwe_pct - cte_cci_state_sy18.prep_tcbwe_pct AS prep_tcbwe_comparison
        , cte_cci_nonstate_sy18.prep_twbe_pct - cte_cci_state_sy18.prep_twbe_pct AS prep_twbe_comparison
        , cte_cci_nonstate_sy18.prep_tcbwe_and_twbe_pct - cte_cci_state_sy18.prep_tcbwe_and_twbe_pct AS prep_tcbwe_and_twbe_comparison
        , cte_cci_nonstate_sy18.aprep_pct - cte_cci_state_sy18.aprep_pct AS aprep_comparison
        , cte_cci_nonstate_sy18.aprep_ap_pct - cte_cci_state_sy18.aprep_ap_pct AS aprep_ap_comparison
        , cte_cci_nonstate_sy18.aprep_ib_pct - cte_cci_state_sy18.aprep_ib_pct AS aprep_ib_comparison
        , cte_cci_nonstate_sy18.aprep_ag_pct - cte_cci_state_sy18.aprep_ag_pct AS aprep_ag_comparison
        , cte_cci_nonstate_sy18.aprep_cte_pct - cte_cci_state_sy18.aprep_cte_pct AS aprep_cte_comparison
        , cte_cci_nonstate_sy18.aprep_ag_cte_pct - cte_cci_state_sy18.aprep_ag_cte_pct AS aprep_ag_cte_comparison
        , cte_cci_nonstate_sy18.aprep_one_college_course_pct - cte_cci_state_sy18.aprep_one_college_course_pct AS parep_one_college_course_comparison
        , cte_cci_nonstate_sy18.aprep_two_college_course_pct - cte_cci_state_sy18.aprep_two_college_course_pct AS aprep_two_college_course_comparison
        , cte_cci_nonstate_sy18.aprep_one_milsci_course_pct - cte_cci_state_sy18.aprep_one_milsci_course_pct AS aprep_one_milsci_course_comparison
        , cte_cci_nonstate_sy18.aprep_two_milsci_course_pct - cte_cci_state_sy18.aprep_two_milsci_course_pct AS aprep_two_milsci_course_comparison
        , cte_cci_nonstate_sy18.aprep_non_reg_pre_pct - cte_cci_state_sy18.aprep_non_reg_pre_pct AS aprep_non_reg_pre_comparison
        , cte_cci_nonstate_sy18.aprep_reg_pre_pct - cte_cci_state_sy18.aprep_reg_pre_pct AS aprep_reg_pre_comparison
        , cte_cci_nonstate_sy18.aprep_statefedjobs_pct - cte_cci_state_sy18.aprep_statefedjobs_pct AS aprep_statefedjobs_comparison
        , cte_cci_nonstate_sy18.aprep_ssb_pct - cte_cci_state_sy18.aprep_ssb_pct AS aprep_ssb_comparison
        , cte_cci_nonstate_sy18.aprep_tcbwe_pct - cte_cci_state_sy18.aprep_tcbwe_pct AS aprep_tcbwe_comparison
        , cte_cci_nonstate_sy18.aprep_twbe_pct - cte_cci_state_sy18.aprep_twbe_pct AS aprep_twbe_comparison
        , cte_cci_nonstate_sy18.aprep_tcbwe_and_twbe_pct - cte_cci_state_sy18.aprep_tcbwe_and_twbe_pct AS aprep_tcbwe_and_twbe_comparison
        , cte_cci_nonstate_sy18.currstatus - cte_cci_state_sy18.currstatus AS currstatus_comparison
        , cte_cci_nonstate_sy18.statuslevel - cte_cci_state_sy18.statuslevel AS statuslevel_comparison
        , cte_cci_nonstate_sy18.change - cte_cci_state_sy18.change AS change_comparison
        , cte_cci_nonstate_sy18.changelevel - cte_cci_state_sy18.changelevel AS changelevel_comparison
    FROM cte_cci_nonstate_sy18
    LEFT JOIN cte_cci_state_sy18
        ON cte_cci_nonstate_sy18.studentgroup = cte_cci_state_sy18.studentgroup
)

SELECT * FROM cte_cci_state_compare_sy22

UNION ALL

SELECT * FROM cte_cci_state_compare_sy21

UNION ALL

SELECT * FROM cte_cci_state_compare_sy20

UNION ALL

SELECT * FROM cte_cci_state_compare_sy19

UNION ALL

SELECT * FROM cte_cci_state_compare_sy18