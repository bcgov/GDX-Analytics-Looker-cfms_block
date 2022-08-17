
connection: "redshift_pacific_time"
# Set the week start day to Sunday. Default is Monday
week_start_day: sunday
# Set fiscal year to begin April 1st -- https://docs.looker.com/reference/model-params/fiscal_month_offset
fiscal_month_offset: 3

# include all views in this project
include: "/Views/*.view"

# include themes_cache
include: "//cmslite_metadata/Explores/themes_cache.explore.lkml"

# For now, don't include the dashboard we built. There is an editable version in the Shared -> Service BC Folder
# include: "*.dashboard"

datagroup: theq_sbc_datagroup {
  description: "Datagroup for TheQ SBC caching"
  max_cache_age: "1 hour"
  sql_trigger: SELECT MAX(latest_time) FROM derived.theq_step1 ;;
}
explore: cfms_poc {
  access_filter: {
    field: office_filter # use the version of office names that have "_" instead of " "
    user_attribute: office_name
  }
  persist_with: theq_sbc_datagroup

  join: appointments {
    type: left_outer
    sql_on: ${appointments.client_id} = ${cfms_poc.client_id};;
    relationship: many_to_one
  }
}
explore: cfms_poc_no_filter {
  from: cfms_poc
  persist_with: theq_sbc_datagroup
}

explore: theq_merged_view {
  access_filter: {
    field: office_filter # use the version of office names that have "_" instead of " "
    user_attribute: office_name
  }
  #persist_with: theq_sbc_datagroup
}




# A copy of the original CFMS / TheQ model for comparisons
explore: cfms_old {}

explore: cfms_dev {
  persist_for: "2 hours"
}

explore: cats {
  join: cmslite_themes {
    type: left_outer
    sql_on: ${cats.node_id} = ${cmslite_themes.node_id} ;;
    relationship: one_to_one
  }
}

explore: cfms_all_events {}

explore: all_appointments {}
explore: appointments {
  access_filter: {
    field: office_filter # use the version of office names that have "_" instead of " "
    user_attribute: office_name
  }
}

datagroup: datagroup_09_29_49 {
  label: "09 and 39 Minute Datagroup"
  description: "Update every 20 minutes to drive incrementals PDT at 09, 29, and 49 past the hour except during overnight maintenance window"
  sql_trigger: SELECT CASE WHEN DATE_PART('hour',timezone('America/Vancouver', now())) BETWEEN 3 AND 5
                  THEN DATE(timezone('America/Vancouver', now())) + interval '150 minutes'
            WHEN DATE_PART('minute',timezone('America/Vancouver', now())) < 09 OR DATE_PART('minute',timezone('America/Vancouver', now())) >= 49
              THEN DATE_TRUNC('hour',timezone('America/Vancouver', now()))
            WHEN DATE_PART('minute',timezone('America/Vancouver', now())) >= 09 OR DATE_PART('minute',timezone('America/Vancouver', now())) < 29
              THEN DATE_TRUNC('hour',timezone('America/Vancouver', now())) +  interval '20 minutes'
            ELSE DATE_TRUNC('hour',timezone('America/Vancouver', now())) +  interval '40 minutes' END ;;
}
