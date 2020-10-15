
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

explore: cfms_poc {
  access_filter: {
    field: office_filter # use the version of office names that have "_" instead of " "
    user_attribute: office_name
  }
  persist_for: "5 minutes"

  join: appointments {
    type: left_outer
    sql_on: ${appointments.client_id} = ${cfms_poc.client_id};;
    relationship: many_to_one
  }
}
explore: cfms_poc_no_filter {
  from: cfms_poc
  persist_for: "5 minutes"

}

# A copy of the original CFMS / TheQ model for comparisons
explore: cfms_old {}

#explore: cfms_dev {
#  persist_for: "12 hours"
#}

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
