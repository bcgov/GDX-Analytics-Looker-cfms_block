
connection: "redshift"
# Set the week start day to Sunday. Default is Monday
week_start_day: sunday
# Set fiscal year to begin April 1st -- https://docs.looker.com/reference/model-params/fiscal_month_offset
fiscal_month_offset: 3

# include all views in this project
include: "*.view"

# For now, don't include the dashboard we built. There is an editable version in the Shared -> Service BC Folder
# include: "*.dashboard"

explore: cfms_poc {
  access_filter: {
    field: office_name
    user_attribute: office_name
  }
}


# See: https://docs.looker.com/reference/explore-params/access_filter
explore: cfms_dev {
  access_filter: {
    field: office_name
    user_attribute: office_name
  }
}


explore: cats {
  join: cmslite_themes {
    type: left_outer
    sql_on: ${cats.node_id} = ${cmslite_themes.node_id} ;;
    relationship: one_to_one
  }
}

explore: cfms_all_events {}
