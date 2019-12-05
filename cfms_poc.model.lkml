
connection: "redshift_pacific_time"
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
    field: office_filter # use the version of office names that have "_" instead of " "
    user_attribute: office_name
  }
  persist_for: "5 minutes"
}
explore: cfms_poc_no_filter {
  from: cfms_poc
  persist_for: "5 minutes"

}

# A copy of the original CFMS / TheQ model for comparisons
explore: cfms_old {}

explore: cfms_dev {
  persist_for: "5 minutes"
}

explore: cats {}

explore: cfms_all_events {}

explore: all_appointments {}
