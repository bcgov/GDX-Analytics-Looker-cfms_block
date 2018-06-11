
connection: "redshift"
# Set the week start day to Sunday. Default is Monday
week_start_day: sunday

# include all views in this project
include: "*.view"

# For now, don't include the dashboard we built. There is an editable version in the Shared -> Service BC Folder
# include: "*.dashboard"

explore: cfms_poc {}
explore: cats {}
