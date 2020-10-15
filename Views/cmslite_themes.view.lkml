include: "//cmslite_metadata/Views/themes.view"

view: cmslite_themes {
  extends: [themes]

# Hide unneeded dimensions from base view
  dimension: parent_node_id {hidden: yes}
  dimension: parent_title {hidden:yes}
  dimension: node_id {hidden: yes}  # use cats.node_id
  dimension: hr_url {hidden: yes}

}
