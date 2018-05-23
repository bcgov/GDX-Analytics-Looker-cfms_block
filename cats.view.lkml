view: cats {
  derived_table: {
    sql: SELECT govdate,
          get_string,
          refer,
          site,
          source_translated_ip,
          source_host_name,
          flex_string -- , sbc.*
          FROM servicebc.cats_gdx AS gdx
          JOIN servicebc.cats_sbc AS sbc ON gdx.port = sbc.source_translated_port
          LEFT JOIN static.cats_info ON static.cats_info.asset_tag = sbc.source_host_name
          WHERE abs(DATEDIFF('minute', gdx.govdate, sbc.firewall_time)) < 30 ;;
  }


  dimension: govdate {
    type: date_time
    sql: ${TABLE}.govdate;;
  }

  dimension: get_string {
    type: string
    sql: ${TABLE}.get_string;;
  }

  dimension: refer {
    type: string
    sql: ${TABLE}.refer;;
  }

  dimension: site {
    type: string
    sql: ${TABLE}.site;;
  }

  dimension: source_translated_ip {
    type: string
    sql: ${TABLE}.source_translated_ip;;
  }

  dimension: source_host_name {
    type: string
    sql: ${TABLE}.source_host_name;;
  }

  dimension: flex_string {
    type: string
    sql: ${TABLE}.flex_string;;
  }
}
