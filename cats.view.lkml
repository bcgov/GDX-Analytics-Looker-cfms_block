# This view provides the "RAW" constructed server logs. It pulls together data from SBC logs and GDX logs.

view: cats {
  derived_table: {
    sql: SELECT govdate,
          get_string,
          SPLIT_PART(SPLIT_PART(get_string, ' ', 2), '?',1) AS url,
          SPLIT_PART(SPLIT_PART(get_string, ' ', 2), '?',2) AS query,
          SPLIT_PART(SPLIT_PART(REGEXP_SUBSTR ( SPLIT_PART(SPLIT_PART(get_string, ' ', 2), '?',2), 'q=.*'), '=',2), '&', 1) AS search,
          REGEXP_SUBSTR ( SPLIT_PART(SPLIT_PART(get_string, ' ', 2), '?',2), 'q=.*&') AS search,
          refer,
          site,
          gdx_id,
          ip AS gdx_ip,
          source_translated_ip,
          source_host_name,
          flex_string,
          dd.isweekend,
          dd.isholiday,
          dd.sbcquarter, dd.lastdayofpsapayperiod::date,
          to_char(govdate, 'HH24:00-HH24:59') AS hourly_bucket,
          CASE WHEN date_part(minute, govdate) < 30
            THEN to_char(govdate, 'HH24:00-HH24:29')
            ELSE to_char(govdate, 'HH24:30-HH24:59')
          END AS half_hour_bucket,
          to_char(govdate, 'HH24:MI:SS') AS date_time_of_day

          FROM servicebc.cats_gdx AS gdx
          LEFT JOIN servicebc.cats_sbc AS sbc ON gdx.port = sbc.source_translated_port AND abs(DATEDIFF('minute', gdx.govdate, sbc.firewall_time)) < 30
          LEFT JOIN static.cats_info ON static.cats_info.asset_tag = sbc.source_host_name
          JOIN servicebc.datedimension AS dd on govdate::date = dd.datekey::date
          ;;
  }


  dimension: get_string {
    type: string
    sql: ${TABLE}.get_string;;
  }

  dimension: url {
    type: string
    sql: ${TABLE}.url;;
  }

  dimension: query {
    type: string
    sql: ${TABLE}.query;;
  }
  dimension: search {
    type: string
    sql: ${TABLE}.search;;
  }
  dimension: refer {
    type: string
    sql: ${TABLE}.refer;;
  }

  dimension: site {
    type: string
    sql: ${TABLE}.site;;
  }
  dimension: gdx_ip {
    type: string
    sql: ${TABLE}.gdx_ip;;
  }

  dimension: gdx_id {
    type: number
    sql: ${TABLE}.gdx_id;;
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


  dimension: time {
    type: string
    sql: ${TABLE}.date_time_of_day ;;
    group_label: "Date"
  }

  dimension: hourly_bucket {
    type: string
    sql: ${TABLE}.hourly_bucket ;;
    group_label: "Date"
  }
  dimension: half_hour_bucket {
    type: string
    sql: ${TABLE}.half_hour_bucket ;;
    group_label: "Date"
  }

  dimension: date {
    type:  date
    sql:  ${TABLE}.govdate ;;
    group_label: "Date"
  }
  dimension: week {
    type:  date_week_of_year
    sql:  ${TABLE}.govdate ;;
    group_label: "Date"
  }
  dimension: month {
    type:  date_month_name
    sql:  ${TABLE}.govdate ;;
    group_label: "Date"
  }
  dimension: year {
    type:  date_year
    sql:  ${TABLE}.govdate ;;
    group_label: "Date"
  }

  dimension: day_of_month {
    type:  date_day_of_month
    sql:  ${TABLE}.govdate ;;
    group_label: "Date"
  }
  dimension: day_of_week {
    type:  date_day_of_week
    sql:  ${TABLE}.govdate ;;
    group_label: "Date"
  }
  dimension: day_of_week_number {
    type:  date_day_of_week_index
    sql:  ${TABLE}.govdate + interval '1 day' ;;
    group_label: "Date"
  }


  dimension: is_weekend {
    type:  yesno
    sql:  ${TABLE}.isweekend ;;
    group_label:  "Date"
  }
  dimension: is_holiday {
    type:  yesno
    sql:  ${TABLE}.isholiday ;;
    group_label:  "Date"
  }
  dimension: fiscal_year {
    type:  date_fiscal_year
    sql:  ${TABLE}.govdate ;;
    group_label:  "Date"
  }
  dimension: fiscal_month {
    type:  date_fiscal_month_num
    sql:  ${TABLE}.govdate ;;
    group_label:  "Date"
  }
  dimension: fiscal_quarter {
    type:  date_fiscal_quarter
    sql:  ${TABLE}.govdate ;;
    group_label:  "Date"
  }
  dimension: fiscal_quarter_of_year {
    type:  date_fiscal_quarter_of_year
    sql:  ${TABLE}.govdate ;;
    group_label:  "Date"
  }
  dimension: sbc_quarter {
    type:  string
    sql:  ${TABLE}.sbcquarter ;;
    group_label:  "Date"
  }
  dimension: last_day_of_pay_period {
    type: date
    sql:  ${TABLE}.lastdayofpsapayperiod ;;
    group_label: "Date"
  }


}
