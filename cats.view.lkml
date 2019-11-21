view: cats {
  derived_table: {
    sql: SELECT govdate,
          COALESCE(cms.node_id,SPLIT_PART(SPLIT_PART(SPLIT_PART(get_string, ' ', 2), '?',2), 'id=', 2)) AS node_id,
          get_string,
          CASE WHEN (SPLIT_PART(SPLIT_PART(get_string, ' ', 2), '?',1) = '/gov/search')
            THEN 'Search'
            ELSE COALESCE (SPLIT_PART(cms.dcterms_creator, '|', 2), SPLIT_PART(cms_guid.dcterms_creator, '|', 2))
            END AS page_owner,
          themes.theme,themes.subtheme,themes.theme_id,themes.subtheme_id,themes.title,
          CASE WHEN (SPLIT_PART(SPLIT_PART(get_string, ' ', 2), '?',1) = '/gov/content')
            THEN cms_guid.hr_url
            ELSE SPLIT_PART(SPLIT_PART(get_string, ' ', 2), '?',1)
          END AS url,
          SPLIT_PART(SPLIT_PART(get_string, ' ', 2), '?',2) AS query,
          SPLIT_PART(SPLIT_PART(REGEXP_SUBSTR ( SPLIT_PART(SPLIT_PART(get_string, ' ', 2), '?',2), 'q=.*'), '=',2), '&', 1) AS search,
          -- REGEXP_SUBSTR ( SPLIT_PART(SPLIT_PART(get_string, ' ', 2), '?',2), 'q=.*&') AS search,
          refer,
          office AS office_name,
          client AS office_owner,
          gdx_id,
          ip AS gdx_ip,
          source_translated_ip,
          proxy,
          port,
          --source_host_name,
          --source_mac_address,
          --sbc.source_host_name AS asset_tag,
          flex_string,
          dd.isweekend::BOOLEAN,
          dd.isholiday::BOOLEAN,
          dd.sbcquarter, dd.lastdayofpsapayperiod::date,
          to_char(govdate, 'HH24:00-HH24:59') AS hourly_bucket,
          CASE WHEN date_part(minute, govdate::timestamp) < 30
            THEN to_char(govdate, 'HH24:00-HH24:29')
            ELSE to_char(govdate, 'HH24:30-HH24:59')
          END AS half_hour_bucket,
          to_char(govdate, 'HH24:MI:SS') AS date_time_of_day,
          office_info.officesize AS office_size,
          office_info.area AS area_number,
          office_info.current_area AS current_area,
          office_info.id AS office_id
          FROM servicebc.cats_gdx  AS gdx
          LEFT JOIN servicebc.cats_sbc AS sbc ON gdx.port = sbc.source_translated_port AND abs(DATEDIFF('minute', gdx.govdate::timestamp, sbc.firewall_time)) < 30
          -- Use sbc.office where it exists. If it is NULL, the try looking up the site based on the asset tag
          --LEFT JOIN servicebc.cats_info ON servicebc.cats_info.asset_tag = sbc.source_host_name AND sbc.source_host_name <> ''
          LEFT JOIN servicebc.office_info ON servicebc.office_info.site = sbc.office AND end_date IS NULL -- for now, get the most recent office info
          JOIN servicebc.datedimension AS dd on govdate::date = dd.datekey::date
          LEFT JOIN cmslite.metadata AS cms ON cms.hr_url = 'https://www2.gov.bc.ca' || SPLIT_PART(SPLIT_PART(get_string, ' ', 2), '?',1)
          LEFT JOIN cmslite.metadata AS cms_guid ON
              SPLIT_PART(SPLIT_PART(get_string, ' ', 2), '?',1)  =  '/gov/content'
              AND cms_guid.node_id = SPLIT_PART(SPLIT_PART(SPLIT_PART(get_string, ' ', 2), '?',2), 'id=', 2)
          LEFT JOIN cmslite.themes ON cmslite.themes.node_id = COALESCE(cms.node_id,SPLIT_PART(SPLIT_PART(SPLIT_PART(get_string, ' ', 2), '?',2), 'id=', 2));;
    # https://docs.looker.com/data-modeling/learning-lookml/caching
    # This should cause the table to rebuild every day at 7am. May need to confirm timezones.
      sql_trigger_value: SELECT FLOOR((EXTRACT(epoch from GETDATE()) - 60*60*7)/(60*60*24)) ;;
      distribution_style: all
    }

    dimension: title {
      description: "The CMS Lite page title."
      type: string
      sql: ${TABLE}.title ;;
      group_label: "Page Info"
    }
    dimension: theme {
      description: "The CMS Lite theme."
      type: string
      sql: ${TABLE}.theme ;;
      group_label: "Page Info"
    }
    dimension: theme_id {
      description: "The alphanumeric CMS Lite theme identifer."
      type: string
      sql: COALESCE(${TABLE}.theme_id,'') ;; #ensure that this field is not NULL, so that user attribute filters work
      group_label: "Page Info"
    }

    dimension: subtheme {
      description: "The CMS Lite subtheme."
      type: string
      sql: ${TABLE}.subtheme ;;
      group_label: "Page Info"
    }
    dimension: subtheme_id {
      description: "The alphanumeric CMS Lite subtheme identifer."
      type: string
      sql: ${TABLE}.subtheme_id ;;
      group_label: "Page Info"
    }

    dimension: proxy {
      type: string
      sql: ${TABLE}.proxy;;
      group_label: "Page Info"
    }

    dimension: port {
      type: string
      sql: ${TABLE}.port;;
      group_label: "Page Info"
    }

    dimension: get_string {
      type: string
      sql: ${TABLE}.get_string;;
      group_label: "Page Info"
    }

    dimension: url {
      type: string
      sql: ${TABLE}.url;;
      group_label: "Page Info"
    }
    dimension: page_owner {
      type:  string
      sql:  ${TABLE}.page_owner ;;
      group_label: "Page Info"
      drill_fields: [url]
    }
    dimension: node_id {
      description: "The alphanumeric CMS Lite node identifier."
      type:  string
      sql:  ${TABLE}.node_id ;;
      group_label: "Page Info"
    }

    dimension: query {
      type: string
      sql: ${TABLE}.query;;
      group_label: "Page Info"
    }
    dimension: search {
      type: string
      sql: ${TABLE}.search;;
      group_label: "Page Info"
    }
    dimension: refer {
      type: string
      sql: ${TABLE}.refer;;
      group_label: "Page Info"
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

    #dimension: source_host_name {
    #  type: string
    #  sql: ${TABLE}.source_host_name;;
    #}

    dimension: flex_string {
      type: string
      sql: ${TABLE}.flex_string;;
    }

    dimension: asset_tag {
      type: string
      sql: ${TABLE}.asset_tag;;
    }
    #dimension: source_mac_address {
    #  type: string
    #  sql: ${TABLE}.source_mac_address;;
    #}

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
    measure: count  {
      type:  count
    }
    dimension: office_id {
      type: number
      sql: ${TABLE}.office_id ;;
      group_label: "Office Info"
      drill_fields: [office_name]
    }

    dimension: office_name {
      type:  string
      sql:  ${TABLE}.office_name ;;
      group_label: "Office Info"
      drill_fields: [office_name]
    }
    dimension: office_owner {
      type:  string
      sql:  ${TABLE}.office_owner ;;
      group_label: "Office Info"
    }
    dimension: office_size {
      type:  string
      sql:  ${TABLE}.office_size ;;
      group_label: "Office Info"
      drill_fields: [office_name]
    }
    dimension: area_number {
      type:  number
      sql:  ${TABLE}.area_number ;;
      group_label: "Office Info"
      drill_fields: [office_name]
    }
    dimension: current_area {
      type:  string
      sql:  ${TABLE}.current_area ;;
      group_label: "Office Info"
      drill_fields: [office_name]
    }

  }
