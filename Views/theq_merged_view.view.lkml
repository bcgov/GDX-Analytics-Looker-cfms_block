# Version 1.0.0
view: theq_merged_view {
  derived_table: {
    sql: WITH full_list AS (
        SELECT
            client_id,
            service_count,
            namespace,
            office_name,
            program_name,
            back_office,
            welcome_time,
            inaccurate_time,
            CASE WHEN (ABS(service_creation_duration_zscore) >= 3) THEN TRUE ELSE FALSE END AS service_creation_outlier,
            CASE WHEN (ABS(prep_duration_zscore) >= 3) THEN TRUE ELSE FALSE END AS prep_outlier,
            CASE WHEN (ABS(serve_duration_zscore) >= 3) THEN TRUE ELSE FALSE END AS serve_outlier,
            service_creation_duration_total,
            prep_duration_total,
            serve_duration_total,
            COALESCE(service_creation_duration_total,0) + COALESCE(prep_duration_total,0) + COALESCE(serve_duration_total,0) AS time_total
        FROM
            derived.theq_step1 AS cfms_poc
        WHERE welcome_time > '2022-01-01'
      ),
      inaccurate_list AS (
        SELECT client_id, namespace, BOOL_OR(inaccurate_time) OR BOOL_OR(service_creation_outlier) OR BOOL_OR(prep_outlier) OR BOOL_OR(serve_outlier) AS outlier
        FROM full_list
        GROUP BY 1,2
      )SELECT full_list.*, outlier,
        CASE WHEN outlier THEN full_list.client_id ELSE NULL END AS client_id_outlier,
        CASE WHEN outlier THEN NULL ELSE full_list.client_id END AS client_id_no_outlier,
        CASE WHEN outlier THEN NULL ELSE service_creation_duration_total END AS service_creation_duration_total_no_outliers,
        CASE WHEN outlier THEN NULL ELSE prep_duration_total END AS prep_duration_total_no_outliers,
        CASE WHEN outlier THEN NULL ELSE serve_duration_total END AS serve_duration_total_no_outliers,
        CASE WHEN outlier THEN NULL ELSE time_total END AS time_total_no_outliers,  CASE WHEN outlier THEN service_creation_duration_total ELSE NULL END AS service_creation_duration_total_outliers,
        CASE WHEN outlier THEN prep_duration_total ELSE NULL END AS prep_duration_total_outliers,
        CASE WHEN outlier THEN serve_duration_total ELSE NULL END AS serve_duration_total_outliers,
        CASE WHEN outlier THEN time_total ELSE NULL END AS time_total_outliers  FROM full_list
        LEFT JOIN inaccurate_list ON full_list.client_id = inaccurate_list.client_id AND full_list.namespace = inaccurate_list.namespace
        ;;
                # https://docs.looker.com/data-modeling/learning-lookml/caching
          #distribution_style: all
          #sql_trigger_value: SELECT COUNT(*) FROM derived.theq_step1 ;;
    }

  # Build measures and dimensions
    measure: visits_count {
      description: "Count of distinct client IDs."
      type: number
      sql: COUNT (DISTINCT ${client_id} ) ;;
      group_label: "Counts"
    }
    measure: visits_count_no_outlier {
      type: number
      sql: COUNT (DISTINCT ${TABLE}.client_id_no_outlier ) ;;
      group_label: "Counts"
    }
    measure: visits_count_outlier {
      type: number
      sql: COUNT (DISTINCT ${TABLE}.client_id_outlier ) ;;
      group_label: "Counts"
    }
    dimension: back_office {
      description: "Whether a given service was front or back office."
      type:  string
      sql:  ${TABLE}.back_office ;;
    }
    # Time based measures
    measure: service_creation_duration_total {
      description: "Total service_creation duration."
      type:  sum
      sql: (1.00 * ${TABLE}.service_creation_duration)/(60*60*24) ;;
      value_format: "[h]:mm:ss"
      group_label: "service_creation Duration"
    }
    measure: service_creation_duration_per_visit_max {
      description: "Maximum service_creation duration."
      type:  max
      sql: (1.00 * ${TABLE}.service_creation_duration)/(60*60*24) ;;
      value_format: "[h]:mm:ss"
      group_label: "service_creation Duration"
    }
    measure: service_creation_duration_per_visit_average {
      description: "Average service_creation duration."
      type:  average
      sql: (1.00 * ${TABLE}.service_creation_duration)/(60*60*24) ;;
      value_format: "[h]:mm:ss"
      group_label: "service_creation Duration"
    }
    measure: waiting_duration_total {
      description: "Total waiting duration."
      type: sum
      sql: (1.00 * ${TABLE}.waiting_duration)/(60*60*24) ;;
      value_format: "[h]:mm:ss"
      group_label: "Waiting Duration"
    }
    measure: waiting_duration_per_service_average {
      description: "Average waiting duration per service delivered."
      type:  average
      sql: (1.00 * ${TABLE}.waiting_duration)/(60*60*24) ;;
      value_format: "[h]:mm:ss"
      group_label: "Waiting Duration"
    }  # See here to understand the use of sum_distinct and average_distinct:
    #    https://docs.looker.com/reference/field-reference/measure-type-reference#sum_distinct
    measure: waiting_duration_per_visit_max {
      description: "Maximum total waiting duration per visit."
      type: max
      sql: (1.00 * ${TABLE}.waiting_duration_total)/(60*60*24) ;;
      value_format: "[h]:mm:ss"
      group_label: "Waiting Duration"
    }
    measure: waiting_duration_per_visit_average {
      description: "Average waiting duration per visit."
      type: average_distinct
      sql_distinct_key: ${client_id} ;;
      sql: (1.00 * ${TABLE}.waiting_duration_total)/(60*60*24) ;;
      value_format: "[h]:mm:ss"
      group_label: "Waiting Duration"
    }
    measure: prep_duration_total {
      description: "Total preparation duration."
      type: sum
      sql: (1.00 * ${TABLE}.prep_duration)/(60*60*24) ;;
      value_format: "[h]:mm:ss"
      group_label: "Prep Duration"
    }
    measure: prep_duration_per_service_average {
      description: "Average preparation duration per service delivered."
      type:  average
      sql: (1.00 * ${TABLE}.prep_duration)/(60*60*24) ;;
      value_format: "[h]:mm:ss"
      group_label: "Prep Duration"
    }
    measure: prep_duration_per_visit_max {
      description: "Maximum preparation duration per visit."
      type: max
      sql: (1.00 * ${TABLE}.prep_duration_total)/(60*60*24) ;;
      value_format: "[h]:mm:ss"
      group_label: "Prep Duration"
    }
    measure: prep_duration_per_visit_average {
      description: "Average preparation duration per visit."
      type: average_distinct
      sql_distinct_key: ${client_id} ;;
      sql: (1.00 * ${TABLE}.prep_duration_total)/(60*60*24) ;;
      value_format: "[h]:mm:ss"
      group_label: "Prep Duration"
    }
    measure: hold_duration_total {
      description: "Total hold duration."
      type: sum_distinct
      sql_distinct_key: ${client_id} ;;
      sql: (1.00 * ${TABLE}.hold_duration)/(60*60*24) ;;
      value_format: "[h]:mm:ss"
      group_label: "Hold Duration"
    }
    measure: hold_duration_per_service_average {
      description: "Average hold duration per service delivered."
      type:  average_distinct
      sql_distinct_key: ${client_id} ;;
      sql: (1.00 * ${TABLE}.hold_duration)/(60*60*24) ;;
      value_format: "[h]:mm:ss"
      group_label: "Hold Duration"
    }
    measure: hold_duration_per_visit_max {
      description: "Maximum total hold duration per visit."
      type: max
      sql: (1.00 * ${TABLE}.hold_duration_total)/(60*60*24) ;;
      value_format: "[h]:mm:ss"
      group_label: "Hold Duration"
    }
    measure: hold_duration_per_visit_average {
      description: "Average hold duration per visit."
      type: average_distinct
      sql_distinct_key: ${client_id} ;;
      sql: (1.00 * ${TABLE}.hold_duration_total)/(60*60*24) ;;
      value_format: "[h]:mm:ss"
      group_label: "Hold Duration"
    }
    measure: serve_duration_total {
      description: "Total serve duration."
      type: sum
      sql: (1.00 * ${TABLE}.serve_duration)/(60*60*24) ;;
      value_format: "[h]:mm:ss"
      group_label: "Serve Duration"
    }
    measure: serve_duration_per_service_average {
      description: "Average serve duration per service delivered."
      type:  average
      sql: (1.00 * ${TABLE}.serve_duration)/(60*60*24) ;;
      value_format: "[h]:mm:ss"
      group_label: "Serve Duration"
    }  measure: serve_duration_per_visit_max {
      description: "Maximum total serve duration per visit."
      type: max
      sql: (1.00 * ${TABLE}.serve_duration_total)/(60*60*24) ;;
      value_format: "[h]:mm:ss"
      group_label: "Serve Duration"
    }  #measure: serve_duration_total_raw {
    #  type: sum_distinct
    #  sql_distinct_key: ${client_id} ;;
    #  sql: ${TABLE}.serve_duration_total;;
    #  group_label: "Durations"
    #}
    measure: serve_duration_per_visit_average {
      description: "Average serve duration per visit."
      type: average_distinct
      sql_distinct_key: ${client_id} ;;
      sql: (1.00 * ${TABLE}.serve_duration_total)/(60*60*24) ;;
      value_format: "[h]:mm:ss"
      group_label: "Serve Duration"
    }
    measure: serve_duration_per_visit_no_outliers {
      description: "Average serve duration per visit."
      type: average_distinct
      sql_distinct_key: ${client_id} ;;
      sql: (1.00 * ${TABLE}.serve_duration_total_no_outliers)/(60*60*24) ;;
      value_format: "[h]:mm:ss"
      group_label: "Serve Duration"
    }  measure: time_total {
      type: sum_distinct
      sql_distinct_key: ${client_id} ;;
      sql: (1.00 * ${TABLE}.time_total)/(60*60*24) ;;
      value_format: "[h]:mm:ss"
      group_label: "Serve Duration"
    }
    measure: time_total_no_outliers {
      type: sum_distinct
      sql_distinct_key: ${client_id} ;;
      sql: (1.00 * ${TABLE}.time_total_no_outliers)/(60*60*24) ;;
      value_format: "[h]:mm:ss"
      group_label: "Serve Duration"
    }
    measure: time_total_outliers {
      type: sum_distinct
      sql_distinct_key: ${client_id} ;;
      sql: (1.00 * ${TABLE}.time_total_outliers)/(60*60*24) ;;
      value_format: "[h]:mm:ss"
      group_label: "Serve Duration"
    }  # Time based dimentions
    dimension: service_creation_duration_per_visit {
      description: "service_creation duration for this visit."
      type:  number
      sql: (1.00 * ${TABLE}.service_creation_duration)/(60*60*24) ;;
      value_format: "[h]:mm:ss"
      group_label: "Durations"
    }
    dimension: waiting_duration_per_service {
      description: "Waiting duration for this individual service."
      type:  number
      sql: (1.00 * ${TABLE}.waiting_duration)/(60*60*24) ;;
      value_format: "[h]:mm:ss"
      group_label: "Durations"
    }
    dimension: waiting_duration_per_visit {
      description: "Total waiting duration for this visit."
      type:  number
      sql: (1.00 * ${TABLE}.waiting_duration_total)/(60*60*24) ;;
      value_format: "[h]:mm:ss"
      group_label: "Durations"
    }
    dimension: prep_duration_per_service {
      description: "Preparation duration for this individual service."
      type:  number
      sql: (1.00 * ${TABLE}.prep_duration)/(60*60*24) ;;
      value_format: "[h]:mm:ss"
      group_label: "Durations"
    }
    dimension: prep_duration_per_visit {
      description: "Total preparation duration for this visit."
      type:  number
      sql: (1.00 * ${TABLE}.prep_duration_total)/(60*60*24) ;;
      value_format: "[h]:mm:ss"
      group_label: "Durations"
    }
    dimension: hold_duration_per_service {
      description: "Hold duration for this individual service."
      type:  number
      sql: (1.00 * ${TABLE}.hold_duration)/(60*60*24) ;;
      value_format: "[h]:mm:ss"
      group_label: "Durations"
    }
    dimension: hold_duration_per_visit {
      description: "Total hold duration for this visit."
      type:  number
      sql: (1.00 * ${TABLE}.hold_duration_total)/(60*60*24) ;;
      value_format: "[h]:mm:ss"
      group_label: "Durations"
    }
    dimension: serve_duration_per_service {
      description: "Serve duration for this individual service."
      type:  number
      sql: (1.00 * ${TABLE}.serve_duration)/(60*60*24) ;;
      value_format: "[h]:mm:ss"
      group_label: "Durations"
    }
    dimension: serve_duration_per_visit {
      description: "Total serve duration for this visit."
      type:  number
      sql: (1.00 * ${TABLE}.serve_duration_total)/(60*60*24) ;;
      value_format: "[h]:mm:ss"
      group_label: "Durations"
    }
    # Outlier dimensions
    dimension: service_creation_duration_zscore {
      type:  number
      sql: ${TABLE}.service_creation_duration_zscore ;;
      group_label: "Z-Scores"
      hidden: yes
    }
    dimension: waiting_duration_zscore {
      type:  number
      sql: ${TABLE}.waiting_duration_zscore ;;
      group_label: "Z-Scores"
      hidden: yes
    }
    dimension: prep_duration_zscore {
      type:  number
      sql: ${TABLE}.prep_duration_zscore ;;
      group_label: "Z-Scores"
      hidden: yes
    }
    dimension: hold_duration_zscore {
      type:  number
      sql: ${TABLE}.hold_duration_zscore ;;
      group_label: "Z-Scores"
      hidden: yes
    }
    dimension: serve_duration_zscore {
      type:  number
      sql: ${TABLE}.serve_duration_zscore ;;
      group_label: "Z-Scores"
      hidden: yes
    }
    dimension: service_creation_duration_outlier {
      description: "Is the service_creation duration greater than 3 standard deviations from the average?"
      type:  yesno
      sql: abs(${TABLE}.service_creation_duration_zscore) >= 3 ;;
      group_label: "Z-Scores"
    }
    dimension: waiting_duration_outlier {
      description: "Is the waiting duration greater than 3 standard deviations from the average?"
      type:  yesno
      sql: abs(${TABLE}.waiting_duration_zscore) >= 3 ;;
      group_label: "Z-Scores"
    }
    dimension: prep_duration_outlier {
      description: "Is the preparation duration greater than 3 standard deviations from the average?"
      type:  yesno
      sql: abs(${TABLE}.prep_duration_zscore) >= 3 ;;
      group_label: "Z-Scores"
    }
    dimension: hold_duration_outlier {
      description: "Is the hold duration greater than 3 standard deviations from the average?"
      type:  yesno
      sql: abs(${TABLE}.hold_duration_zscore) >= 3 ;;
      group_label: "Z-Scores"
    }
    dimension: serve_duration_outlier {
      description: "Is the serve duration greater than 3 standard deviations from the average?"
      type:  yesno
      sql: abs( ${TABLE}.serve_duration_zscore) >= 3;;
      group_label: "Z-Scores"
    }
    dimension: welcome_time {
      description: "Welcome time for this visit."
      type: date_time
      sql: ${TABLE}.welcome_time ;;
      group_label: "Timing Points"
    }
    dimension: date {
      type:  date
      sql:  ${TABLE}.welcome_time ;;
      group_label: "Date"
    }
    dimension: month {
      type:  date_month_name
      sql:  ${TABLE}.welcome_time ;;
      group_label: "Date"
    }
    dimension: year {
      type:  date_year
      sql:  ${TABLE}.welcome_time ;;
      group_label: "Date"
    }
    dimension: client_id {
      type: number
      sql: ${TABLE}.client_id ;;
      html: {{ rendered_value }} ;;
    }
    dimension: office_name {
      type:  string
      sql:  ${TABLE}.office_name ;;
      group_label: "Office Info"
    }
    dimension: office_filter{
      type: string
      sql: TRANSLATE(TRANSLATE(${TABLE}.office_name, ' ', '_'),'.','') ;; #-- translate location names to use "_" instead of " " for filtering
      group_label: "Office Info"
    }
    dimension: program_name {
      type: string
      sql: ${TABLE}.program_name ;;
      group_label: "Program Information"
    }
  }
