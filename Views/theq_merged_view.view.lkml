# Version 1.0.0
view: theq_merged_view {
  derived_table: {
    sql: -- Add filter values to final query
-- date, program name, back office, transaction name, office name, channel
--"program_name" AS "cfms_poc.program_name",
--"back_office" AS "cfms_poc.back_office",
--"transaction_name" AS "cfms_poc.transaction_name",
--"office_name" AS "cfms_poc.office_name",
--"channel_sort" AS "cfms_poc.channel_sort",
--"channel" AS "cfms_poc.channel"

-- combo of client_id and service_count guarentees a unique record per service (also works for transactions)

WITH
    full_list AS ( -- all relevent data for each individual service for each individual visit
        SELECT
            cfms_poc.client_id AS "client_id"
      ,"service_count" AS "service_count"
      ,"namespace" AS "namespace"
            ,"office_id" AS "office_id"
            ,"office_name" AS "office_name"
      ,"welcome_time" AS "welcome_time"
      ,"program_name" AS "program_name"
      ,"back_office" AS "back_office"
      ,"transaction_name" AS "transaction_name"
      ,"channel" AS "channel"
      ,"inaccurate_time" AS  "inaccurate_time"
      ,"transaction_count" AS "transaction_count"
            ,service_creation_duration AS "service_creation_duration"
            ,prep_duration AS "prep_duration"
            ,serve_duration AS "serve_duration"
            ,CASE WHEN (ABS(service_creation_duration_zscore) >= 3) THEN TRUE ELSE FALSE END AS service_creation_outlier
            ,CASE WHEN (ABS(prep_duration_zscore) >= 3) THEN TRUE ELSE FALSE END AS prep_outlier
            ,CASE WHEN (ABS(serve_duration_zscore) >= 3) THEN TRUE ELSE FALSE END AS serve_outlier
        FROM
            "derived"."theq_step1" AS "cfms_poc"
    )
    ,inaccurate_list AS ( -- sets a flag for a visit if any one of its services is an outlier
        SELECT
            "client_id" AS "client_id"
      ,"namespace" AS "namespace"
      ,CASE WHEN (BOOL_OR(inaccurate_time) OR BOOL_OR(service_creation_outlier) OR BOOL_OR(prep_outlier) OR BOOL_OR(serve_outlier) = TRUE) THEN TRUE ELSE FALSE END AS outlier
        FROM
            full_list
    GROUP BY
      "client_id"
      ,"namespace"
    )
SELECT
    full_list."client_id"
  ,full_list."service_count"
  ,full_list."namespace"
    ,full_list."office_id"
    ,full_list."office_name"
  ,full_list."welcome_time"
  ,full_list."program_name"
  ,full_list."back_office"
  ,full_list."transaction_name"
  ,full_list."channel"
  ,full_list."transaction_count"
    ,full_list."service_creation_duration"
    ,full_list."prep_duration"
    ,full_list."serve_duration"
    ,full_list.service_creation_outlier
    ,full_list.prep_outlier
    ,full_list.serve_outlier
  ,inaccurate_list."outlier"
  ,CASE WHEN inaccurate_list."outlier" THEN full_list."client_id" ELSE NULL END AS client_id_outlier
  ,CASE WHEN inaccurate_list."outlier" THEN full_list."service_creation_duration" ELSE NULL END AS "service_creation_duration_outlier"
  ,CASE WHEN inaccurate_list."outlier" THEN full_list."prep_duration" ELSE NULL END AS "prep_duration_outlier"
  ,CASE WHEN inaccurate_list."outlier" THEN full_list."serve_duration" ELSE NULL END AS "serve_duration_outlier"
  ,CASE WHEN NOT inaccurate_list."outlier" THEN full_list."client_id" ELSE NULL END AS client_id_not_outlier
  ,CASE WHEN NOT inaccurate_list."outlier" THEN full_list."service_creation_duration" ELSE NULL END AS "service_creation_duration_not_outlier"
  ,CASE WHEN NOT inaccurate_list."outlier" THEN full_list."prep_duration" ELSE NULL END AS "prep_duration_not_outlier"
  ,CASE WHEN NOT inaccurate_list."outlier" THEN full_list."serve_duration" ELSE NULL END AS "serve_duration_not_outlier"
FROM
    full_list
    LEFT JOIN inaccurate_list ON
      full_list."client_id" = inaccurate_list."client_id"
      AND full_list."namespace" = inaccurate_list."namespace"

  ;;
                # https://docs.looker.com/data-modeling/learning-lookml/caching
          #distribution_style: all
          #sql_trigger_value: SELECT COUNT(*) FROM derived.theq_step1
    }


  dimension: outlier {}
  dimension: client_id {
    type: number
    sql: ${TABLE}.client_id ;;
    html: {{ rendered_value }} ;;
  }
  dimension: service_count {}
  dimension: office_id {}
  dimension: program_name {}
  dimension: back_office {}
  dimension: transaction_name {}
  dimension: channel {}
  dimension: transaction_count {}
  dimension_group: event {
    type: time
    timeframes: [raw, time, minute, minute10, time_of_day, hour_of_day, hour, date, day_of_month, day_of_week, week, month, quarter, year]
    sql: ${TABLE}.welcome_time ;;
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


  # Build measures
    measure: visits_count {
      description: "Count of distinct client IDs."
      type: number
      sql: COUNT (DISTINCT ${client_id} ) ;;
      group_label: "Counts"
    }
    measure: visits_count_no_outlier {
      type: number
      sql: COUNT (DISTINCT ${TABLE}.client_id_not_outlier ) ;;
      group_label: "Counts"
    }
    measure: visits_count_outlier {
      type: number
      sql: COUNT (DISTINCT ${TABLE}.client_id_outlier ) ;;
      group_label: "Counts"
    }


  measure: service_creation_duration_average {
    description: "Average service creation Duration."
    type:  average
    sql: (1.00 * ${TABLE}.service_creation_duration)/(60*60*24) ;;
    value_format: "[h]:mm:ss"
    group_label: "Service Creation Duration"
  }
  measure: service_creation_duration_average_outlier {
    description: "Average service creation Duration (outlier)."
    type:  average
    sql: (1.00 * ${TABLE}.service_creation_duration_outlier)/(60*60*24) ;;
    value_format: "[h]:mm:ss"
    group_label: "Service Creation Duration"
  }
  measure: service_creation_duration_average_not_outlier {
    description: "Average service creation Duration (not outlier)."
    type:  average
    sql: (1.00 * ${TABLE}.service_creation_duration_not_outlier)/(60*60*24) ;;
    value_format: "[h]:mm:ss"
    group_label: "Service Creation Duration"
  }

   }
