view: cfms_poc {
  derived_table: {
    sql: WITH step1 AS( -- Build a CTE containing all events using the name_tracker "CFMS_poc"
          -- this will include all fields for all possible events. We will then
          -- build the individual tables from this big one below
          -- NOTE: we are ignoring instances where there is no client_id
          --
          -- See here for info on incrementally building 'derived.cfms_step1'
          -- https://github.com/snowplow-proservices/ca.bc.gov-snowplow-pipeline/tree/master/jobs/cfms
    SELECT * FROM derived.cfms_step1
    WHERE namespace <> 'TheQ_dev'
    AND client_id NOT IN (SELECT * from servicebc.bad_clientids ) -- exclude entries in servicebc.bad_clientids from all reporting
    ),
      welcome_table AS( -- This CTE captures all events that could trigger a "Welcome time".
                        -- This occurs when the "addcitizen" event is hit
          SELECT
            namespace,
            event_name,
            event_time,
            client_id,
            service_count,
            office_id,
            office_type,
            agent_id,
            event_time welcome_time
          FROM step1
          WHERE event_name in ('addcitizen')
          ORDER BY event_time
          ),
        stand_table AS( -- This CTE captures all events that could trigger a "Stand time".
                        -- This occurs when the "addtoqueue" event is hit
          SELECT
            namespace,
            event_name,
            event_time,
            client_id,
            service_count,
            office_id,
            agent_id,
            event_time stand_time
          FROM step1
          WHERE event_name in ('addtoqueue')
          ORDER BY event_time
          ),
        invite_table AS(-- This CTE captures all events that could trigger a "Invite time".
                        -- This occurs when the "invitecitizen" or "invitefrom list" event is hit
                        -- Note that in calculations below we will take the LAST occurence of this
          SELECT
            namespace,
            event_name,
            event_time,
            client_id,
            service_count,
            office_id,
            agent_id,
            event_time invite_time
          FROM step1
          WHERE event_name in ('invitecitizen','invitefromlist')
          ORDER BY event_time DESC
          ),
        start_table AS( -- This CTE captures all events that could trigger a "Start time".
                        -- This occurs when the "beginservice" event is hit
          SELECT
            namespace,
            event_name,
            event_time,
            client_id,
            service_count,
            office_id,
            agent_id,
            event_time start_time
          FROM step1
          WHERE event_name in ('beginservice')
          ORDER BY event_time
          ),
        hold_table AS( -- This CTE captures all events that could trigger a "Hold time".
                        -- This occurs when the "beginservice" event is hit
          SELECT
            namespace,
            event_name,
            event_time,
            client_id,
            service_count,
            office_id,
            agent_id,
            event_time hold_time
          FROM step1
          WHERE event_name in ('hold')
          ORDER BY event_time
          ),
        invitefromhold_table AS( -- This CTE captures all events that could trigger a "Invite from Hold time".
                        -- This occurs when the "beginservice" event is hit
          SELECT
            namespace,
            event_name,
            event_time,
            client_id,
            service_count,
            office_id,
            agent_id,
            event_time invitefromhold_time
          FROM step1
          WHERE event_name in ('invitefromhold')
          ORDER BY event_time
          ),
        finish_table AS( -- This CTE captures all events that could trigger a "Finish time".
                        -- This occurs when the "finish" or "custermleft" event is hit
                        -- NOTE: there is also a count and inacurate_time flag here
                        --  also, inaccurate_time can be overidden by entries in servicebc.inaccurate_time_clientids
          SELECT
            namespace,
            event_name,
            event_time,
            step1.client_id,
            service_count,
            office_id,
            agent_id,
            count AS transactions_count,
            CASE
              WHEN inaccurate_time_clientids.client_id IS NOT NULL THEN true
              ELSE inaccurate_time
            END AS inaccurate_time,
            event_time finish_time
          FROM step1
          LEFT JOIN servicebc.inaccurate_time_clientids ON step1.client_id = inaccurate_time_clientids.client_id
          WHERE event_name in ('finish','customerleft')
          ORDER BY event_time
          ),
        chooseservice_table AS( -- This CTE captures all events that could trigger a "Chooseserviec time".
                        -- This occurs when the "chooseservice" event is hit
                        -- This is where we learn the service info.
                        -- NOTE: we want the LAST call for a given client_id/service_count
          SELECT
            namespace,
            event_name,
            event_time,
            client_id,
            service_count,
            office_id,
            agent_id,
            channel,
            program_id,
            parent_id,
            program_name,
            transaction_name,
            event_time chooseservice_time
          FROM step1
          WHERE event_name in ('chooseservice')
          ORDER BY event_time DESC
          ),
        hold_calculations AS ( --  build hold calculations. For a given client_id+service_count we use
              -- Sum of all (invitefromhold – hold) = sum (invitefromhold) - sum(hold)
          SELECT
            client_id,
            service_count,
            SUM(CASE WHEN event_name = 'hold' THEN DATEDIFF(milliseconds, event_time, current_date)/1000.0 END) +
            SUM(CASE WHEN event_name = 'invitefromhold' THEN DATEDIFF(milliseconds, current_date, event_time)/1000.0 END) AS hold_duration,
            COUNT( CASE WHEN event_name = 'hold' THEN 1 END) AS hold_count,
            COUNT( CASE WHEN event_name = 'invitefromhold' THEN 1 END) AS invitefromhold_count,
            -- "holdparity" if the number of hold and invitehold calls aren't balanced, we'll exclude these from caluclations below
            COUNT( CASE WHEN event_name = 'hold' THEN 1 END) - COUNT( CASE WHEN event_name = 'invitefromhold' THEN 1 END) AS holdparity
          FROM step1
          WHERE event_name in ('hold','invitefromhold')
          GROUP BY client_id, service_count
          ),
        calculations AS ( -- Here we build an array of all possible calcultation combinations
          SELECT
          welcome_time as t1,
          stand_time as t2,
          invite_time as t3,
          start_time as t4,
          finish_time as t5,
          welcome_table.client_id,
          finish_table.service_count,
          finish_table.transactions_count,
          CASE WHEN (welcome_time IS NOT NULL AND stand_time IS NOT NULL AND inaccurate_time <> True
          AND  ( (holdparity IS NULL OR holdparity = 0) AND invite_time IS NOT NULL AND start_time IS NOT NULL AND finish_time IS NOT NULL)
          ) THEN DATEDIFF(milliseconds, welcome_time, stand_time)/1000.0
              ELSE NULL
              END AS reception_duration,
          CASE WHEN (stand_time IS NOT NULL AND invite_time IS NOT NULL AND inaccurate_time <> True
AND  ( (holdparity IS NULL OR holdparity = 0) AND invite_time IS NOT NULL AND start_time IS NOT NULL AND finish_time IS NOT NULL)
) THEN DATEDIFF(milliseconds, stand_time, invite_time)/1000.0
              ELSE NULL
              END AS waiting_duration,
          CASE WHEN (invite_time IS NOT NULL AND start_time IS NOT NULL AND inaccurate_time <> True
AND  ( (holdparity IS NULL OR holdparity = 0) AND invite_time IS NOT NULL AND start_time IS NOT NULL AND finish_time IS NOT NULL)
) THEN DATEDIFF(milliseconds, invite_time, start_time)/1000.0
              ELSE NULL
              END AS prep_duration,
          CASE WHEN (inaccurate_time <> True
AND  ( (holdparity IS NULL OR holdparity = 0) AND invite_time IS NOT NULL AND start_time IS NOT NULL AND finish_time IS NOT NULL)
) THEN COALESCE(hold_duration,0)
              ELSE NULL
              END AS hold_duration,
          CASE WHEN (finish_time IS NOT NULL AND start_time IS NOT NULL AND inaccurate_time <> True AND hold_duration IS NOT NULL
AND  ( (holdparity IS NULL OR holdparity = 0) AND invite_time IS NOT NULL AND start_time IS NOT NULL AND finish_time IS NOT NULL)
)
                 THEN DATEDIFF(milliseconds, start_time, finish_time)/1000.0 - hold_duration
              WHEN (finish_time IS NOT NULL AND start_time IS NOT NULL AND inaccurate_time <> True AND hold_duration IS NULL
AND  ( (holdparity IS NULL OR holdparity = 0) AND invite_time IS NOT NULL AND start_time IS NOT NULL AND finish_time IS NOT NULL)
)
                 THEN DATEDIFF(milliseconds, start_time, finish_time)/1000.0
              ELSE NULL
              END AS serve_duration,
          CASE WHEN  ( (holdparity IS NOT NULL AND holdparity <> 0)
            OR invite_time IS NULL
            OR start_time IS NULL
            OR finish_time IS NULL) THEN True ELSE False END AS missing_calls

          FROM welcome_table
          LEFT JOIN finish_table ON welcome_table.client_id = finish_table.client_id
          LEFT JOIN stand_table ON welcome_table.client_id = stand_table.client_id AND finish_table.service_count = stand_table.service_count
          LEFT JOIN invite_table ON welcome_table.client_id = invite_table.client_id AND finish_table.service_count = invite_table.service_count
          LEFT JOIN start_table ON welcome_table.client_id = start_table.client_id AND finish_table.service_count = start_table.service_count
          LEFT JOIN hold_calculations ON finish_table.client_id = hold_calculations.client_id AND finish_table.service_count = hold_calculations.service_count
          ORDER BY welcome_time, stand_time, invite_time, start_time
        ),
        finalcalc AS (-- This is where we choose the correct one.
                      -- this rank needs to be matched below where we pick final set
                      -- NOTE: we want the:
                        -- first: welcome time (t1)
                        -- first: stand time (t2)
                        -- LAST: invite time (t3)
                        -- LAST: start time (t4)
                        -- first: finish_time (t5)
                      -- These are selected using the ROW_NUMBER partition method below.
                      -- NOTE: the ordering is chosen insite the PARTITION statement where we have a "t3 DESC".
          SELECT ranked.*
          FROM (
            SELECT *, ROW_NUMBER() OVER (PARTITION BY client_id, service_count ORDER BY t1, t2, t3 DESC, t4 DESC, t5) AS client_id_ranked
            -- we want the LAST invite_time = invite time
            FROM calculations
            ORDER BY client_id, service_count, t1, t2, t3 DESC, t4 DESC, t5
          ) AS ranked
          WHERE ranked.client_id_ranked = 1
        ),
        combined AS ( -- Combine it all together into a big table. Note that we still have duplicate entries here.
          SELECT
          welcome_table.namespace,
          welcome_table.client_id,
          finish_table.service_count,
          finish_table.event_name AS finish_event,
          welcome_table.office_id,
          office_info.site AS office_name,
          office_info.officesize AS office_size,
          office_info.area AS area_number,
          welcome_table.office_type AS office_type,
          welcome_table.agent_id,
          chooseservice_table.program_id,
          chooseservice_table.parent_id,
          chooseservice_table.program_name,
          transaction_name,
          chooseservice_table.channel,
          finish_table.inaccurate_time,
          c1.missing_calls,
          finish_table.transactions_count,
          welcome_time, stand_time, invite_time, start_time, finish_time, chooseservice_time, hold_time, invitefromhold_time,
          c1.reception_duration AS reception_duration,
          c1.waiting_duration AS waiting_duration,
          c1.prep_duration AS prep_duration,
          c1.hold_duration AS hold_duration,
          c1.serve_duration AS serve_duration
          FROM welcome_table
          LEFT JOIN finish_table ON welcome_table.client_id = finish_table.client_id
          LEFT JOIN stand_table ON welcome_table.client_id = stand_table.client_id AND finish_table.service_count = stand_table.service_count
          LEFT JOIN invite_table ON welcome_table.client_id = invite_table.client_id AND finish_table.service_count = invite_table.service_count
          LEFT JOIN start_table ON welcome_table.client_id = start_table.client_id AND finish_table.service_count = start_table.service_count
          LEFT JOIN chooseservice_table ON welcome_table.client_id = chooseservice_table.client_id AND finish_table.service_count = chooseservice_table.service_count
          LEFT JOIN hold_table ON welcome_table.client_id = hold_table.client_id AND finish_table.service_count = hold_table.service_count
          LEFT JOIN invitefromhold_table ON welcome_table.client_id = invitefromhold_table.client_id AND finish_table.service_count = invitefromhold_table.service_count
          LEFT JOIN servicebc.office_info ON servicebc.office_info.rmsofficecode = chooseservice_table.office_id AND end_date IS NULL -- for now, get the most recent office info
          LEFT JOIN finalcalc AS c1 ON welcome_table.client_id = c1.client_id AND finish_table.service_count = c1.service_count
        ),
          finalset AS ( -- Use the ROW_NUMBER method again to get a unique list for each client_id/service_count pair
            SELECT ranked.*
            FROM (
              SELECT *, ROW_NUMBER() OVER (PARTITION BY client_id, service_count
                  ORDER BY welcome_time, stand_time, invite_time DESC, start_time DESC, finish_time) AS client_id_ranked
                                            -- NOTE: the sort ordering here must match the order in finalcalc above
              FROM combined
              ORDER BY welcome_time, client_id, service_count
            ) AS ranked
            WHERE ranked.client_id_ranked = 1
          )
          SELECT finalset.*,-- ADD in the aggregate calculations summed over each of the services for a given client_id.
                            -- this is because the reception_duration only happens once per client
                            -- waiting_duration and prep_duration can be per client or per service. This gives us the per client version
                            -- below we use "sum_distinct" and "average_distinct" to report out on these versions
            SUM(c2.waiting_duration) AS waiting_duration_total,
            SUM(c2.prep_duration) AS prep_duration_total,
            SUM(c2.hold_duration) AS hold_duration_total,
            SUM(c2.serve_duration) AS serve_duration_total,
            -----------------------------------
            -- A sort field on Channel, so that "in-person" shows first in the sort order
            CASE WHEN channel = 'in-person'
               THEN '0-in-person'
               ELSE channel
              END AS channel_sort,
            -----------------------------------
            -- Add a flag for back office transactions
            CASE WHEN program_name = 'Back Office'
              THEN 'Back Office'
              ELSE 'Front Office'
              END as back_office,
            -----------------------------------
            -- Calculating zscores so we can filter out outliers
            -- To calculate the zscore, we use the formula (value - mean) / std_dev
            -- The std_dev and mean are to be calculated for all values for a given office
            --    (and with the code change below) program_id
            -- This is done using a Redshift Window Statement. For example, see:
            --         https://docs.aws.amazon.com/redshift/latest/dg/r_WF_AVG.html
            --
            -- We use the CASE statement to avoid dividing by zero. We could move this logic to the LookML below as well
            ---
            -- This example shows how to set it for both office_id and program_id.
            -- For now we just use office_id as we don't have a big enough data set yet
            --CASE WHEN (stddev(finalset.reception_duration) over (PARTITION BY finalset.office_id, finalset.program_id)) <> 0
            --    THEN (finalset.reception_duration - avg(finalset.reception_duration) over (PARTITION BY finalset.office_id, finalset.program_id))
            --              / (stddev(finalset.reception_duration) over (PARTITION BY finalset.office_id, finalset.program_id))
            --    ELSE NULL
            --END AS reception_duration_zscore,
            -----------------------------------
            CASE WHEN (stddev(finalset.reception_duration) over (PARTITION BY finalset.office_id)) <> 0
                THEN (finalset.reception_duration - avg(finalset.reception_duration) over (PARTITION BY finalset.office_id))
                   / (stddev(finalset.reception_duration) over (PARTITION BY finalset.office_id))
                ELSE NULL
            END AS reception_duration_zscore,
            CASE WHEN (stddev(finalset.waiting_duration) over (PARTITION BY finalset.office_id)) <> 0
                THEN (finalset.waiting_duration - avg(finalset.waiting_duration) over (PARTITION BY finalset.office_id))
                   / (stddev(finalset.waiting_duration) over (PARTITION BY finalset.office_id))
                ELSE NULL
            END AS waiting_duration_zscore,
            CASE WHEN (stddev(finalset.prep_duration) over (PARTITION BY finalset.office_id)) <> 0
                THEN (finalset.prep_duration - avg(finalset.prep_duration) over (PARTITION BY finalset.office_id))
                   / (stddev(finalset.prep_duration) over (PARTITION BY finalset.office_id))
                ELSE NULL
            END AS prep_duration_zscore,
            CASE WHEN (stddev(finalset.hold_duration) over (PARTITION BY finalset.office_id)) <> 0
                THEN (finalset.hold_duration - avg(finalset.hold_duration) over (PARTITION BY finalset.office_id))
                   / (stddev(finalset.hold_duration) over (PARTITION BY finalset.office_id))
                ELSE NULL
            END AS hold_duration_zscore,
            CASE WHEN (stddev(finalset.serve_duration) over (PARTITION BY finalset.office_id)) <> 0
                THEN (finalset.serve_duration - avg(finalset.serve_duration) over (PARTITION BY finalset.office_id))
                   / (stddev(finalset.serve_duration) over (PARTITION BY finalset.office_id))
                ELSE NULL
            END AS serve_duration_zscore,
            -- End zscore calculations
            ----------------------
            dd.isweekend::BOOLEAN,
            dd.isholiday::BOOLEAN,
            dd.sbcquarter, dd.lastdayofpsapayperiod::date,
            -- NOTE: We need to do an explicit timezone conversion here as we are casting ot a CHAR
            -- in other places the underlying UTC is converterd to Pacific time by Looker. It can't
            -- do it here as the data it will see below is a string, not a data
            to_char(CONVERT_TIMEZONE('UTC', 'US/Pacific', welcome_time), 'HH24:00-HH24:59') AS hourly_bucket,
            CASE WHEN date_part(minute, CONVERT_TIMEZONE('UTC', 'US/Pacific', welcome_time)) < 30
                THEN to_char(CONVERT_TIMEZONE('UTC', 'US/Pacific', welcome_time), 'HH24:00-HH24:29')
                ELSE to_char(CONVERT_TIMEZONE('UTC', 'US/Pacific', welcome_time), 'HH24:30-HH24:59')
            END AS half_hour_bucket,
            to_char(CONVERT_TIMEZONE('UTC', 'US/Pacific', welcome_time), 'HH24:MI:SS') AS date_time_of_day
          FROM finalset
          LEFT JOIN finalcalc AS c2 ON c2.client_id = finalset.client_id
          JOIN servicebc.datedimension AS dd on welcome_time::date = dd.datekey::date
          WHERE finalset.client_id_ranked = 1
            AND program_name IS NOT NULL
            AND office_name IS NOT NULL
            AND office_name <> ''
          GROUP BY namespace,
            finalset.client_id,
            finalset.service_count,
            finalset.office_id,
            office_name,
            office_size,
            area_number,
            office_type,
            agent_id,
            program_id,
            program_name,
            parent_id,
            transaction_name,
            channel,
            inaccurate_time,
            finish_event,
            finalset.transactions_count,
            finalset.missing_calls,
            welcome_time, stand_time, invite_time, start_time, finish_time, chooseservice_time, hold_time, invitefromhold_time,
            finalset.reception_duration,
            finalset.waiting_duration,
            finalset.prep_duration,
            finalset.hold_duration,
            finalset.serve_duration,
            finalset.client_id_ranked,
            dd.isweekend,
            dd.isholiday,
            dd.sbcquarter, dd.lastdayofpsapayperiod::date
          ORDER BY welcome_time, client_id, service_count
          ;;
          # https://docs.looker.com/data-modeling/learning-lookml/caching
      #persist_for: "1 hour"
      distribution_style: all
      sql_trigger_value: SELECT COUNT(*) FROM derived.cfms_step1 WHERE namespace <> 'TheQ_dev' AND client_id NOT IN (SELECT * from servicebc.bad_clientids ) ;;
    }

# Build measures and dimensions
    dimension: namespace {
      description: "The namespace identifies Production, Test, and Dev environments."
      type: string
      sql:  ${TABLE}.namespace ;;
    }
    measure: visits_count {
      description: "Count of distinct client IDs."
      type: number
      sql: COUNT (DISTINCT ${client_id} ) ;;
      group_label: "Counts"
    }
    measure: services_count {
      description: "Count of distinct services delivered for each visit."
      type: count
      group_label: "Counts"
    }
    measure: transactions_count {
      description: "Count of transactions. (eg. 20 property taxes for one service)"
      type: sum
      sql:  ${TABLE}.transactions_count ;;
      group_label: "Counts"
    }

    measure: dummy_for_back_office {
      type: number
      sql: 1=1 ;;
      hidden: yes
      drill_fields: [channel, program_name, prep_duration_total, reception_duration_total, serve_duration_total]
    }

    dimension: back_office {
      description: "Whether a given service was front or back office."
      type:  string
      sql:  ${TABLE}.back_office ;;
    }

    dimension: back_office_with_drill {
      description: "Whether a given service was front or back office (with drill to Service Time by Channel)."
      type:  string
      sql:  ${TABLE}.back_office ;;
      link: {
        label: "Service Time by Channel"
        url: "
        {% assign table_calc = '[{\"table_calculation\":\"total_time_credit\",\"label\":\"Total Time Credit\",\"expression\":\"${cfms_poc.prep_duration_total}+${cfms_poc.reception_duration_total}+${cfms_poc.serve_duration_total}\",\"value_format\":\"[h]:mm:ss\",\"value_format_name\":null,\"_kind_hint\":\"measure\",\"_type_hint\":\"number\"},{\"table_calculation\":\"total_time_credit_percent\",\"label\":\"Total Time Credit Percent\",\"expression\":\"${total_time_credit}/sum(pivot_row(${total_time_credit}))\",\"value_format\":null,\"value_format_name\":\"percent_1\",\"_kind_hint\":\"measure\",\"_type_hint\":\"number\"}]' %}
        {% assign filter_config = '{\"cfms_poc.office_name\":[{\"type\":\"=\",\"values\":[{\"constant\":\"\"},{}],\"id\":3,\"error\":false}],\"cfms_poc.date\":[{\"type\":\"anytime\",\"values\":[{},{}],\"id\":4,\"error\":false}],\"cfms_poc.back_office\":[{\"type\":\"=\",\"values\":[{\"constant\":\"\"},{}],\"id\":5,\"error\":false}]}' %}
        {% assign vis_config = '
        {\"stacking\":\"percent\" ,
        \"colors\":[\"#991426\" ,
        \"#a9c574\" ,
        \"#929292\" ,
        \"#9fdee0\" ,
        \"#1f3e5a\" ,
        \"#90c8ae\" ,
        \"#92818d\" ,
        \"#c5c6a6\" ,
        \"#82c2ca\" ,
        \"#cee0a0\" ,
        \"#928fb4\" ,
        \"#9fc190\"] ,
        \"show_value_labels\":true ,
        \"label_density\":25 ,
        \"legend_position\":\"center\" ,
        \"x_axis_gridlines\":false ,
        \"y_axis_gridlines\":true ,
        \"show_view_names\":false ,
        \"point_style\":\"none\" ,
        \"series_colors\":{} ,
        \"limit_displayed_rows\":false ,
        \"y_axis_combined\":true ,
        \"show_y_axis_labels\":true ,
        \"show_y_axis_ticks\":true ,
        \"y_axis_tick_density\":\"default\" ,
        \"y_axis_tick_density_custom\":5 ,
        \"show_x_axis_label\":true ,
        \"show_x_axis_ticks\":true ,
        \"x_axis_scale\":\"auto\" ,
        \"y_axis_scale_mode\":\"linear\" ,
        \"x_axis_reversed\":false ,
        \"y_axis_reversed\":false ,
        \"plot_size_by_field\":false ,
        \"ordering\":\"none\" ,
        \"show_null_labels\":false ,
        \"show_totals_labels\":false ,
        \"show_silhouette\":false ,
        \"totals_color\":\"#808080\" ,
        \"type\":\"looker_column\" ,
        \"hidden_fields\":[\"cfms_poc.prep_duration_total\" ,
        \"cfms_poc.reception_duration_total\" ,
        \"cfms_poc.serve_duration_total\" ,
        \"total_time_credit_percent\"] ,
        \"y_axes\":[]}' %}

        {{ dummy_for_back_office._link }}&vis_config={{ vis_config | encode_uri }}&pivots=cfms_poc.channel&sorts=cfms_poc.channel 0,total_time_credit desc 3&limit=1000&column_limit=50&filter_config={{ filter_config | encode_uri }}&dynamic_fields={{ table_calc | replace: '  ', '' | encode_uri }}"

      }
    }


    dimension: finish_type {
      description: "Whether the client finished successfully, left, or their ticket is still open."
      type:  string
      sql: CASE
        WHEN ${TABLE}.finish_event = 'finish' THEN 'Finish'
        WHEN ${TABLE}.finish_event = 'customerleft' THEN 'Customer Left'
        ELSE 'Open Ticket'
      END;;
    }
    # Time based measures
    measure: reception_duration_total {
      description: "Total reception duration."
      type:  sum
      sql: (1.00 * ${TABLE}.reception_duration)/(60*60*24) ;;
      value_format: "[h]:mm:ss"
      group_label: "Reception Duration"
    }
    measure: reception_duration_per_visit_max {
      description: "Maximum reception duration."
      type:  max
      sql: (1.00 * ${TABLE}.reception_duration)/(60*60*24) ;;
      value_format: "[h]:mm:ss"
      group_label: "Reception Duration"
    }
    measure: reception_duration_per_visit_average {
      description: "Average reception duration."
      type:  average
      sql: (1.00 * ${TABLE}.reception_duration)/(60*60*24) ;;
      value_format: "[h]:mm:ss"
      group_label: "Reception Duration"
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
    }

    # See here to understand the use of sum_distinct and average_distinct:
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
    }
    measure: serve_duration_per_visit_max {
      description: "Maximum total serve duration per visit."
      type: max
      sql: (1.00 * ${TABLE}.serve_duration_total)/(60*60*24) ;;
      value_format: "[h]:mm:ss"
      group_label: "Serve Duration"
    }

    #measure: serve_duration_total_raw {
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

    # Time based dimentions
    dimension: reception_duration_per_visit {
      description: "Reception duration for this visit."
      type:  number
      sql: (1.00 * ${TABLE}.reception_duration)/(60*60*24) ;;
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

    # buckets
    # Serve Duration by Service
    dimension: serve_duration_bucket {
      description: "Serve duration for this individual service in buckets (0-5, 5-20, 20-60, 60+ minutes)."
      case: {
        when: {
          sql: ${TABLE}.serve_duration < 300 ;;
          label: "0-5"
        }
        when: {
          sql:  ${TABLE}.serve_duration < 1200 ;;
          label: "5-20"
        }
        when: {
          sql: ${TABLE}.serve_duration < 3600 ;;
          label: "20-60"
        }
        when: {
          sql: ${TABLE}.serve_duration >= 3600 ;;
          label: "60+"
        }
        else:"Unknown"
      }
      group_label: "Durations"
    }
    measure: serve_duration_bucket_0_5 {
      description: "Count of individual services with serve duration 0-5 minutes."
      type:  sum
      sql:  CASE WHEN ${TABLE}.serve_duration < 300 THEN 1
              ELSE 0
              END;;
      label: "Serve Duration: 0-5"
      group_label: "Duration Buckets"
    }
    measure: serve_duration_bucket_5_20 {
      description: "Count of individual services with serve duration 5-20 minutes."
      type:  sum
      sql:  CASE WHEN ${TABLE}.serve_duration >= 300 AND ${TABLE}.serve_duration < 1200 THEN 1
              ELSE 0
              END;;
      label: "Serve Duration: 5-20"
      group_label: "Duration Buckets"
    }
    measure: serve_duration_bucket_20_60 {
      description: "Count of individual services with serve duration 20-60 minutes."
      type:  sum
      sql:  CASE WHEN ${TABLE}.serve_duration >= 1200 AND ${TABLE}.serve_duration < 3600 THEN 1
              ELSE 0
              END;;
      label: "Serve Duration: 20-60"
      group_label: "Duration Buckets"
    }
    measure: serve_duration_bucket_60_plus {
      description: "Count of individual services with serve duration 60+ minutes."
      type:  sum
      sql:  CASE WHEN ${TABLE}.serve_duration >= 3600 THEN 1
              ELSE 0
              END;;
      label: "Serve Duration: 60+"
      group_label: "Duration Buckets"
    }

    # Waiting Duration by Visit
    dimension: waiting_duration_bucket {
      description: "Waiting duration for this individual service in buckets (0-5, 5-20, 20-60, 60+ minutes)."
      case: {
        when: {
          sql: ${TABLE}.waiting_duration_total < 300 ;;
          label: "0-5"
        }
        when: {
          sql:  ${TABLE}.waiting_duration_total < 1200 ;;
          label: "5-20"
        }
        when: {
          sql: ${TABLE}.waiting_duration_total < 3600 ;;
          label: "20-60"
        }
        when: {
          sql: ${TABLE}.waiting_duration_total >= 3600 ;;
          label: "60+"
        }
        else:"Unknown"
      }
      group_label: "Durations"
    }
    measure: waiting_duration_bucket_0_5 {
      description: "Count of individual services with waiting duration 0-5 minutes."
      type:  sum
      sql:  CASE WHEN ${TABLE}.waiting_duration_total < 300 THEN 1
              ELSE 0
              END;;
      label: "Waiting Duration: 0-5"
      group_label: "Duration Buckets"
    }
    measure: waiting_duration_bucket_5_20 {
      description: "Count of individual services with waiting duration 5-20 minutes."
      type:  sum
      sql:  CASE WHEN ${TABLE}.waiting_duration_total >= 300 AND ${TABLE}.waiting_duration_total < 1200 THEN 1
              ELSE 0
              END;;
      label: "Waiting Duration: 5-20"
      group_label: "Duration Buckets"
    }
    measure: waiting_duration_bucket_20_60 {
      description: "Count of individual services with waiting duration 20-60 minutes."
      type:  sum
      sql:  CASE WHEN ${TABLE}.waiting_duration_total >= 1200 AND ${TABLE}.waiting_duration_total < 3600 THEN 1
              ELSE 0
              END;;
      label: "Waiting Duration: 20-60"
      group_label: "Duration Buckets"
    }
    measure: waiting_duration_bucket_60_plus {
      description: "Count of individual services with waiting duration 60+ minutes."
      type:  sum
      sql:  CASE WHEN ${TABLE}.waiting_duration_total >= 3600 THEN 1
              ELSE 0
              END;;
      label: "Waiting Duration: 60+"
      group_label: "Duration Buckets"
    }

    # Outlier dimensions
    dimension: reception_duration_zscore {
      type:  number
      sql: ${TABLE}.reception_duration_zscore ;;
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
    dimension: reception_duration_outlier {
      description: "Is the reception duration greater than 3 standard deviations from the average?"
      type:  yesno
      sql: abs(${TABLE}.reception_duration_zscore) >= 3 ;;
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
    measure: count_of_days {
      type: number
      sql: count(distinct date(${TABLE}.welcome_time));;
      hidden: yes
    }
    dimension: p_key {
      primary_key: yes
      hidden: yes
      sql: ${client_id} ;;
      #sql: ${client_id} || ${program_id} || ${service_count} ;;
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
      sql:  ${TABLE}.welcome_time ;;
      group_label: "Date"
    }
    dimension: week {
      type:  date_week_of_year
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
    dimension: day_of_month {
      type:  date_day_of_month
      sql:  ${TABLE}.welcome_time ;;
      group_label: "Date"
    }
    dimension: day_of_week {
      type:  date_day_of_week
      sql:  ${TABLE}.welcome_time ;;
      group_label: "Date"
    }
    dimension: day_of_week_number {
      type:  date_day_of_week_index
      sql:  ${TABLE}.welcome_time + interval '1 day' ;;
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
      sql:  ${TABLE}.welcome_time ;;
      group_label:  "Date"
    }
    dimension: fiscal_month {
      type:  date_fiscal_month_num
      sql:  ${TABLE}.welcome_time ;;
      group_label:  "Date"
    }
    dimension: fiscal_quarter {
      type:  date_fiscal_quarter
      sql:  ${TABLE}.welcome_time ;;
      group_label:  "Date"
    }
    dimension: fiscal_quarter_of_year {
      type:  date_fiscal_quarter_of_year
      sql:  ${TABLE}.welcome_time ;;
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
    dimension: stand_time {
      type: date_time
      sql: ${TABLE}.stand_time ;;
      group_label: "Timing Points"
    }
    dimension: invite_time {
      type: date_time
      sql: ${TABLE}.invite_time ;;
      group_label: "Timing Points"
    }
    dimension: start_time {
      type: date_time
      sql: ${TABLE}.start_time ;;
      group_label: "Timing Points"
    }
    dimension: chooseservice_time {
      type: date_time
      sql:  ${TABLE}.chooseservice_time ;;
      group_label: "Timing Points"
    }
    dimension: finish_time {
      type: date_time
      sql: ${TABLE}.finish_time ;;
      group_label: "Timing Points"
    }
    dimension: hold_time {
      type: date_time
      sql: ${TABLE}.hold_time ;;
      group_label: "Timing Points"
    }
    dimension: invitefromhold_time {
      type: date_time
      sql: ${TABLE}.invitefromhold_time ;;
      group_label: "Timing Points"
    }
    dimension: client_id {
      type: number
      sql: ${TABLE}.client_id ;;
      html: {{ rendered_value }} ;;
    }
    dimension: service_count {
      description: "The number of this service within a visit. (ie. the 1st, 2nd, 3rd... service for a given visit.)"
      type: number
      sql:  ${TABLE}.service_count ;;
    }
    dimension: office_id {
      type: number
      sql: ${TABLE}.office_id ;;
      group_label: "Office Info"
      hidden: yes
    }
    dimension: office_name {
      type:  string
      sql:  ${TABLE}.office_name ;;
      group_label: "Office Info"
    }
    dimension: office_size {
      type:  string
      sql:  ${TABLE}.office_size ;;
      group_label: "Office Info"
    }
    dimension: area_number {
      type:  number
      sql:  ${TABLE}.area_number ;;
      group_label: "Office Info"
    }
    dimension: office_type {
      type:  string
      sql:  ${TABLE}.office_type ;;
      group_label: "Office Info"
    }
    dimension: agent_id {
      type: number
      sql: ${TABLE}.agent_id ;;
    }
    dimension: program_id {
      description: "The internal ID number for this program."
      type: string
      sql: ${TABLE}.program_id ;;
      group_label: "Program Information"
    }
    dimension: parent_id {
      description: "The internal ID number for the parent of this program."
      type: string
      sql: ${TABLE}.parent_id ;;
      group_label: "Program Information"
    }


    measure: dummy {
      type: number
      sql: 1=1 ;;
      hidden: yes
      drill_fields: [transaction_name, channel, transactions_count]
    }
    dimension: program_name {
      type: string
      sql: ${TABLE}.program_name ;;
      group_label: "Program Information"
    }

    dimension: program_name_with_drill_1{
      type: string
      sql: ${TABLE}.program_name ;;
      group_label: "Program Information"
      description: "Program Name with drill to Program Distribution."
      link: {
        label: "Program Distribution"
        url: "
        {% assign table_calc = '[]' %}
        {% assign filter_config = '{\"cfms_poc.office_name\":[{\"type\":\"=\",\"values\":[{\"constant\":\"\"},{}],\"id\":6,\"error\":false}],\"cfms_poc.program_name\":[{\"type\":\"=\",\"values\":[{\"constant\":\"Other\"},{}],\"id\":7,\"error\":false}],\"cfms_poc.date\":[{\"type\":\"past\",\"values\":[{\"constant\":\"60\",\"unit\":\"day\"},{}],\"id\":8,\"error\":false}]}' %}
        {% assign vis_config = '
        {\"stacking\":\"normal\" ,
        \"colors\":[\"#991426\" ,
        \"#a9c574\" ,
        \"#929292\" ,
        \"#9fdee0\" ,
        \"#1f3e5a\" ,
        \"#90c8ae\" ,
        \"#92818d\" ,
        \"#c5c6a6\" ,
        \"#82c2ca\" ,
        \"#cee0a0\" ,
        \"#928fb4\" ,
        \"#9fc190\"] ,
        \"show_value_labels\":true ,
        \"label_density\":25 ,
        \"legend_position\":\"center\" ,
        \"x_axis_gridlines\":false ,
        \"y_axis_gridlines\":true ,
        \"show_view_names\":false ,
        \"point_style\":\"none\" ,
        \"series_colors\":{} ,
        \"limit_displayed_rows\":false ,
        \"y_axes\":[] ,
        \"y_axis_combined\":true ,
        \"show_y_axis_labels\":true ,
        \"show_y_axis_ticks\":true ,
        \"y_axis_tick_density\":\"default\" ,
        \"y_axis_tick_density_custom\":5 ,
        \"show_x_axis_label\":true ,
        \"show_x_axis_ticks\":true ,
        \"x_axis_scale\":\"auto\" ,
        \"y_axis_scale_mode\":\"linear\" ,
        \"x_axis_reversed\":false ,
        \"y_axis_reversed\":false ,
        \"plot_size_by_field\":false ,
        \"ordering\":\"desc\" ,
        \"show_null_labels\":false ,
        \"show_dropoff\":false ,
        \"show_totals_labels\":false ,
        \"show_silhouette\":false ,
        \"totals_color\":\"#808080\" ,
        \"type\":\"looker_column\" ,
        \"hidden_fields\":[\"calculation_2\"]}' %}

        {{ dummy._link }}&vis_config={{ vis_config | encode_uri }}&pivots=cfms_poc.channel&sorts=cfms_poc.channel 0,cfms_poc.transactions_count desc 6&limit=1000&column_limit=50&row_total=right&filter_config={{ filter_config| encode_uri }}&dynamic_fields={{ table_calc | replace: '  ', '' | encode_uri }}"
      }
    }
    measure: dummy_service_count {
      type: number
      sql: 1=1 ;;
      hidden: yes
      drill_fields: [transaction_name, prep_duration_per_visit_average, serve_duration_per_visit_average, reception_duration_per_visit_average]
    }

    dimension: program_name_with_drill_2 {
      type: string
      sql: ${TABLE}.program_name ;;
      group_label: "Program Information"
      description: "Program Name with drill to ervice Time."
      link: {
        label: "Service Time"
        url: "
        {% assign table_calc = '[{\"table_calculation\":\"average_service_time\",\"label\":\"Average Service Time\",\"expression\":\"${cfms_poc.prep_duration_per_visit_average}+${cfms_poc.serve_duration_per_visit_average}+${cfms_poc.reception_duration_per_visit_average}\",\"value_format\":\"[h]:mm:ss\",\"value_format_name\":null,\"_kind_hint\":\"measure\",\"_type_hint\":\"number\"}]' %}
        {% assign filter_config = '{\"cfms_poc.office_name\":[{\"type\":\"=\",\"values\":[{\"constant\":\"\"},{}],\"id\":8,\"error\":false}],\"cfms_poc.program_name\":[{\"type\":\"=\",\"values\":[{\"constant\":\"\"},{}],\"id\":9,\"error\":false}],\"cfms_poc.date\":[{\"type\":\"anytime\",\"values\":[{},{}],\"id\":10,\"error\":false}],\"cfms_poc.back_office\":[{\"type\":\"=\",\"values\":[{\"constant\":\"Front Office\"},{}],\"id\":11,\"error\":false}]}' %}
        {% assign vis_config = '
        {\"stacking\":\"\" ,
        \"colors\":[\"#991426\" ,
        \"#a9c574\" ,
        \"#929292\" ,
        \"#9fdee0\" ,
        \"#1f3e5a\" ,
        \"#90c8ae\" ,
        \"#92818d\" ,
        \"#c5c6a6\" ,
        \"#82c2ca\" ,
        \"#cee0a0\" ,
        \"#928fb4\" ,
        \"#9fc190\"] ,
        \"show_value_labels\":false ,
        \"label_density\":25 ,
        \"legend_position\":\"center\" ,
        \"x_axis_gridlines\":false ,
        \"y_axis_gridlines\":true ,
        \"show_view_names\":false ,
        \"point_style\":\"none\" ,
        \"series_colors\":{} ,
        \"limit_displayed_rows\":false ,
        \"y_axis_combined\":true ,
        \"show_y_axis_labels\":true ,
        \"show_y_axis_ticks\":true ,
        \"y_axis_tick_density\":\"default\" ,
        \"y_axis_tick_density_custom\":5 ,
        \"show_x_axis_label\":true ,
        \"show_x_axis_ticks\":true ,
        \"x_axis_scale\":\"auto\" ,
        \"y_axis_scale_mode\":\"linear\" ,
        \"x_axis_reversed\":false ,
        \"y_axis_reversed\":false ,
        \"plot_size_by_field\":false ,
        \"ordering\":\"none\" ,
        \"show_null_labels\":false ,
        \"show_totals_labels\":false ,
        \"show_silhouette\":false ,
        \"totals_color\":\"#808080\" ,
        \"type\":\"looker_column\" ,
        \"hidden_fields\":[\"cfms_poc.prep_duration_per_visit_average\" ,
        \"cfms_poc.reception_duration_per_visit_average\" ,
        \"cfms_poc.serve_duration_per_visit_average\"] ,
        \"y_axes\":[]}' %}

        {{ dummy_service_count._link }}&vis_config={{ vis_config | encode_uri }}&sorts=average_service_time desc&limit=1000&column_limit=50&filter_config={{ filter_config | encode_uri }}&dynamic_fields={{ table_calc | replace: '  ', '' | encode_uri }}"

      }
    }

    dimension: transaction_name {
      type: string
      sql: ${TABLE}.transaction_name ;;
      group_label: "Program Information"
    }

    # Apply a sort field, see: https://docs.looker.com/reference/field-params/order_by_field
    dimension: channel_sort {
      type: string
      sql: ${TABLE}.channel_sort ;;
      hidden: yes
    }
    dimension: channel {
      type: string
      sql: ${TABLE}.channel ;;
      order_by_field: channel_sort
    }
    dimension: inaccurate_time {
      description: "A flag to indicate that the timing on this service is unreliable. It will be included in counts, but not timing averages."
      type: yesno
      sql: ${TABLE}.inaccurate_time ;;
    }
    dimension: missing_calls {
      description: "A flag to indicate that there are missing timing points for this service is unreliable. It will be included in counts, but not timing averages."
      type: yesno
      sql: ${TABLE}.missing_calls ;;
    }

    # flexible_filter_date_range provides the necessary filter for Explores of current_period and last_period
    # where current_period captures the sessions from within the date range selected
    # and compares to last_period, which is the same duration as current_period, but
    # is offset such that it's end date exactly precedes current_period's start date
    filter: flexible_filter_date_range {
      description: "This provides a date range used by dimensions in the Flexible Filters group. NOTE: On its own it does not do anything."
      type:  date
    }

    # Documentation references:
    # Looker Liquid Variables:
    #   https://docs.looker.com/reference/liquid-variables
    # Using date_start and date_end with date filters:
    #   https://discourse.looker.com/t/using-date-start-and-date-end-with-date-filters/2880

    # period_difference calculates the number of days between the start and end dates
    # selected on the flexible_filter_date_range filter, as selected in an Explore.
    # This is used by last_period to calculate its duration.
    dimension: period_difference {
      group_label: "Flexible Filter"
      type: number
      sql: DATEDIFF(DAY, {% date_start flexible_filter_date_range %}, {% date_end flexible_filter_date_range %}) ;;
      hidden: yes
    }

    # current_period filters sessions that are within the start and end range
    # of the flexible_filter_date_range filter, as selected in an Explore.
    # the flexible_filter_date_range filter is required for current_period
    # the last_period dimension is required to compare against current_period
    dimension: current_period {
      type: yesno
      group_label: "Flexible Filter"
      sql: ${TABLE}.welcome_time >= {% date_start flexible_filter_date_range %}
        AND ${TABLE}.welcome_time <= {% date_end flexible_filter_date_range %}   ;;
      hidden: yes
    }

    # last_period selects the the sessions that occurred immediately prior to the current_period and
    # over the same duration the current_period. Used in an explore, the flexible_filter_date_range filter provides
    # the necessary is how input for date ranges to compare in this way.
    # the flexible_filter_date_range filter is required for last_period
    # the current_period dimension is required to compare against last_period
    dimension: last_period {
      group_label: "Flexible Filter"
      type: yesno
      sql: ${TABLE}.welcome_time >= DATEADD(DAY, -${period_difference}, {% date_start flexible_filter_date_range %})
        AND ${TABLE}.welcome_time <= DATEADD(DAY, -${period_difference}, {% date_end flexible_filter_date_range %}) ;;
      required_fields: [current_period]
      hidden: yes
    }

    # is_in_current_period_or_last_period determines which sessions occur between the start of the last_period
    # and the end of the current_period, as selected on the flexible_filter_date_range filter in an Explore.
    filter: is_in_current_period_or_last_period {
      type: yesno
      sql:  ${TABLE}.welcome_time >= DATEADD(DAY, -${period_difference}, {% date_start flexible_filter_date_range %})
        AND ${TABLE}.welcome_time <= {% date_end flexible_filter_date_range %} ;;
      hidden: yes
    }

    # date_window tags rows as being one of either the current or the last period according to their welcome_time
    # if the welcome time falls outside either, or is otherwise corrupted, it will return unknown.
    dimension: date_window {
      description: "Pivot on Date Window to compare measures between the current and last periods, use with Comparison Date"
      type: string
      group_label: "Flexible Filter"
      case: {
        when: {
          sql: ${current_period} ;;
          label: "current_period"
        }
        when: {
          sql: ${last_period} ;;
          label: "last_period"
        }
        else: "unknown"
      }
    }

    # comparison_date returns dates in the current_period providing a positive offset of
    # the last_period date range by. Exploring comparison_date with any Measure and a pivot
    # on date_window results in a pointwise comparison of current and last periods
    dimension: comparison_date {
      description: "Comparison Date offsets measures from the last period to appear in the range of the current period,
      allowing a pairwise comparison between these periods when used with Date Window."
      group_label: "Flexible Filter"
      required_fields: [date_window]
      type: date
      sql:
       CASE
         WHEN ${date_window} = 'current_period' THEN
           ${TABLE}.welcome_time
         WHEN ${date_window} = 'last_period' THEN
           DATEADD(DAY,${period_difference},${TABLE}.welcome_time)
         ELSE
           NULL
       END ;;
    }

    # on_final_date will be yes for welcome_times in the last day of the current_period.
    # since date ranges are selected "until (before)", this means any welcome time over the day that is one
    # prior to the date selected as the end date in the flexible_filter_date_range filter
    dimension: on_final_date {
      type:  yesno
      group_label: "Flexible Filter"
      sql: ${TABLE}.welcome_time >= DATEADD(DAY, -1, {% date_end flexible_filter_date_range %})
        AND ${TABLE}.welcome_time <= {% date_end flexible_filter_date_range %}   ;;
      hidden: yes
    }
  }
