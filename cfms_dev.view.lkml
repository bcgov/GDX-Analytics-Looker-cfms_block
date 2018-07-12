view: cfms_dev {
  derived_table: {
    sql: WITH step1 AS( -- Build a CTE containing all events using the name_tracker "CFMS_poc"
                        -- this will include all fields for all possible events. We will then
                        -- build the individual tables from this big one below
                        -- NOTE: we are ignoring instances where there is no client_id
          SELECT
            event_name,
            -- CONVERT_TIMEZONE('UTC', 'US/Pacific', derived_tstamp) AS
            derived_tstamp AS event_time,
            client_id,
            service_count,
            office_id,
            office_type,
            agent_id,
            channel,
            program_id,
            parent_id,
            program_name,
            transaction_name,
            count,
            inaccurate_time
          FROM atomic.events AS ev
          LEFT JOIN atomic.ca_bc_gov_cfmspoc_agent_2 AS a
              ON ev.event_id = a.root_id
          LEFT JOIN atomic.ca_bc_gov_cfmspoc_citizen_3 AS c
              ON ev.event_id = c.root_id
          LEFT JOIN atomic.ca_bc_gov_cfmspoc_office_1 AS o
              ON ev.event_id = o.root_id
          LEFT JOIN atomic.ca_bc_gov_cfmspoc_chooseservice_2 AS cs
              ON ev.event_id = cs.root_id
          LEFT JOIN atomic.ca_bc_gov_cfmspoc_finish_1 AS fi
              ON ev.event_id = fi.root_id
          LEFT JOIN atomic.ca_bc_gov_cfmspoc_hold_1 AS ho
              ON ev.event_id = ho.root_id
          WHERE name_tracker = 'CFMS_poc' AND client_id IS NOT NULL
          ),
      welcome_table AS( -- This CTE captures all events that could trigger a "Welcome time".
                        -- This occurs when the "addcitizen" event is hit
          SELECT
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
          SELECT
            event_name,
            event_time,
            client_id,
            service_count,
            office_id,
            agent_id,
            count,
            inaccurate_time,
            event_time finish_time
          FROM step1
          WHERE event_name in ('finish','customerleft')
          ORDER BY event_time
          ),
        chooseservice_table AS( -- This CTE captures all events that could trigger a "Chooseserviec time".
                        -- This occurs when the "chooseservice" event is hit
                        -- This is where we learn the service info.
                        -- NOTE: we want the LAST call for a given client_id/service_count
          SELECT
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
        hold_calculations AS ( --  build hold calculations. For a given client_id+service_count we use  Sum of all (invitefromhold – hold) = sum (invitefromhold) - sum(hold)
          SELECT
            client_id,
            service_count,
            SUM(CASE WHEN event_name = 'hold' THEN DATEDIFF(seconds, event_time, current_date) END) +
            SUM(CASE WHEN event_name = 'invitefromhold' THEN DATEDIFF(seconds, current_date, event_time) END) AS hold_duration,
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
          CASE WHEN (welcome_time IS NOT NULL and stand_time IS NOT NULL) THEN DATEDIFF(seconds, welcome_time, stand_time)
              ELSE NULL
              END AS reception_duration,
          CASE WHEN (stand_time IS NOT NULL and invite_time IS NOT NULL) THEN DATEDIFF(seconds, stand_time, invite_time)
              ELSE NULL
              END AS waiting_duration,
          CASE WHEN (invite_time IS NOT NULL and start_time IS NOT NULL) THEN DATEDIFF(seconds, invite_time, start_time)
              ELSE NULL
              END AS prep_duration,
          COALESCE(hold_duration,0) AS hold_duration,
          CASE WHEN (finish_time IS NOT NULL and start_time IS NOT NULL) THEN DATEDIFF(seconds, start_time, finish_time) - COALESCE(hold_duration,0)
              ELSE NULL
              END AS serve_duration

          FROM welcome_table
          LEFT JOIN stand_table ON welcome_table.client_id = stand_table.client_id
          LEFT JOIN finish_table ON welcome_table.client_id = finish_table.client_id
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
            SELECT *, ROW_NUMBER() OVER (PARTITION BY client_id, service_count ORDER BY t1, t2, t3 DESC, t4 DESC, t5) AS client_id_ranked -- we want the LAST invite_time = invite time
            FROM calculations
            ORDER BY client_id, service_count, t1, t2, t3 DESC, t4 DESC, t5
          ) AS ranked
          WHERE ranked.client_id_ranked = 1
        ),
        combined AS ( -- Combine it all together into a big table. Note that we still have duplicate entries here.
          SELECT
          welcome_table.client_id,
          finish_table.service_count,
          welcome_table.office_id,
          office_info.site AS office_name,
          office_info.officesize AS office_size,
          office_info.area AS area_number,
          welcome_table.office_type AS office_type,
          welcome_table.agent_id,
          chooseservice_table.program_id,
          chooseservice_table.program_name,
          transaction_name,
          chooseservice_table.channel,
          finish_table.inaccurate_time,
          welcome_time, stand_time, invite_time, start_time, finish_time, chooseservice_time, hold_time, invitefromhold_time,
          c1.reception_duration,
          c1.waiting_duration,
          c1.prep_duration,
          c1.hold_duration,
          c1.serve_duration
          FROM welcome_table
          LEFT JOIN stand_table ON welcome_table.client_id = stand_table.client_id
          LEFT JOIN finish_table ON welcome_table.client_id = finish_table.client_id
          LEFT JOIN invite_table ON welcome_table.client_id = invite_table.client_id AND finish_table.service_count = invite_table.service_count
          LEFT JOIN start_table ON welcome_table.client_id = start_table.client_id AND finish_table.service_count = start_table.service_count
          LEFT JOIN chooseservice_table ON welcome_table.client_id = chooseservice_table.client_id AND finish_table.service_count = chooseservice_table.service_count
          LEFT JOIN hold_table ON welcome_table.client_id = hold_table.client_id AND finish_table.service_count = hold_table.service_count
          LEFT JOIN invitefromhold_table ON welcome_table.client_id = invitefromhold_table.client_id AND finish_table.service_count = invitefromhold_table.service_count
          LEFT JOIN servicebc.office_info ON servicebc.office_info.id = chooseservice_table.office_id AND end_date IS NULL -- for now, get the most recent office info
          JOIN finalcalc AS c1 ON welcome_table.client_id = c1.client_id AND finish_table.service_count = c1.service_count
        ),
          finalset AS ( -- Use the ROW_NUMBER method again to get a unique list for each client_id/service_count pair
            SELECT ranked.*
            FROM (
              SELECT *, ROW_NUMBER() OVER (PARTITION BY client_id, service_count ORDER BY welcome_time, stand_time, invite_time DESC, start_time DESC, finish_time) AS client_id_ranked
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
            SUM(c2.waiting_duration) AS waiting_duration_sum,
            SUM(c2.prep_duration) AS prep_duration_sum,
            SUM(c2.hold_duration) AS hold_duration_sum,
            SUM(c2.serve_duration) AS serve_duration_sum,
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
            --    THEN (finalset.reception_duration - avg(finalset.reception_duration) over (PARTITION BY finalset.office_id, finalset.program_id)) / (stddev(finalset.reception_duration) over (PARTITION BY finalset.office_id, finalset.program_id))
            --    ELSE NULL
            --END AS reception_duration_zscore,
            -----------------------------------
            CASE WHEN (stddev(finalset.reception_duration) over (PARTITION BY finalset.office_id)) <> 0
                THEN (finalset.reception_duration - avg(finalset.reception_duration) over (PARTITION BY finalset.office_id)) / (stddev(finalset.reception_duration) over (PARTITION BY finalset.office_id))
                ELSE NULL
            END AS reception_duration_zscore,
            CASE WHEN (stddev(finalset.waiting_duration) over (PARTITION BY finalset.office_id)) <> 0
                THEN (finalset.waiting_duration - avg(finalset.waiting_duration) over (PARTITION BY finalset.office_id)) / (stddev(finalset.waiting_duration) over (PARTITION BY finalset.office_id))
                ELSE NULL
            END AS waiting_duration_zscore,
            CASE WHEN (stddev(finalset.prep_duration) over (PARTITION BY finalset.office_id)) <> 0
                THEN (finalset.prep_duration - avg(finalset.prep_duration) over (PARTITION BY finalset.office_id)) / (stddev(finalset.prep_duration) over (PARTITION BY finalset.office_id))
                ELSE NULL
            END AS prep_duration_zscore,
            CASE WHEN (stddev(finalset.hold_duration) over (PARTITION BY finalset.office_id)) <> 0
                THEN (finalset.hold_duration - avg(finalset.hold_duration) over (PARTITION BY finalset.office_id)) / (stddev(finalset.hold_duration) over (PARTITION BY finalset.office_id))
                ELSE NULL
            END AS hold_duration_zscore,
            CASE WHEN (stddev(finalset.serve_duration) over (PARTITION BY finalset.office_id)) <> 0
                THEN (finalset.serve_duration - avg(finalset.serve_duration) over (PARTITION BY finalset.office_id)) / (stddev(finalset.serve_duration) over (PARTITION BY finalset.office_id))
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
          LEFT JOIN finalcalc AS c2 ON c2.client_id = finalset.client_id AND inaccurate_time <> True
          JOIN servicebc.datedimension AS dd on welcome_time::date = dd.datekey::date
          WHERE finalset.client_id_ranked = 1
            AND program_name IS NOT NULL
            AND office_name IS NOT NULL
            AND office_name <> ''
          GROUP BY finalset.client_id,
            finalset.service_count,
            finalset.office_id,
            office_name,
            office_size,
            area_number,
            office_type,
            agent_id,
            program_id,
            program_name,
            transaction_name,
            channel,
            inaccurate_time,
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
      persist_for: "1 hour"
      distribution_style: all
    }

# Build measures and dimensions

    measure: count {
      type: count
      #  drill_fields: [detail*]
    }

    measure: reception_duration_average {
      type:  average
      sql: (1.00 * ${TABLE}.reception_duration)/(60*60*24) ;;
      value_format: "[h]:mm:ss"
      group_label: "Durations"
    }

    measure: waiting_duration_per_issue_sum {
      type: sum
      sql: (1.00 * ${TABLE}.waiting_duration)/(60*60*24) ;;
      value_format: "[h]:mm:ss"
      group_label: "Durations"
    }
    measure: waiting_duration_per_issue_average {
      type:  average
      sql: (1.00 * ${TABLE}.waiting_duration)/(60*60*24) ;;
      value_format: "[h]:mm:ss"
      group_label: "Durations"
    }

    # See here to understand the use of sum_distinct and average_distinct:
    #    https://docs.looker.com/reference/field-reference/measure-type-reference#sum_distinct
    measure: waiting_duration_sum {
      type: sum_distinct
      sql_distinct_key: ${TABLE}.client_id;;
      sql: (1.00 * ${TABLE}.waiting_duration_sum)/(60*60*24) ;;
      value_format: "[h]:mm:ss"
      group_label: "Durations"
    }
    measure: waiting_duration_average {
      type: average_distinct
      sql: (1.00 * ${TABLE}.waiting_duration_sum)/(60*60*24) ;;
      sql_distinct_key: ${TABLE}.client_id;;
      value_format: "[h]:mm:ss"
      group_label: "Durations"
    }

    #dimension: prep_duration {
    #  type:  number
    #  sql: (1.00 * ${TABLE}.prep_duration)/(60*60*24) ;;
    #  value_format: "[h]:mm:ss"
    #  group_label: "Durations"
    #}
    measure: prep_duration_per_issue_sum {
      type: sum
      sql: (1.00 * ${TABLE}.prep_duration)/(60*60*24) ;;
      value_format: "[h]:mm:ss"
      group_label: "Durations"
    }
    measure: prep_duration_per_issue_average {
      type:  average
      sql: (1.00 * ${TABLE}.prep_duration)/(60*60*24) ;;
      value_format: "[h]:mm:ss"
      group_label: "Durations"
    }
    measure: prep_duration_sum {
      type: sum_distinct
      sql_distinct_key: ${TABLE}.client_id;;
      sql: (1.00 * ${TABLE}.prep_duration_sum)/(60*60*24) ;;
      value_format: "[h]:mm:ss"
      group_label: "Durations"
    }
    measure: prep_duration_average {
      type: average_distinct
      sql: (1.00 * ${TABLE}.prep_duration_sum)/(60*60*24) ;;
      sql_distinct_key: ${TABLE}.client_id;;
      value_format: "[h]:mm:ss"
      group_label: "Durations"
    }

    measure: hold_duration_per_issue_sum {
      type: sum
      sql: (1.00 * ${TABLE}.hold_duration)/(60*60*24) ;;
      value_format: "[h]:mm:ss"
      group_label: "Durations"
    }
    measure: hold_duration_per_issue_average {
      type:  average
      sql: (1.00 * ${TABLE}.hold_duration)/(60*60*24) ;;
      value_format: "[h]:mm:ss"
      group_label: "Durations"
    }
    measure: hold_duration_sum {
      type: sum_distinct
      sql_distinct_key: ${TABLE}.client_id;;
      sql: (1.00 * ${TABLE}.hold_duration_sum)/(60*60*24) ;;
      value_format: "[h]:mm:ss"
      group_label: "Durations"
    }
    measure: hold_duration_average {
      type: average_distinct
      sql: (1.00 * ${TABLE}.hold_duration_sum)/(60*60*24) ;;
      sql_distinct_key: ${TABLE}.client_id;;
      value_format: "[h]:mm:ss"
      group_label: "Durations"
    }

    measure: serve_duration_per_issue_sum {
      type: sum
      sql: (1.00 * ${TABLE}.serve_duration)/(60*60*24) ;;
      value_format: "[h]:mm:ss"
      group_label: "Durations"
    }
    measure: serve_duration_per_issue_average {
      type:  average
      sql: (1.00 * ${TABLE}.serve_duration)/(60*60*24) ;;
      value_format: "[h]:mm:ss"
      group_label: "Durations"
    }
    measure: serve_duration_sum {
      type: sum_distinct
      sql_distinct_key: ${TABLE}.client_id;;
      sql: (1.00 * ${TABLE}.serve_duration_sum)/(60*60*24) ;;
      value_format: "[h]:mm:ss"
      group_label: "Durations"
    }
    measure: serve_duration_average {
      type: average_distinct
      sql: (1.00 * ${TABLE}.serve_duration_sum)/(60*60*24) ;;
      sql_distinct_key: ${TABLE}.client_id;;
      value_format: "[h]:mm:ss"
      group_label: "Durations"
    }



    dimension: reception_duration_zscore {
      type:  number
      sql: ${TABLE}.reception_duration_zscore ;;
      group_label: "Z-Scores"
    }
    dimension: waiting_duration_zscore {
      type:  number
      sql: ${TABLE}.waiting_duration_zscore ;;
      group_label: "Z-Scores"
    }
    dimension: prep_duration_zscore {
      type:  number
      sql: ${TABLE}.prep_duration_zscore ;;
      group_label: "Z-Scores"
    }
    dimension: hold_duration_zscore {
      type:  number
      sql: ${TABLE}.hold_duration_zscore ;;
      group_label: "Z-Scores"
    }
    dimension: serve_duration_zscore {
      type:  number
      sql: ${TABLE}.serve_duration_zscore ;;
      group_label: "Z-Scores"
    }

    dimension: reception_duration_outlier {
      type:  yesno
      sql: abs(${TABLE}.reception_duration_zscore) >= 3 ;;
      group_label: "Z-Scores"
    }
    dimension: waiting_duration_outlier {
      type:  yesno
      sql: abs(${TABLE}.waiting_duration_zscore) >= 3 ;;
      group_label: "Z-Scores"
    }
    dimension: prep_duration_outlier {
      type:  yesno
      sql: abs(${TABLE}.prep_duration_zscore) >= 3 ;;
      group_label: "Z-Scores"
    }
    dimension: hold_duration_outlier {
      type:  yesno
      sql: abs(${TABLE}.hold_duration_zscore) >= 3 ;;
      group_label: "Z-Scores"
    }
    dimension: serve_duration_outlier {
      type:  yesno
      sql: abs( ${TABLE}.serve_duration_zscore) >= 3;;
      group_label: "Z-Scores"
    }

    dimension: welcome_time {
      type: date_time
      sql: ${TABLE}.welcome_time ;;
      group_label: "Timing Points"
    }

    measure: count_of_days {
      type: number
      sql: count(distinct date(${TABLE}.welcome_time));;
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
      type: number
      sql:  ${TABLE}.service_count ;;
    }
    dimension: office_id {
      type: number
      sql: ${TABLE}.office_id ;;
      group_label: "Office Info"
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
      type: number
      sql: ${TABLE}.program_id ;;
      html: {{ rendered_value }} ;;
    }

    dimension: program_name {
      type: string
      sql: ${TABLE}.program_name ;;
    }

    dimension: transaction_name {
      type: string
      sql: ${TABLE}.transaction_name ;;
    }

    dimension: channel {
      type: string
      sql: ${TABLE}.channel ;;
    }

    dimension: inaccurate_time {
      type: yesno
      sql: ${TABLE}.inaccurate_time ;;
    }
  }
