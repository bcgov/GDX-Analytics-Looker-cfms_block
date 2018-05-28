view: cfms_poc {
  derived_table: {
    sql: WITH step1 AS( -- Build a CTE containing all events using the name_tracker "CFMS_poc"
                        -- this will include all fields for all possible events. We will then
                        -- build the individual tables from this big one below
                        -- NOTE: we are ignoring instances where there is no client_id
          SELECT
            event_name,
            derived_tstamp AS event_time,
            client_id,
            service_count,
            office_id,
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
        calculations AS ( -- Here we build an array of all possible calcultation combinations
          SELECT
          welcome_time AS t1,
          stand_time AS t2,
          invite_time AS t3,
          start_time AS t4,
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
              END AS prep_duration

          FROM welcome_table
          LEFT JOIN stand_table ON welcome_table.client_id = stand_table.client_id
          LEFT JOIN finish_table ON welcome_table.client_id = finish_table.client_id
          LEFT JOIN invite_table ON welcome_table.client_id = invite_table.client_id AND finish_table.service_count = invite_table.service_count
          LEFT JOIN start_table ON welcome_table.client_id = start_table.client_id AND finish_table.service_count = start_table.service_count
          ORDER BY welcome_time, stand_time, invite_time, start_time
        ),
        finalcalc AS (-- This is where we choose the correct one.
                      -- NOTE: we want the:
                        -- first: welcome time (t1)
                        -- first: stand time (t2)
                        -- LAST: invite time (t3)
                        -- first: start time (t4)
                      -- These are selected using the ROW_NUMBER partition method below.
                      -- NOTE: the ordering is chosen insite the PARTITION statement where we have a "T3 DESC".
          SELECT ranked.*
          FROM (
            SELECT *, ROW_NUMBER() OVER (PARTITION BY client_id, service_count ORDER BY t1, t2, t3 DESC, t4) AS client_id_ranked -- we want the LAST t3 = invite time
            FROM calculations
            ORDER BY client_id, service_count, t1, t2, t3 DESC, t4
          ) AS ranked
          WHERE ranked.client_id_ranked = 1
        ),
        combined AS ( -- Combine it all together into a big table. Note that we still have duplicate entries here.
          SELECT
          welcome_table.client_id,
          finish_table.service_count,
          welcome_table.office_id,
          service_bc_office_info.name AS office_name,
          welcome_table.agent_id,
          chooseservice_table.program_id,
          chooseservice_table.program_name,
          transaction_name,
          chooseservice_table.channel,
          finish_table.inaccurate_time,
          welcome_time, stand_time, invite_time, start_time, finish_time, chooseservice_time,
          c1.reception_duration,
          c1.waiting_duration,
          c1.prep_duration
          FROM welcome_table
          LEFT JOIN stand_table ON welcome_table.client_id = stand_table.client_id
          LEFT JOIN finish_table ON welcome_table.client_id = finish_table.client_id
          LEFT JOIN invite_table ON welcome_table.client_id = invite_table.client_id AND finish_table.service_count = invite_table.service_count
          LEFT JOIN start_table ON welcome_table.client_id = start_table.client_id AND finish_table.service_count = start_table.service_count
          LEFT JOIN chooseservice_table ON welcome_table.client_id = chooseservice_table.client_id AND finish_table.service_count = chooseservice_table.service_count
          LEFT JOIN static.service_bc_office_info ON static.service_bc_office_info.id = chooseservice_table.office_id
          JOIN finalcalc AS c1 ON welcome_table.client_id = c1.client_id AND finish_table.service_count = c1.service_count
        ),
          finalset AS ( -- Use the ROW_NUMBER method again to get a unique list for each client_id/service_count pair
            SELECT ranked.*
            FROM (
              SELECT *, ROW_NUMBER() OVER (PARTITION BY client_id, service_count ORDER BY welcome_time) AS client_id_ranked
              FROM combined
              ORDER BY client_id, welcome_time
            ) AS ranked
            WHERE ranked.client_id_ranked = 1
          )
          SELECT finalset.*,-- ADD in the aggregate calculations summed over each of the services for a given client_id.
                            -- this is because the reception_duration only happens once per client
                            -- waiting_duration and prep_duration can be per client or per service. This gives us the per client version
                            -- below we use "sum_distinct" and "average_distinct" to report out on these versions
            SUM(c2.waiting_duration) AS waiting_duration_sum,
            SUM(c2.prep_duration) AS prep_duration_sum
          FROM finalset
          JOIN finalcalc AS c2 ON c2.client_id = finalset.client_id
          WHERE finalset.client_id_ranked = 1
          GROUP BY finalset.client_id,
            finalset.service_count,
            finalset.office_id,
            office_name,
            agent_id,
            program_id,
            program_name,
            transaction_name,
            channel,
            inaccurate_time,
            welcome_time, stand_time, invite_time, start_time, finish_time, chooseservice_time,
            finalset.reception_duration,
            finalset.waiting_duration,
            finalset.prep_duration,
            finalset.client_id_ranked
          ;;
  }

# Build measures and dimensions

  measure: count {
    type: count
    drill_fields: [detail*]
  }

  measure: reception_duration_average {
    type:  average
    sql: (1.00 * ${TABLE}.reception_duration)/(60*60*24) ;;
    value_format: "[h]:mm:ss"
  }

  dimension: reception_duration {
    type:  number
    sql: (1.00 * ${TABLE}.reception_duration)/(60*60*24) ;;
    value_format: "[h]:mm:ss"
  }

  dimension: waiting_duration {
    type:  number
    sql: (1.00 * ${TABLE}.waiting_duration)/(60*60*24) ;;
    value_format: "[h]:mm:ss"
  }
  measure: waiting_duration_per_issue_sum {
    type: sum
    sql: (1.00 * ${TABLE}.waiting_duration)/(60*60*24) ;;
    value_format: "[h]:mm:ss"
  }
  measure: waiting_duration_per_issue_average {
    type:  average
    sql: (1.00 * ${TABLE}.waiting_duration)/(60*60*24) ;;
    value_format: "[h]:mm:ss"
  }

  # See here to understand the use of sum_distinct and average_distinct:
  #    https://docs.looker.com/reference/field-reference/measure-type-reference#sum_distinct
  measure: waiting_duration_sum {
    type: sum_distinct
    sql_distinct_key: ${TABLE}.client_id;;
    sql: (1.00 * ${TABLE}.waiting_duration_sum)/(60*60*24) ;;
    value_format: "[h]:mm:ss"
  }
  measure: waiting_duration_average {
    type: average_distinct
    sql: (1.00 * ${TABLE}.waiting_duration_sum)/(60*60*24) ;;
    sql_distinct_key: ${TABLE}.client_id;;
    value_format: "[h]:mm:ss"
  }

  dimension: prep_duration {
    type:  number
    sql: (1.00 * ${TABLE}.prep_duration)/(60*60*24) ;;
    value_format: "[h]:mm:ss"
  }
  measure: prep_duration_per_issue_sum {
    type: sum
    sql: (1.00 * ${TABLE}.prep_duration)/(60*60*24) ;;
    value_format: "[h]:mm:ss"
  }
  measure: prep_duration_per_issue_average {
    type:  average
    sql: (1.00 * ${TABLE}.prep_duration)/(60*60*24) ;;
    value_format: "[h]:mm:ss"
  }
  measure: prep_duration_sum {
    type: sum_distinct
    sql_distinct_key: ${TABLE}.client_id;;
    sql: (1.00 * ${TABLE}.prep_duration_sum)/(60*60*24) ;;
    value_format: "[h]:mm:ss"
  }
  measure: prep_duration_average {
    type: average_distinct
    sql: (1.00 * ${TABLE}.prep_duration_sum)/(60*60*24) ;;
    sql_distinct_key: ${TABLE}.client_id;;
    value_format: "[h]:mm:ss"
  }

  dimension: welcome_time {
    type: date_time
    sql: ${TABLE}.welcome_time ;;
  }

  dimension: date {
    type:  date
    sql:  ${TABLE}.welcome_time ;;
  }
  dimension: week {
    type:  date_week
    sql:  ${TABLE}.welcome_time ;;
  }
  dimension: month {
    type:  date_month_name
    sql:  ${TABLE}.welcome_time ;;
  }
  dimension: year {
    type:  date_year
    sql:  ${TABLE}.welcome_time ;;
  }

  dimension: day_of_month {
    type:  date_day_of_month
    sql:  ${TABLE}.welcome_time ;;
  }
  dimension: day_of_week {
    type:  date_day_of_week
    sql:  ${TABLE}.welcome_time ;;
  }

  dimension: stand_time {
    type: date_time
    sql: ${TABLE}.stand_time ;;
  }

  dimension: invite_time {
    type: date_time
    sql: ${TABLE}.invite_time ;;
  }

  dimension: start_time {
    type: date_time
    sql: ${TABLE}.start_time ;;
  }

  dimension: chooseservice_time {
    type: date_time
    sql:  ${TABLE}.chooseservice_time ;;
  }


  dimension: finish_time {
    type: date_time
    sql: ${TABLE}.finish_time ;;
  }

  dimension: client_id {
    type: number
    sql: ${TABLE}.client_id ;;
  }

  dimension: service_count {
    type: number
    sql:  ${TABLE}.service_count ;;
  }
  dimension: office_id {
    type: number
    sql: ${TABLE}.office_id ;;
  }

  dimension: office_name {
    type:  string
    sql:  ${TABLE}.office_name ;;
  }

  dimension: agent_id {
    type: number
    sql: ${TABLE}.agent_id ;;
  }

  dimension: program_id {
    type: number
    sql: ${TABLE}.program_id ;;
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

# TO FIX "set: detail"
  set: detail {
    fields: [
      client_id,
      service_count,
      office_id,
      office_name,
      agent_id,
      program_id,
      program_name,
      transaction_name,
      channel,
      inaccurate_time,
      welcome_time,
      stand_time,
      invite_time,
      start_time,
      chooseservice_time,
      finish_time,
      date
    ]
  }
}
