view: appointments {
  derived_table: {
    sql:
      WITH appointment_create_raw AS (
        SELECT name_tracker, role, CONVERT_TIMEZONE('UTC', 'America/Vancouver', ev.dvce_created_tstamp) AS create_time, appointment_id
        FROM atomic.ca_bc_gov_cfmspoc_appointment_create_1 AS acr
        LEFT JOIN atomic.events AS ev ON ev.event_id = acr.root_id AND ev.collector_tstamp = acr.root_tstamp
        LEFT JOIN atomic.ca_bc_gov_cfmspoc_agent_3 AS a ON acr.root_id = a.root_id
      ),
      appointment_update_raw AS (
        SELECT name_tracker, CONVERT_TIMEZONE('UTC', 'America/Vancouver', ev.dvce_created_tstamp) AS update_time, appointment_id, status
        FROM atomic.ca_bc_gov_cfmspoc_appointment_update_1 AS aup
        LEFT JOIN atomic.events AS ev ON ev.event_id = aup.root_id AND ev.collector_tstamp = aup.root_tstamp
      ),
      appointment_checkin_raw AS (
        SELECT name_tracker, CONVERT_TIMEZONE('UTC', 'America/Vancouver', ev.dvce_created_tstamp) AS checkin_time, appointment_id
        FROM atomic.ca_bc_gov_cfmspoc_appointment_checkin_1 AS ach
        LEFT JOIN atomic.events AS ev ON ev.event_id = ach.root_id AND ev.collector_tstamp = ach.root_tstamp
      ),
      creates AS (
        SELECT name_tracker, appointment_id, role, MIN(create_time) AS min_create_time, MAX(create_time) AS max_create_time, COUNT(*) AS create_count
        FROM appointment_create_raw
        GROUP BY 1, 2, 3
      ),
      updates AS ( -- this counts updates and cancellations
        SELECT name_tracker, appointment_id, MIN(update_time) AS min_update_time, MAX(update_time) AS max_update_time,COUNT(*) AS update_count
        FROM appointment_update_raw
        GROUP BY 1, 2
      ),
      cancels AS ( -- this just counts cancellations
        SELECT name_tracker, appointment_id, COUNT(*) AS cancel_count
        FROM appointment_update_raw
        WHERE status = 'cancel'
        GROUP BY 1, 2
      ),
      checkins AS (
        SELECT name_tracker, appointment_id, MIN(checkin_time) AS min_checkin_time, MAX(checkin_time) AS max_checkin_time, COUNT(*) AS checkin_count
        FROM appointment_checkin_raw
        GROUP BY 1, 2
      ),
      appointments_raw AS (
          SELECT name_tracker, ev.dvce_created_tstamp, appointment_id, client_id, agent_id, a.counter_type, role, office_id, office_type, CONVERT_TIMEZONE('UTC', 'America/Vancouver', appointment_start_timestamp::timestamp) AS appointment_start_timestamp, CONVERT_TIMEZONE('UTC', 'America/Vancouver', appointment_end_timestamp::timestamp) AS appointment_end_timestamp, program_id, parent_id, program_name, transaction_name

          FROM atomic.ca_bc_gov_cfmspoc_appointment_create_1 AS acr
          LEFT JOIN atomic.events AS ev ON ev.event_id = acr.root_id AND ev.collector_tstamp = acr.root_tstamp
          LEFT JOIN atomic.ca_bc_gov_cfmspoc_citizen_4 AS c ON acr.root_id = c.root_id
          LEFT JOIN atomic.ca_bc_gov_cfmspoc_agent_3 AS a ON acr.root_id = a.root_id
          LEFT JOIN atomic.ca_bc_gov_cfmspoc_office_1 AS o ON acr.root_id = o.root_id
          WHERE name_tracker = 'TheQ_prod'
        UNION
          SELECT name_tracker, CONVERT_TIMEZONE('UTC', 'America/Vancouver', ev.dvce_created_tstamp) AS update_time, appointment_id, client_id,  agent_id, a.counter_type, role, office_id, office_type, CONVERT_TIMEZONE('UTC', 'America/Vancouver', appointment_start_timestamp::timestamp) AS appointment_start_timestamp, CONVERT_TIMEZONE('UTC', 'America/Vancouver', appointment_end_timestamp::timestamp) AS appointment_end_timestamp, program_id, parent_id, program_name, transaction_name
          FROM atomic.ca_bc_gov_cfmspoc_appointment_update_1 AS aup
          LEFT JOIN atomic.events AS ev ON ev.event_id = aup.root_id AND ev.collector_tstamp = aup.root_tstamp
          LEFT JOIN atomic.ca_bc_gov_cfmspoc_citizen_4 AS c ON aup.root_id = c.root_id
          LEFT JOIN atomic.ca_bc_gov_cfmspoc_agent_3 AS a ON aup.root_id = a.root_id
          LEFT JOIN atomic.ca_bc_gov_cfmspoc_office_1 AS o ON aup.root_id = o.root_id
          WHERE name_tracker = 'TheQ_prod'
      ),
      appointments_ranked AS ( -- find the last create or update
        SELECT *, ROW_NUMBER() OVER (PARTITION BY name_tracker, appointment_id) AS appointments_ranked
        FROM appointments_raw
        ORDER BY dvce_created_tstamp DESC
      )
      SELECT ar.name_tracker, ar.appointment_id, client_id, agent_id, office_id, counter_type, creates.role,  program_id, parent_id, program_name, transaction_name,
        CASE
          WHEN COALESCE(cancel_count,0) > 0 THEN 'cancelled'
          WHEN appointment_start_timestamp::date < CURRENT_DATE THEN 'past'
          WHEN appointment_start_timestamp::date = CURRENT_DATE THEN 'today'
          WHEN appointment_start_timestamp::date > CURRENT_DATE THEN 'future'
          ELSE 'error'
        END AS appointment_period,
        CASE
          WHEN COALESCE(cancel_count,0) > 0 THEN 'cancelled'
          WHEN COALESCE(cancel_count,0) = 0 AND appointment_start_timestamp::date < CURRENT_DATE AND (COALESCE(checkin_count,0) = 0) THEN 'no-show'
          WHEN COALESCE(cancel_count,0) = 0 AND (COALESCE(checkin_count,0) > 0) AND min_checkin_time > dateadd(minute, 10, appointment_start_timestamp) THEN 'late'
          WHEN COALESCE(cancel_count,0) = 0 AND (COALESCE(checkin_count,0) > 0) AND min_checkin_time <= dateadd(minute, 10, appointment_start_timestamp) THEN 'on-time'
          WHEN COALESCE(cancel_count,0) = 0 AND appointment_start_timestamp::date = CURRENT_DATE AND (COALESCE(checkin_count,0) = 0) THEN 'appointment still open today'
          WHEN COALESCE(cancel_count,0) = 0 AND appointment_start_timestamp::date > CURRENT_DATE THEN 'appointment in future'
          ELSE 'error'
        END AS appointment_status,
        CASE WHEN COALESCE(cancel_count,0) > 0 THEN 1 ELSE 0 END AS cancel_count,
        CASE WHEN COALESCE(cancel_count,0) = 0 AND appointment_start_timestamp::date < CURRENT_DATE AND (COALESCE(checkin_count,0) = 0) THEN 1 ELSE 0 END AS no_show_count,
        CASE WHEN COALESCE(cancel_count,0) = 0 AND (COALESCE(checkin_count,0) > 0) AND min_checkin_time > dateadd(minute, 10, appointment_start_timestamp) THEN 1 ELSE 0 END AS late_count,
        CASE WHEN COALESCE(cancel_count,0) = 0 AND (COALESCE(checkin_count,0) > 0) AND min_checkin_time <= dateadd(minute, 10, appointment_start_timestamp)THEN 1 ELSE 0 END AS on_time_count,
        CASE WHEN COALESCE(cancel_count,0) = 0 AND appointment_start_timestamp::date = CURRENT_DATE AND (COALESCE(checkin_count,0) = 0) THEN 1 ELSE 0 END AS open_count,
        CASE WHEN COALESCE(cancel_count,0) = 0 AND appointment_start_timestamp::date > CURRENT_DATE THEN 1 ELSE 0 END AS in_future_count,
        appointment_start_timestamp, appointment_end_timestamp,
        min_create_time, create_count,
        min_update_time, max_update_time, COALESCE(update_count,0) AS update_count,
        min_checkin_time, max_checkin_time, COALESCE(checkin_count,0) AS checkin_count,
        office_info.site AS office_name,
        office_info.officesize AS office_size,
        office_info.area AS area_number,
        office_type,
        office_info.current_area AS current_area
      FROM appointments_ranked AS ar
      LEFT JOIN creates on creates.appointment_id = ar.appointment_id AND creates.name_tracker = ar.name_tracker
      LEFT JOIN updates on updates.appointment_id = ar.appointment_id AND updates.name_tracker = ar.name_tracker
      LEFT JOIN cancels on cancels.appointment_id = ar.appointment_id AND cancels.name_tracker = ar.name_tracker
      LEFT JOIN checkins on checkins.appointment_id = ar.appointment_id AND checkins.name_tracker = ar.name_tracker
      LEFT JOIN servicebc.office_info ON servicebc.office_info.rmsofficecode = office_id AND end_date IS NULL -- for now, get the most recent office info
      WHERE appointments_ranked = 1;;
    persist_for: "1 hour"
    distribution_style: all
  }

  dimension: namespace {
    type: string
    sql: ${TABLE}.name_tracker ;;
  }

  dimension: client_id {
    type: number
    sql: ${TABLE}.client_id ;;
    html: {{ rendered_value }} ;;
  }
  dimension: appointment_id {
    type: number
    sql: ${TABLE}.appointment_id ;;
    html: {{ rendered_value }} ;;
  }

  dimension: appointment_period {
    type: string
    sql: ${TABLE}.appointment_period ;;
  }
  dimension: appointment_status {
    type: string
    sql:  ${TABLE}.appointment_status ;;
  }

  dimension_group: appointment_start {
    type: time
    timeframes: [raw, time, minute, minute10, time_of_day, hour_of_day, hour, date, day_of_month, day_of_week, week, month, quarter, year]
    sql: ${TABLE}.appointment_start_timestamp ;;
  }

  dimension_group: appointment_end {
    type: time
    timeframes: [raw, time, minute, minute10, time_of_day, hour_of_day, hour, date, day_of_month, day_of_week, week, month, quarter, year]
    sql: ${TABLE}.appointment_end_timestamp ;;
  }


  dimension_group: create_time {
    type: time
    timeframes: [raw, time, minute, minute10, time_of_day, hour_of_day, hour, date, day_of_month, day_of_week, week, month, quarter, year]
    sql: ${TABLE}.min_create_time ;;
  }
  dimension: create_count {
    type: number
    sql: ${TABLE}.create_count;;
    description: "Number of times this appointment was created (should always be 1)"
    group_label: "Counts"
  }
  dimension_group: min_update_time {
    type: time
    timeframes: [raw, time, minute, minute10, time_of_day, hour_of_day, hour, date, day_of_month, day_of_week, week, month, quarter, year]
    sql: ${TABLE}.min_update_time ;;
  }
  dimension_group: max_update_time {
    type: time
    timeframes: [raw, time, minute, minute10, time_of_day, hour_of_day, hour, date, day_of_month, day_of_week, week, month, quarter, year]
    sql: ${TABLE}.max_update_time ;;
  }
  dimension: update_count {
    type: number
    description: "Number of times this appointment was updated"
    sql: ${TABLE}.update_count;;
    group_label: "Counts"
  }
  dimension_group: min_checkin_time {
    type: time
    timeframes: [raw, time, minute, minute10, time_of_day, hour_of_day, hour, date, day_of_month, day_of_week, week, month, quarter, year]
    sql: ${TABLE}.min_checkin_time ;;
  }
  dimension_group: max_checkin_time {
    type: time
    timeframes: [raw, time, minute, minute10, time_of_day, hour_of_day, hour, date, day_of_month, day_of_week, week, month, quarter, year]
    sql: ${TABLE}.max_checkin_time ;;
  }
  dimension: checkin_count {
    type: number
    sql: ${TABLE}.checkin_count;;
    description: "Number of times there was a check-in for the appointment"
    group_label: "Counts"
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
  dimension: office_filter{
    type: string
    sql: TRANSLATE(TRANSLATE(${TABLE}.office_name, ' ', '_'),'.','') ;; #-- translate location names to use "_" instead of " " for filtering
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
  dimension: current_area {
    type: string
    sql:  ${TABLE}.current_area ;;
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
  dimension: role {
    type: string
    sql: ${TABLE}.role ;;
  }
  dimension: counter_type {
    type: string
    sql: ${TABLE}.counter_type ;;
  }


  dimension: program_id {
    type: number
    sql: ${TABLE}.program_id ;;
  }

  dimension: parent_id {
    type: number
    sql: ${TABLE}.parent_id ;;
  }

  dimension: program_name {
    type: string
    sql: ${TABLE}.program_name ;;
  }

  dimension: transaction_name {
    type: string
    sql: ${TABLE}.transaction_name ;;
  }

  dimension: made_appointment {
    type: yesno
    sql: appointment_id IS NOT NULL ;;
  }


  measure: count {
    type: count
  }
  measure: cancel_count {
    type: sum
    description: "Count of people who cancelled their appointment"
    sql: ${TABLE}.update_count;;
  }
  measure: no_show_count {
    type: sum
    description: "Count of people who did not show by end of day of appointment"
    sql: ${TABLE}.no_show_count ;;
  }
  measure: late_count {
    type: sum
    description: "Count of people who showed up more than 10 minutes after scheduled time for appointment"
    sql: ${TABLE}.late_count ;;
  }
  measure: on_time_count {
    type: sum
    description: "Count of people who showed up early or within 10 minutes of scheduled time for appointment"
    sql: ${TABLE}.on_time_count ;;
  }
  measure: open_count {
    type: sum
    description: "Count of people who have not yet shown up for appointments today"
    sql: ${TABLE}.open_count ;;
  }
  measure: in_future_count {
    type: sum
    description: "Count of people appointments in the future"
    sql: ${TABLE}.in_future_count ;;
  }
}
