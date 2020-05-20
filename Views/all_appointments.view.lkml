view: all_appointments {
    derived_table: {
      sql:
          WITH appointments AS (
            SELECT root_id, root_tstamp, schema_name AS event_name, appointment_id, CONVERT_TIMEZONE('UTC', 'America/Vancouver', appointment_start_timestamp::timestamp) AS appointment_start_timestamp, CONVERT_TIMEZONE('UTC', 'America/Vancouver', appointment_end_timestamp::timestamp) AS appointment_end_timestamp, NULL AS status, program_id, parent_id, program_name, transaction_name  FROM atomic.ca_bc_gov_cfmspoc_appointment_create_1
          UNION
            SELECT root_id, root_tstamp, schema_name AS event_name, appointment_id, NULL AS appointment_start_timestamp, NULL AS appointment_end_timestamp, NULL AS status, NULL AS program_id, NULL AS parent_id, NULL AS program_name, NULL AS transaction_name  FROM atomic.ca_bc_gov_cfmspoc_appointment_checkin_1
          UNION
            SELECT root_id, root_tstamp, schema_name AS event_name, appointment_id, CONVERT_TIMEZONE('UTC', 'America/Vancouver', appointment_start_timestamp::timestamp) AS appointment_start_timestamp, CONVERT_TIMEZONE('UTC', 'America/Vancouver', appointment_end_timestamp::timestamp) AS appointment_end_timestamp, status, program_id, parent_id, program_name, transaction_name  FROM atomic.ca_bc_gov_cfmspoc_appointment_update_1
          )
          SELECT
            CONVERT_TIMEZONE('UTC', 'America/Vancouver', appointments.root_tstamp) AS event_time,
            event_name, appointment_id, appointment_start_timestamp, appointment_end_timestamp, status, program_id, parent_id, program_name, transaction_name,
            appointments.root_id AS event_id,
            a.agent_id,
            a.role,
            a.counter_type,
            c.client_id,
            o.office_id
          FROM appointments
          LEFT JOIN atomic.ca_bc_gov_cfmspoc_agent_3 AS a
            ON appointments.root_id = a.root_id
          LEFT JOIN atomic.ca_bc_gov_cfmspoc_citizen_4 AS c
            ON appointments.root_id = c.root_id
          LEFT JOIN atomic.ca_bc_gov_cfmspoc_office_1 AS o
            ON appointments.root_id = o.root_id
          ORDER BY event_time, client_id, appointment_id
                  ;;
                  # https://docs.looker.com/data-modeling/learning-lookml/caching
    }

      dimension: event_name {
        type: string
        sql: ${TABLE}.event_name ;;
      }
      dimension: event_time {
        type: date_time
        sql: ${TABLE}.event_time ;;
      }
      dimension: event_date {
        type: date
        sql: ${TABLE}.event_time ;;
      }

      dimension: client_id {
        type: number
        sql: ${TABLE}.client_id ;;
        html: {{ rendered_value }} ;;
      }

      dimension: office_id {
        type: number
        sql: ${TABLE}.office_id ;;
      }

       dimension: agent_id {
        type: string
        sql: ${TABLE}.agent_id ;;
      }
      dimension: role {
        type: string
        sql: ${TABLE}.role ;;
      }
      dimension: counter_type {
        type: number
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


      dimension: status {
        type: string
        sql: ${TABLE}.status ;;
      }

      measure: count {
        type: count
      }

      dimension: event_id {
        type: string
        sql:  ${TABLE}.event_id ;;
      }

      dimension: appointment_start_timestamp {
        type: date
        sql: ${TABLE}.appointment_start_timestamp ;;
      }
      dimension: appointment_end_timestamp {
        type: date
        sql: ${TABLE}.appointment_end_timestamp ;;
      }

    }
