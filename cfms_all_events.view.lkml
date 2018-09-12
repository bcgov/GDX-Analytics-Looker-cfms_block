view: cfms_all_events {
  derived_table: {
    sql: -- Build a view containing all events using the name_tracker "CFMS_poc"
      -- this will include all fields for all possible events.
      -- NOTE: we are ignoring instances where there is no client_id
      SELECT
      name_tracker AS namespace,
      event_name,
      -- CONVERT_TIMEZONE('UTC', 'US/Pacific', derived_tstamp) AS
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
      LEFT JOIN atomic.ca_bc_gov_cfmspoc_chooseservice_3 AS cs
      ON ev.event_id = cs.root_id
      LEFT JOIN atomic.ca_bc_gov_cfmspoc_finish_1 AS fi
      ON ev.event_id = fi.root_id
      LEFT JOIN atomic.ca_bc_gov_cfmspoc_hold_1 AS ho
      ON ev.event_id = ho.root_id
      WHERE ev.name_tracker IN ('CFMS_poc', 'TheQ_dev', 'TheQ_test', 'TheQ_prod') AND client_id IS NOT NULL
      ORDER BY event_time, client_id, service_count
          ;;
          # https://docs.looker.com/data-modeling/learning-lookml/caching
      persist_for: "1 hour"
      distribution_style: all
    }

  dimension: namespace {
    type: string
    sql: ${TABLE}.namespace ;;
  }
  dimension: event_name {
    type: string
    sql: ${TABLE}.event_name ;;
  }
  dimension: event_time {
    type: date_time
    sql: ${TABLE}.event_time ;;
  }

    dimension: client_id {
      type: number
      sql: ${TABLE}.client_id ;;
      html: {{ rendered_value }} ;;
    }

    dimension: service_count {
      type: number
      sql: ${TABLE}.service_count ;;
    }

    dimension: office_id {
      type: number
      sql: ${TABLE}.office_id ;;
    }

    dimension: agent_id {
      type: number
      sql: ${TABLE}.agent_id ;;
    }

    dimension: channel {
      type: string
      sql: ${TABLE}.channel ;;
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

    dimension: count {
      type: number
      sql: ${TABLE}.count ;;
    }

    dimension: inaccurate_time {
      type: yesno
      sql: ${TABLE}.inaccurate_time ;;
    }


  }
