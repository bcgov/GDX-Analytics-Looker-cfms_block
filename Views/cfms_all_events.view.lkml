view: cfms_all_events {
  derived_table: {
    sql: -- Build a view containing all events using the name_tracker "CFMS_poc"
      -- this will include all fields for all possible events.
      -- NOTE: we are ignoring instances where there is no client_id
        WITH agent AS (
          SELECT schema_vendor, schema_name, schema_format, schema_version, root_id, root_tstamp, ref_root, ref_tree, ref_parent, agent_id,
               CASE WHEN (quick_txn) THEN 'Quick Transaction' END AS counter_type, role
          FROM atomic.ca_bc_gov_cfmspoc_agent_2 AS a2
        UNION
          SELECT schema_vendor, schema_name, schema_format, schema_version, root_id, root_tstamp, ref_root, ref_tree, ref_parent, agent_id, counter_type, role
          FROM atomic.ca_bc_gov_cfmspoc_agent_3 AS a3
        ),
        citizen AS (
          SELECT schema_vendor, schema_name, schema_format, schema_version, root_id, root_tstamp, ref_root, ref_tree, ref_parent, client_id,
            CASE WHEN (quick_txn) THEN 'Quick Transaction' END AS counter_type, service_count
          FROM atomic.ca_bc_gov_cfmspoc_citizen_3 AS c3
        UNION
          SELECT schema_vendor, schema_name, schema_format, schema_version, root_id, root_tstamp, ref_root, ref_tree, ref_parent, client_id, counter_type, service_count
          FROM atomic.ca_bc_gov_cfmspoc_citizen_4 AS c4
      )
      SELECT
      event_id, name_tracker AS namespace,
      event_name,
      event_version,
      CONVERT_TIMEZONE('UTC', 'America/Vancouver', dvce_created_tstamp) AS event_time,
      client_id,
      service_count,
      office_id,
      agent_id,
      c.counter_type,
      channel,
      program_id,
      parent_id,
      program_name,
      transaction_name,
      leave_status,
      count,
      quantity,
      COALESCE(fi.inaccurate_time,fi1.inaccurate_time) AS inaccurate_time
      FROM atomic.events AS ev
      LEFT JOIN agent AS a
      ON ev.event_id = a.root_id
      LEFT JOIN citizen AS c
      ON ev.event_id = c.root_id
      LEFT JOIN atomic.ca_bc_gov_cfmspoc_office_1 AS o
      ON ev.event_id = o.root_id
      LEFT JOIN atomic.ca_bc_gov_cfmspoc_chooseservice_3 AS cs
      ON ev.event_id = cs.root_id
      LEFT JOIN atomic.ca_bc_gov_cfmspoc_finish_1 AS fi1
      ON ev.event_id = fi1.root_id
      LEFT JOIN atomic.ca_bc_gov_cfmspoc_finish_2 AS fi
      ON ev.event_id = fi.root_id
      LEFT JOIN atomic.ca_bc_gov_cfmspoc_hold_1 AS ho
      ON ev.event_id = ho.root_id
      LEFT JOIN atomic.ca_bc_gov_cfmspoc_customerleft_2 AS le
      ON ev.event_id = le.root_id
      WHERE ev.name_tracker IN ('CFMS_poc', 'TheQ_dev', 'TheQ_test', 'TheQ_prod', 'TheQ_localhost') AND client_id IS NOT NULL
      ORDER BY event_time, client_id, service_count
          ;;
          # https://docs.looker.com/data-modeling/learning-lookml/caching
    }

    dimension: namespace {
      type: string
      sql: ${TABLE}.namespace ;;
    }
    dimension: event_name {
      type: string
      sql: ${TABLE}.event_name ;;
    }
    dimension: event_version {
      type: string
      sql: ${TABLE}.event_version ;;
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

    dimension: transaction_count {
      type: number
      sql: ${TABLE}.count ;;
    }

    dimension: inaccurate_time {
      type: yesno
      sql: ${TABLE}.inaccurate_time ;;
    }

    measure: count {
      type: count
    }

    dimension: event_id {
      type: string
      sql:  ${TABLE}.event_id ;;
    }

    dimension: leave_status {
      type: string
      sql: ${TABLE}.leave_status ;;
    }

  }
