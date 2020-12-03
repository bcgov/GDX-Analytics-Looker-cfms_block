view: cfms_dev {
  derived_table: {
    sql: WITH agent AS (
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
  ),
  pre AS (
  SELECT
    ev.name_tracker AS namespace,
    ev.event_name,
    CONVERT_TIMEZONE('UTC', 'America/Vancouver', ev.dvce_created_tstamp) AS event_time,
    DATEDIFF(milliseconds, '1970-01-01 00:00:00',event_time)/1000.0 AS event_time_number,
    client_id,
    service_count,
    office_id,
    office_type,
    agent_id,
    channel,
    program_id,
    parent_id,
    COALESCE(si.program, cs.program_name) AS program_name,
    COALESCE(si.service,cs.transaction_name) AS transaction_name,
    COALESCE(fi.quantity,fs.quantity) AS transaction_count, -- from finish-2.0.0 or finishstopped-1.0.0
    inaccurate_time, -- from finish-2.0.0
    leave_status -- from customerleft-2.0.0

  FROM atomic.events AS ev
  LEFT JOIN agent AS a
      ON ev.event_id = a.root_id AND ev.collector_tstamp = a.root_tstamp
  LEFT JOIN citizen AS c
      ON ev.event_id = c.root_id AND ev.collector_tstamp = c.root_tstamp
  LEFT JOIN atomic.ca_bc_gov_cfmspoc_office_1 AS o
      ON ev.event_id = o.root_id AND ev.collector_tstamp = o.root_tstamp
  LEFT JOIN atomic.ca_bc_gov_cfmspoc_chooseservice_3 AS cs
      ON ev.event_id = cs.root_id AND ev.collector_tstamp = cs.root_tstamp
  LEFT JOIN atomic.ca_bc_gov_cfmspoc_finish_2 AS fi
      ON ev.event_id = fi.root_id AND ev.collector_tstamp = fi.root_tstamp
  LEFT JOIN atomic.ca_bc_gov_cfmspoc_finishstopped_1 AS fs
      ON ev.event_id = fs.root_id AND ev.collector_tstamp = fs.root_tstamp
  LEFT JOIN atomic.ca_bc_gov_cfmspoc_hold_1 AS ho
      ON ev.event_id = ho.root_id AND ev.collector_tstamp = ho.root_tstamp
  LEFT JOIN atomic.ca_bc_gov_cfmspoc_customerleft_2 AS cl
      ON ev.event_id = cl .root_id AND ev.collector_tstamp = cl.root_tstamp
  LEFT JOIN servicebc.service_info AS si ON si.svccode = cs.program_id

  WHERE ev.name_tracker IN ('TheQ_dev','TheQ_prod','TheQ_test','TheQ_localhost')
    AND client_id IS NOT NULL
    AND event_name NOT IN ('appointment_checkin','appointment_create','appointment_update')
    AND  ev.dvce_created_tstamp > DATEADD(day,-30, DATE_TRUNC('day',GETDATE()) )
  ),
  service_info_pre AS (
    SELECT
    namespace,
    event_time,
    client_id,
    service_count,
    channel,
    program_id,
    parent_id,
    program_name,
    transaction_name,
    ROW_NUMBER() OVER (PARTITION BY client_id, service_count, namespace) AS service_info_ranked
    FROM pre
    WHERE event_name = 'chooseservice'
    ORDER BY event_time DESC
  ),
  service_info AS ( -- we get our service choices by taking the final choices made by chooseservice for a given client_id and service_count
   SELECT
      service_info_pre.*
    FROM service_info_pre
    WHERE service_info_ranked = 1
  ),
  agent_info_pre AS ( -- we set the Agent ID to be the LAST agent who worked on the case other than setting a "Customer Left" event
    SELECT
    namespace,
    event_time,
    client_id,
    office_id,
    office_type,
    service_count,
    agent_id,
    ROW_NUMBER() OVER (PARTITION BY client_id, service_count, namespace) AS agent_info_ranked
    FROM pre
    WHERE event_name <> 'customerleft'
    ORDER BY event_time DESC
  ),
  agent_info AS ( -- we get our service choices by taking the final choices made by chooseservice for a given client_id and service_count
    SELECT
    agent_info_pre.*
    FROM agent_info_pre
    WHERE agent_info_ranked = 1
  ),
  finish_info_pre AS (
    SELECT
    namespace,
    client_id,
    service_count,
    transaction_count,
    inaccurate_time,
    CASE
      WHEN (inaccurate_time) THEN 'Inaccurate Time'
      WHEN (event_name IN ('finish','finishstopped')) THEN 'Finished'
      ELSE 'Customer Left (' || leave_status || ')' END AS finish_type,
    ROW_NUMBER() OVER (PARTITION BY client_id, service_count, namespace) AS finish_info_ranked
    FROM pre
    WHERE event_name IN ('finish','finishstopped','customerleft')
    GROUP BY namespace, client_id, service_count,finish_type,transaction_count,inaccurate_time
  ),
  finish_info AS ( -- we get our service choices by taking the final choices made by chooseservice for a given client_id and service_count
    SELECT
    finish_info_pre.*
    FROM finish_info_pre
    WHERE finish_info_ranked = 1
  ),

  base_calculations AS (
    SELECT
    client_id,
    service_count,
    namespace,
    min(event_time) AS welcome_time,
    max(event_time) AS latest_time,
    SUM( CASE WHEN (event_name IN ('addtoqueue', 'servecitizen') OR ( event_name = 'customerleft' AND leave_status = 'service-creation')) THEN event_time_number ELSE 0 END)
      - SUM( CASE WHEN (event_name IN ('addcitizen')) THEN event_time_number ELSE 0 END) AS service_creation_duration,
    SUM( CASE WHEN (event_name IN ('addtoqueue', 'servecitizen') OR ( event_name = 'customerleft' AND leave_status = 'service-creation')) THEN 1 ELSE 0 END) AS service_creation_out,
    SUM( CASE WHEN (event_name IN ('addcitizen')) THEN 1 ELSE 0 END) AS service_creation_in,

    SUM( CASE WHEN (event_name IN ('invitecitizen', 'invitefromlist', 'returninvite', 'returnfromlist') ) THEN event_time_number ELSE 0 END)
      - SUM( CASE WHEN (event_name IN ('addtoqueue', 'returntoqueue', 'queuefromprep')) THEN event_time_number ELSE 0 END) AS waiting_duration,
    SUM( CASE WHEN (event_name IN ('invitecitizen', 'invitefromlist', 'returninvite', 'returnfromlist') ) THEN 1 ELSE 0 END) AS waiting_out,
    SUM( CASE WHEN (event_name IN ('addtoqueue', 'returntoqueue', 'queuefromprep')) THEN 1 ELSE 0 END) AS waiting_in,

    SUM( CASE WHEN (event_name IN ('beginservice', 'queuefromprep') OR ( event_name = 'customerleft' AND leave_status = 'at-prep') ) THEN event_time_number ELSE 0 END)
      - SUM( CASE WHEN (event_name IN ('invitecitizen', 'invitefromlist', 'returninvite','returnfromlist') ) THEN event_time_number ELSE 0 END) AS prep_duration,
    SUM( CASE WHEN (event_name IN ('beginservice', 'queuefromprep') OR ( event_name = 'customerleft' AND leave_status = 'at-prep') ) THEN 1 ELSE 0 END) AS prep_out,
    SUM( CASE WHEN (event_name IN ('invitecitizen', 'invitefromlist', 'returninvite','returnfromlist') ) THEN 1 ELSE 0 END) AS prep_in,

    SUM( CASE WHEN (event_name IN ('returntoqueue', 'hold', 'stopservice', 'finish') OR ( event_name = 'customerleft' AND leave_status = 'being-served') ) THEN event_time_number ELSE 0 END)
      - SUM( CASE WHEN (event_name IN ('additionalservice', 'beginservice', 'servecitizen', 'invitefromhold', 'restartservice') ) THEN event_time_number ELSE 0 END) AS serve_duration,
    SUM( CASE WHEN (event_name IN ('returntoqueue', 'hold', 'stopservice', 'finish') OR ( event_name = 'customerleft' AND leave_status = 'being-served') ) THEN 1 ELSE 0 END) AS serve_out,
    SUM( CASE WHEN (event_name IN ('additionalservice', 'beginservice', 'servecitizen', 'invitefromhold', 'restartservice') ) THEN 1 ELSE 0 END) AS serve_in,

    SUM( CASE WHEN (event_name IN ('invitefromhold') ) THEN event_time_number ELSE 0 END)
      - SUM( CASE WHEN (event_name IN ('hold') ) THEN event_time_number ELSE 0 END) AS hold_duration,
    SUM( CASE WHEN (event_name IN ('invitefromhold') ) THEN 1 ELSE 0 END) AS hold_out,
    SUM( CASE WHEN (event_name IN ('hold') ) THEN 1 ELSE 0 END) AS hold_in

    FROM pre
    GROUP BY
    client_id,
    service_count,
    namespace
  ),
  item_list AS (
    SELECT
    base_calculations.client_id,
    base_calculations.service_count,
    base_calculations.namespace,
    welcome_time,
    latest_time,
    CASE WHEN (service_creation_in = service_creation_out AND service_creation_in > 0) THEN service_creation_duration ELSE NULL END AS service_creation_duration,
    CASE WHEN (waiting_in = waiting_out AND waiting_in > 0) THEN waiting_duration ELSE NULL END AS waiting_duration,
    CASE WHEN (prep_in = prep_out AND prep_in > 0) THEN prep_duration ELSE NULL END AS prep_duration,
    CASE WHEN ((inaccurate_time IS NULL OR inaccurate_time = FALSE) AND serve_in = serve_out AND serve_in > 0) THEN serve_duration ELSE NULL END AS serve_duration,
    CASE WHEN (hold_in = hold_out AND hold_in > 0) THEN hold_duration ELSE NULL END AS hold_duration,
    --- Set flags for states
    CASE WHEN service_creation_in = service_creation_out + 1 THEN TRUE END AS service_creation_flag,
    CASE WHEN waiting_in = 0 THEN TRUE END AS no_wait_flag,
    CASE WHEN waiting_in = waiting_out + 1 THEN TRUE END AS waiting_flag,
    CASE WHEN prep_in = prep_out + 1 THEN TRUE END AS prep_flag,
    CASE WHEN serve_in = serve_out + 1 THEN TRUE END AS serve_flag,
    CASE WHEN hold_in = hold_out + 1 THEN TRUE END AS hold_flag,
    CASE WHEN -- to calculate missing flags, we look for cases where the discrepancy is greater than you could see by still being a stage
      (service_creation_in - service_creation_out) NOT in (0, -1)
      AND (waiting_in - waiting_out) NOT in (0, -1)
      AND (prep_in - prep_out) NOT in (0, -1)
      AND (serve_in - serve_out) NOT in (0, -1)
      AND (hold_in - hold_out) NOT in (0, -1)
    THEN TRUE END AS missing_calls_flag,
    inaccurate_time,
    CASE
      WHEN service_creation_in = service_creation_out + 1 THEN 'At Service Creation'
      WHEN waiting_in = waiting_out + 1 THEN 'Waiting in Line'
      WHEN prep_in = prep_out + 1 THEN 'At Prep'
      WHEN serve_in = serve_out + 1 THEN 'Being Served'
      WHEN hold_in = hold_out + 1 THEN 'On Hold'
      WHEN (service_creation_in - service_creation_out) + (waiting_in - waiting_out) + (prep_in - prep_out) + (serve_in - serve_out) + (hold_in - hold_out) <> 0 THEN 'missing_calls'
      ELSE 'complete'
    END AS status
    FROM base_calculations
    LEFT JOIN finish_info ON finish_info.inaccurate_time = TRUE AND finish_info.client_id = base_calculations.client_id AND finish_info.service_count = base_calculations.service_count AND finish_info.namespace = base_calculations.namespace
  ),
  flags AS (
    SELECT
    client_id,
    namespace,
    BOOL_OR(service_creation_flag) AS service_creation_flag_visit,
    BOOL_OR(waiting_flag) AS waiting_flag_visit,
    BOOL_OR(prep_flag) AS prep_flag_visit,
    BOOL_OR(serve_flag) AS serve_flag_visit,
    BOOL_OR(hold_flag) AS hold_flag_visit,
    BOOL_OR(inaccurate_time) AS inaccurate_time_visit,
    BOOL_OR(missing_calls_flag) AS missing_calls_flag_visit,
    BOOL_AND(no_wait_flag) AND (NOT BOOL_OR(inaccurate_time) OR BOOL_OR(inaccurate_time) IS NULL) AS no_wait_visit
    FROM item_list
    GROUP BY client_id, namespace
  )


  SELECT item_list.client_id,
      item_list.service_count,
      item_list.namespace,
      item_list.welcome_time,
      item_list.latest_time,

      item_list.service_creation_duration,
      item_list.waiting_duration,
      item_list.prep_duration,
      item_list.serve_duration,
      item_list.hold_duration,

      transaction_count,
      item_list.inaccurate_time,
      no_wait_visit,
      CASE
        WHEN (status = 'complete') THEN finish_type
        ELSE status
      END AS status,
      agent_info.agent_id,
      office_id,
      office_type,
      channel,
      program_id,
      program_name,
      parent_id,
      transaction_name,

      -----------------------------------
      -- A sort field on Channel, so that "in-person" shows first in the sort order
      CASE WHEN channel = 'in-person'
         THEN '0-in-person'
         ELSE channel
        END AS channel_sort,
      -----------------------------------
      -- Add a flag for back office transactions
      CASE WHEN program_name = 'Back Office' OR channel = 'back-office'
        THEN 'Back Office'
        ELSE 'Front Office'
        END as back_office,
      CASE WHEN missing_calls_flag_visit = TRUE OR service_creation_flag_visit = TRUE THEN NULL
        ELSE SUM(service_creation_duration) OVER (PARTITION BY item_list.client_id, item_list.namespace)
      END AS service_creation_duration_total,
      CASE WHEN missing_calls_flag_visit = TRUE OR waiting_flag_visit = TRUE THEN NULL
        ELSE SUM(waiting_duration) OVER (PARTITION BY item_list.client_id, item_list.namespace)
      END AS waiting_duration_total,
      CASE WHEN missing_calls_flag_visit = TRUE OR prep_flag_visit = TRUE THEN NULL
        ELSE SUM(prep_duration) OVER (PARTITION BY item_list.client_id, item_list.namespace)
      END AS prep_duration_total,
      CASE WHEN missing_calls_flag_visit = TRUE OR hold_flag_visit = TRUE THEN NULL
        ELSE SUM(hold_duration) OVER (PARTITION BY item_list.client_id, item_list.namespace)
      END AS hold_duration_total,
      CASE WHEN missing_calls_flag_visit = TRUE OR serve_flag_visit = TRUE OR inaccurate_time_visit = TRUE THEN NULL
        ELSE SUM(serve_duration) OVER (PARTITION BY item_list.client_id, item_list.namespace)
      END AS serve_duration_total,
      -----------------------------------
      -- Calculating zscores so we can filter out outliers
      -- To calculate the zscore, we use the formula (value - mean) / std_dev
      -- The std_dev and mean are to be calculated for all values for a given office
      --  (and with the code change below) program_id
      -- This is done using a Redshift Window Statement. For example, see:
      --     https://docs.aws.amazon.com/redshift/latest/dg/r_WF_AVG.html
      --
      -- We use the CASE statement to avoid dividing by zero. We could move this logic to the LookML below as well
      ---
      -- This example shows how to set it for both office_id and program_id.
      -- For now we just use office_id as we don't have a big enough data set yet
      --CASE WHEN (stddev(item_list.service_creation) over (PARTITION BY office_id, item_list.program_id)) <> 0
      --  THEN (item_list.service_creation - avg(item_list.service_creation) over (PARTITION BY office_id, item_list.program_id))
      --        / (stddev(item_list.service_creation) over (PARTITION BY office_id, item_list.program_id))
      --  ELSE NULL
      --END AS service_creation_zscore,
      -----------------------------------
      CASE WHEN (stddev(item_list.service_creation_duration) over (PARTITION BY office_id)) <> 0
        THEN (item_list.service_creation_duration - avg(item_list.service_creation_duration) over (PARTITION BY office_id))
           / (stddev(item_list.service_creation_duration) over (PARTITION BY office_id))
        ELSE NULL
      END AS service_creation_duration_zscore,
      CASE WHEN (stddev(item_list.waiting_duration) over (PARTITION BY office_id)) <> 0
        THEN (item_list.waiting_duration - avg(item_list.waiting_duration) over (PARTITION BY office_id))
           / (stddev(item_list.waiting_duration) over (PARTITION BY office_id))
        ELSE NULL
      END AS waiting_duration_zscore,
      CASE WHEN (stddev(item_list.prep_duration) over (PARTITION BY office_id)) <> 0
        THEN (item_list.prep_duration - avg(item_list.prep_duration) over (PARTITION BY office_id))
           / (stddev(item_list.prep_duration) over (PARTITION BY office_id))
        ELSE NULL
      END AS prep_duration_zscore,
      CASE WHEN (stddev(item_list.hold_duration) over (PARTITION BY office_id)) <> 0
        THEN (item_list.hold_duration - avg(item_list.hold_duration) over (PARTITION BY office_id))
           / (stddev(item_list.hold_duration) over (PARTITION BY office_id))
        ELSE NULL
      END AS hold_duration_zscore,
      CASE WHEN (stddev(item_list.serve_duration) over (PARTITION BY office_id)) <> 0
        THEN (item_list.serve_duration - avg(item_list.serve_duration) over (PARTITION BY office_id))
           / (stddev(item_list.serve_duration) over (PARTITION BY office_id))
        ELSE NULL
      END AS serve_duration_zscore,
      -- End zscore calculations
      ----------------------

      office_info.site AS office_name,
      office_info.officesize AS office_size,
      office_info.area AS area_number,
      office_info.current_area AS current_area,
      ----------------------
      dd.isweekend::BOOLEAN,
      dd.isholiday::BOOLEAN,
      dd.sbcquarter, dd.lastdayofpsapayperiod::date,
      to_char(item_list.welcome_time, 'HH24:00-HH24:59') AS hourly_bucket,
      CASE WHEN date_part(minute, item_list.welcome_time) < 30
        THEN to_char(item_list.welcome_time, 'HH24:00-HH24:29')
        ELSE to_char(item_list.welcome_time, 'HH24:30-HH24:59')
      END AS half_hour_bucket,
      to_char(item_list.welcome_time, 'HH24:MI:SS') AS date_time_of_day
      FROM item_list

      LEFT JOIN service_info ON service_info.client_id = item_list.client_id AND service_info.service_count = item_list.service_count AND service_info.namespace = item_list.namespace
      LEFT JOIN agent_info ON agent_info.client_id = item_list.client_id AND agent_info.service_count = item_list.service_count AND agent_info.namespace = item_list.namespace
      LEFT JOIN finish_info ON finish_info.client_id = item_list.client_id AND finish_info.service_count = item_list.service_count AND finish_info.namespace = item_list.namespace
      LEFT JOIN flags ON flags.client_id = item_list.client_id AND flags.namespace = item_list.namespace
      LEFT JOIN servicebc.office_info ON servicebc.office_info.rmsofficecode = office_id AND end_date IS NULL -- for now, get the most recent office info
      JOIN servicebc.datedimension AS dd on item_list.welcome_time::date = dd.datekey::date
      GROUP BY item_list.namespace,
      item_list.client_id,
      item_list.service_count,
      office_id,
      item_list.welcome_time,
      item_list.latest_time,
      transaction_count,
      item_list.inaccurate_time,
      status,
      finish_type,
      agent_info.agent_id,
      item_list.service_creation_duration,
      item_list.serve_duration,
      item_list.waiting_duration,
      item_list.prep_duration,
      item_list.hold_duration,
      service_creation_flag_visit,
      waiting_flag_visit,
      prep_flag_visit,
      serve_flag_visit,
      hold_flag_visit,
      inaccurate_time_visit,
      missing_calls_flag_visit,
      no_wait_visit,
      status,
      office_name,
      office_size,
      area_number,
      current_area,
      office_type,
      program_id,
      program_name,
      parent_id,
      transaction_name,
      channel,
      dd.isweekend,
      dd.isholiday,
      dd.sbcquarter, dd.lastdayofpsapayperiod::date
  ;;
          # https://docs.looker.com/data-modeling/learning-lookml/caching
    #distribution_style: all
    #sql_trigger_value: SELECT COUNT(*) FROM derived.theq_step1 ;;
}


  dimension: service_creation_flag {
    type: yesno
    sql: ${TABLE}.service_creation_flag ;;
    hidden: yes
  }

  dimension: waiting_flag {
    type: yesno
    sql: ${TABLE}.waiting_flag ;;
    hidden: yes
  }

  dimension: prep_flag {
    type: yesno
    sql: ${TABLE}.prep_flag ;;
    hidden: yes
  }

  dimension: serve_flag {
    type: yesno
    sql: ${TABLE}.serve_flag ;;
    hidden: yes
  }

  dimension: hold_flag {
    type: yesno
    sql: ${TABLE}.hold_flag ;;
    hidden: yes
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
    sql:  ${TABLE}.transaction_count ;;
    group_label: "Counts"
  }

  measure: dummy_for_back_office {
    type: number
    sql: 1=1 ;;
    hidden: yes
    drill_fields: [channel, program_name, prep_duration_total, service_creation_duration_total, serve_duration_total]
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
      {% assign table_calc = '[{\"table_calculation\":\"total_time_credit\",\"label\":\"Total Time Credit\",\"expression\":\"${cfms_poc.prep_duration_total}+${cfms_poc.service_creation_duration_total}+${cfms_poc.serve_duration_total}\",\"value_format\":\"[h]:mm:ss\",\"value_format_name\":null,\"_kind_hint\":\"measure\",\"_type_hint\":\"number\"},{\"table_calculation\":\"total_time_credit_percent\",\"label\":\"Total Time Credit Percent\",\"expression\":\"${total_time_credit}/sum(pivot_row(${total_time_credit}))\",\"value_format\":null,\"value_format_name\":\"percent_1\",\"_kind_hint\":\"measure\",\"_type_hint\":\"number\"}]' %}
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
      \"cfms_poc.service_creation_duration_total\" ,
      \"cfms_poc.serve_duration_total\" ,
      \"total_time_credit_percent\"] ,
      \"y_axes\":[]}' %}

      {{ dummy_for_back_office._link }}&vis_config={{ vis_config | encode_uri }}&pivots=cfms_poc.channel&sorts=cfms_poc.channel 0,total_time_credit desc 3&limit=1000&column_limit=50&filter_config={{ filter_config | encode_uri }}&dynamic_fields={{ table_calc | replace: '  ', '' | encode_uri }}"

    }
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
  dimension: serve_duration_bucket_sort {
    description: "Use in combination with Serve Duration Bucket to enforce sort order. Hide from display."
    case: {
      when: {
        sql: ${TABLE}.serve_duration < 300 ;;
        label: "1. 0-5"
      }
      when: {
        sql:  ${TABLE}.serve_duration < 1200 ;;
        label: "2. 5-20"
      }
      when: {
        sql: ${TABLE}.serve_duration < 3600 ;;
        label: "3. 20-60"
      }
      when: {
        sql: ${TABLE}.serve_duration >= 3600 ;;
        label: "4. 60+"
      }
      else:"5. Unknown"
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
  # Waiting Duration by Visit
  dimension: waiting_duration_bucket_sort {
    description: "Use in combination with Waiting Duration Bucket to enforce sort order. Hide from display."
    case: {
      when: {
        sql: ${TABLE}.waiting_duration_total < 300 ;;
        label: "1. 0-5"
      }
      when: {
        sql:  ${TABLE}.waiting_duration_total < 1200 ;;
        label: "2. 5-20"
      }
      when: {
        sql: ${TABLE}.waiting_duration_total < 3600 ;;
        label: "3. 20-60"
      }
      when: {
        sql: ${TABLE}.waiting_duration_total >= 3600 ;;
        label: "4. 60+"
      }
      else:"5. Unknown"
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
  dimension: no_wait_visit {
    description: "Did the visit skip the line?"
    type: yesno
    sql: ${TABLE}.no_wait_visit ;;
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
  dimension: latest_time {
    description: "Time of the latest interaction for this service."
    type: date_time
    sql: ${TABLE}.latest_time ;;
    group_label: "Timing Points"
  }
  measure: count_of_days {
    type: number
    sql: count(distinct date(${TABLE}.welcome_time));;
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
#    dimension: stand_time {
#      type: date_time
#      sql: ${TABLE}.stand_time ;;
#      group_label: "Timing Points"
#    }
#    dimension: invite_time {
#      type: date_time
#      sql: ${TABLE}.invite_time ;;
#      group_label: "Timing Points"
#    }
#    dimension: start_time {
#      type: date_time
#      sql: ${TABLE}.start_time ;;
#      group_label: "Timing Points"
#    }
#    dimension: chooseservice_time {
#      type: date_time
#      sql:  ${TABLE}.chooseservice_time ;;
#      group_label: "Timing Points"
#    }
#    dimension: finish_time {
#      type: date_time
#      sql: ${TABLE}.finish_time ;;
#      group_label: "Timing Points"
#    }
#    dimension: hold_time {
#      type: date_time
#      sql: ${TABLE}.hold_time ;;
#      group_label: "Timing Points"
#    }
#    dimension: invitefromhold_time {
#      type: date_time
#      sql: ${TABLE}.invitefromhold_time ;;
#      group_label: "Timing Points"
#    }
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

      {{ dummy._link }}&vis_config={{ vis_config | encode_uri }}&pivots=cfms_poc.channel&sorts=cfms_poc.channel 0,cfms_poc.transaction_count desc 6&limit=1000&column_limit=50&row_total=right&filter_config={{ filter_config| encode_uri }}&dynamic_fields={{ table_calc | replace: '  ', '' | encode_uri }}"
    }
  }
  measure: dummy_service_count {
    type: number
    sql: 1=1 ;;
    hidden: yes
    drill_fields: [transaction_name, prep_duration_per_visit_average, serve_duration_per_visit_average, service_creation_duration_per_visit_average]
  }

  dimension: program_name_with_drill_2 {
    type: string
    sql: ${TABLE}.program_name ;;
    group_label: "Program Information"
    description: "Program Name with drill to Service Time."
    link: {
      label: "Service Time"
      url: "
      {% assign table_calc = '[{\"table_calculation\":\"average_service_time\",\"label\":\"Average Service Time\",\"expression\":\"${cfms_poc.prep_duration_per_visit_average}+${cfms_poc.serve_duration_per_visit_average}+${cfms_poc.service_creation_duration_per_visit_average}\",\"value_format\":\"[h]:mm:ss\",\"value_format_name\":null,\"_kind_hint\":\"measure\",\"_type_hint\":\"number\"}]' %}
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
      \"cfms_poc.service_creation_duration_per_visit_average\" ,
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
  dimension: status {
    description: "Whether the client's interaction finished successfully, left, or their ticket is still open."
    type:  string
    sql: COALESCE(${TABLE}.status, 'Open Ticket');;
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

  # is_in_current_period_or_last_period determines which sessions occur on an after the start of the last_period
  # and before end of the current_period, as selected on the flexible_filter_date_range filter in an Explore.
  # Here's an explanation of why we use DATEDIFF(SECOND and not DAY
  #    https://www.sqlteam.com/articles/datediff-function-demystified
  filter: is_in_current_period_or_last_period {
    type: yesno
    sql:  DATEDIFF(SECOND,${TABLE}.welcome_time, {% date_start flexible_filter_date_range %}) / 86400.0 <= ${period_difference}
      AND ${TABLE}.welcome_time < {% date_end flexible_filter_date_range %} ;;
  }


  # current period identifies sessions falling between the start and end of the date range selected
  dimension: current_period {
    group_label: "Flexible Filter"
    type: yesno
    sql: ${TABLE}.welcome_time >= {% date_start flexible_filter_date_range %}
      AND ${TABLE}.welcome_time < {% date_end flexible_filter_date_range %} ;;
    hidden: yes
  }

  # last_period selects the the sessions that occurred immediately prior to the current_session and
  # over the same number of days as the current_session.
  # For instance, it would provide a suitable comparison of data from one week to the next.
  dimension: last_period {
    group_label: "Flexible Filter"
    type: yesno
    sql: ${TABLE}.welcome_time >= DATEADD(DAY, -${period_difference}, {% date_start flexible_filter_date_range %})
      AND ${TABLE}.welcome_time < {% date_start flexible_filter_date_range %} ;;
    hidden: yes
  }

  # dimension: date_window provides the pivot label for constructing tables and charts
  # that compare current_period and last_period
  dimension: date_window {
    group_label: "Flexible Filter"
    case: {
      when: {
        sql: ${TABLE}.welcome_time >= {% date_start flexible_filter_date_range %}
          AND ${TABLE}.welcome_time < {% date_end flexible_filter_date_range %} ;;
        label: "current_period"
      }
      when: {
        sql: ${TABLE}.welcome_time >= DATEADD(DAY, -${period_difference}, {% date_start flexible_filter_date_range %})
          AND ${TABLE}.welcome_time < {% date_start flexible_filter_date_range %} ;;
        label: "last_period"
      }
      else: "unknown"
    }
    description: "Pivot on Date Window to compare measures between the current and last periods, use with Comparison Date"
  }

  # comparison_date returns dates in the current_period providing a positive offset of
  # the last_period date range. Exploring comparison_date with any measure and a pivot
  # on date_window results in a pointwise comparison of current and last periods
  #
  dimension: comparison_date {
    group_label: "Flexible Filter"
    required_fields: [date_window]
    description: "Comparison Date offsets measures from the last period to appear in the range of the current period,
    allowing a pairwise comparison between these periods when used with Date Window."
    type: date
    sql:
       CASE
         WHEN ${TABLE}.welcome_time >= {% date_start flexible_filter_date_range %}
             AND ${TABLE}.welcome_time < {% date_end flexible_filter_date_range %}
            THEN ${TABLE}.welcome_time
         WHEN ${TABLE}.welcome_time >= DATEADD(DAY, -${period_difference}, {% date_start flexible_filter_date_range %})
             AND ${TABLE}.welcome_time < {% date_start flexible_filter_date_range %}
            THEN DATEADD(DAY,${period_difference},${TABLE}.welcome_time)
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
