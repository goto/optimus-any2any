package otel

const (
	InstrumentationVersion = "0.0.2"

	SourceRecord             = "optimus_plugin_source_record"              // counter
	SourceRecordBytes        = "optimus_plugin_source_record_bytes"        // counter
	SourceRecordBytesBucket  = "optimus_plugin_source_record_bytes"        // histogram
	SourceProcessLimits      = "optimus_plugin_source_process_limits"      // gauge
	SourceProcess            = "optimus_plugin_source_process"             // gauge
	SourceProcessDuration    = "optimus_plugin_source_process_duration"    // histogram
	SinkRecord               = "optimus_plugin_sink_record"                // counter
	SinkRecordBytes          = "optimus_plugin_sink_record_bytes"          // counter
	SinkRecordBytesBucket    = "optimus_plugin_sink_record_bytes"          // histogram
	SinkProcessLimits        = "optimus_plugin_sink_process_limits"        // gauge
	SinkProcess              = "optimus_plugin_sink_process"               // gauge
	SinkProcessDuration      = "optimus_plugin_sink_process_duration"      // histogram
	ConnectorBytes           = "optimus_plugin_connector_bytes"            // counter
	ConnectorBytesBucket     = "optimus_plugin_connector_bytes"            // histogram
	ConnectorProcessLimits   = "optimus_plugin_connector_process_limits"   // gauge
	ConnectorProcess         = "optimus_plugin_connector_process"          // gauge
	ConnectorProcessDuration = "optimus_plugin_connector_process_duration" // histogram
	Retry                    = "optimus_plugin_retry"                      // counter
	ProcessDuration          = "optimus_plugin_process_duration"           // histogram
)
