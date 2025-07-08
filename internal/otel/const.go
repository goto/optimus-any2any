package otel

const (
	InstrumentationVersion = "0.0.2"

	SourceRecord            = "optimus_plugin_source_record"            // counter
	SourceRecordBytes       = "optimus_plugin_source_record_bytes"      // counter
	SourceRecordBytesBucket = "optimus_plugin_source_record_bytes"      // histogram
	SourceProcessLimits     = "optimus_plugin_source_process_limits"    // gauge
	SourceProcess           = "optimus_plugin_source_process"           // gauge
	SinkRecord              = "optimus_plugin_sink_record"              // counter
	SinkRecordBytes         = "optimus_plugin_sink_record_bytes"        // counter
	SinkRecordBytesBucket   = "optimus_plugin_sink_record_bytes"        // histogram
	SinkProcessLimits       = "optimus_plugin_sink_process_limits"      // gauge
	SinkProcess             = "optimus_plugin_sink_process"             // gauge
	ConnectorBytes          = "optimus_plugin_connector_bytes"          // counter
	ConnectorBytesBucket    = "optimus_plugin_connector_bytes"          // histogram
	ConnectorProcess        = "optimus_plugin_connector_process"        // gauge
	ConnectorProcessLimits  = "optimus_plugin_connector_process_limits" // gauge
	Retry                   = "optimus_plugin_retry"                    // counter
)
