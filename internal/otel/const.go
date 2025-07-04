package otel

const (
	InstrumentationVersion = "0.0.1"

	SourceRecordCount       = "optimus_plugin_source_record_count"        // counter
	SourceRecordBytes       = "optimus_plugin_source_record_bytes"        // counter
	SourceRecordBytesBucket = "optimus_plugin_source_record_bytes_bucket" // histogram
	SinkRecordCount         = "optimus_plugin_sink_record_count"          // counter
	SinkRecordBytes         = "optimus_plugin_sink_record_bytes"          // counter
	SinkRecordBytesBucket   = "optimus_plugin_sink_record_bytes_bucket"   // histogram
	RetryCount              = "optimus_plugin_retry_count"                // counter
	ConnectorBytes          = "optimus_plugin_connector_bytes"            // counter
	ConnectorBytesBucket    = "optimus_plugin_connector_bytes_bucket"     // histogram
	SourceProcess           = "optimus_plugin_source_process"             // gauge
	SinkProcess             = "optimus_plugin_sink_process"               // gauge
	ConnectorProcess        = "optimus_plugin_connector_process"          // gauge
	SourceProcessLimits     = "optimus_plugin_source_process_limits"      // gauge
	SinkProcessLimits       = "optimus_plugin_sink_process_limits"        // gauge
	ConnectorProcessLimits  = "optimus_plugin_connector_process_limits"   // gauge
)
