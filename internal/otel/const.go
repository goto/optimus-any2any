package otel

const (
	InstrumentationVersion = "0.0.1"

	SourceRecordCount       = "optimus_plugin_source_record_count"        // counter
	SourceRecordBytes       = "optimus_plugin_source_record_bytes"        // counter
	SourceRecordBytesBucket = "optimus_plugin_source_record_bytes_bucket" // histogram
	SourceProcessLimits     = "optimus_plugin_source_process_limits"      // gauge
	SourceProcessCount      = "optimus_plugin_source_process_count"       // gauge
	SinkRecordCount         = "optimus_plugin_sink_record_count"          // counter
	SinkRecordBytes         = "optimus_plugin_sink_record_bytes"          // counter
	SinkRecordBytesBucket   = "optimus_plugin_sink_record_bytes_bucket"   // histogram
	SinkProcessLimits       = "optimus_plugin_sink_process_limits"        // gauge
	SinkProcessCount        = "optimus_plugin_sink_process_count"         // gauge
	ConnectorBytes          = "optimus_plugin_connector_bytes"            // counter
	ConnectorBytesBucket    = "optimus_plugin_connector_bytes_bucket"     // histogram
	ConnectorProcessCount   = "optimus_plugin_connector_process_count"    // gauge
	ConnectorProcessLimits  = "optimus_plugin_connector_process_limits"   // gauge
	RetryCount              = "optimus_plugin_retry_count"                // counter
)
