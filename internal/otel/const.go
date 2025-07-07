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
	SourceProcessCount      = "optimus_plugin_source_process_count"       // counter
	SinkProcessCount        = "optimus_plugin_sink_process_count"         // counter
	ConnectorProcessCount   = "optimus_plugin_connector_process_count"    // counter
	ProcessLimits           = "optimus_plugin_process_limits"             // counter
	ProcessCount            = "optimus_plugin_process_count"              // counter
	SourceProcessLimits     = "optimus_plugin_source_process_limits"      // counter
	SinkProcessLimits       = "optimus_plugin_sink_process_limits"        // counter
	ConnectorProcessLimits  = "optimus_plugin_connector_process_limits"   // counter
)
