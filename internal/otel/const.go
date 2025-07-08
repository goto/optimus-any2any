package otel

const (
	InstrumentationVersion = "0.0.2"

	SourceRecord               = "optimus_plugin_source_record"                 // counter
	SourceRecordBytes          = "optimus_plugin_source_record_bytes"           // counter
	SourceRecordBytesBucket    = "optimus_plugin_source_record_bytes"           // histogram
	SourceProcessLimits        = "optimus_plugin_source_process_limits"         // gauge
	SourceProcess              = "optimus_plugin_source_process"                // gauge
	SourceProcessDurationMs    = "optimus_plugin_source_process_duration_ms"    // histogram
	SinkRecord                 = "optimus_plugin_sink_record"                   // counter
	SinkRecordBytes            = "optimus_plugin_sink_record_bytes"             // counter
	SinkRecordBytesBucket      = "optimus_plugin_sink_record_bytes"             // histogram
	SinkProcessLimits          = "optimus_plugin_sink_process_limits"           // gauge
	SinkProcess                = "optimus_plugin_sink_process"                  // gauge
	SinkProcessDurationMs      = "optimus_plugin_sink_process_duration_ms"      // histogram
	ConnectorBytes             = "optimus_plugin_connector_bytes"               // counter
	ConnectorBytesBucket       = "optimus_plugin_connector_bytes"               // histogram
	ConnectorProcessLimits     = "optimus_plugin_connector_process_limits"      // gauge
	ConnectorProcess           = "optimus_plugin_connector_process"             // gauge
	ConnectorProcessDurationMs = "optimus_plugin_connector_process_duration_ms" // histogram
	Retry                      = "optimus_plugin_retry"                         // counter
	ProcessDurationMs          = "optimus_plugin_process_duration_ms"           // histogram
)
