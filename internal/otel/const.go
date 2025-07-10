package otel

const (
	InstrumentationVersion = "0.0.1"

	Record            = "optimus_plugin_record"           // counter
	RecordBytes       = "optimus_plugin_record_bytes"     // counter
	RecordBytesBucket = "optimus_plugin_record_bytes"     // histogram
	ProcessLimits     = "optimus_plugin_process_limits"   // gauge
	Process           = "optimus_plugin_process"          // gauge
	ProcessDuration   = "optimus_plugin_process_duration" // histogram
	Retry             = "optimus_plugin_retry"            // counter
)
