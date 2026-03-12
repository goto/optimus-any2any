package otel

const (
	Record            = "optimus_plugin_%s_record"           // counter
	RecordBytes       = "optimus_plugin_%s_record_bytes"     // counter
	RecordBytesBucket = "optimus_plugin_%s_record_bytes"     // histogram
	ProcessLimits     = "optimus_plugin_%s_process_limits"   // gauge
	Process           = "optimus_plugin_%s_process"          // gauge
	ProcessDuration   = "optimus_plugin_%s_process_duration" // histogram
	Retry             = "optimus_plugin_%s_retry"            // counter
)
