package googleanalytics

import (
	"fmt"

	"github.com/goto/optimus-any2any/internal/component/common"
	"github.com/goto/optimus-any2any/internal/model"
	"github.com/goto/optimus-any2any/pkg/flow"
	"github.com/pkg/errors"
	data "google.golang.org/genproto/googleapis/analytics/data/v1beta"
)

// GASource is a source that reads data from Google Analytics.
type GASource struct {
	common.Source
	client *GAClient
	req    *data.RunReportRequest
}

var _ flow.Source = (*GASource)(nil)

// NewSource creates a new Google Analytics source.
func NewSource(commonSource common.Source, svcAcc string, tlsCert, tlsCACert, tlsKey string, propertyID string, startDate, endDate string, dimensions, metrics []string, batchSize int64) (*GASource, error) {
	// create client
	client, err := NewClient(commonSource.Context(), svcAcc, tlsCert, tlsCACert, tlsKey, propertyID)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	// create request
	req := createRequest(propertyID, startDate, endDate, dimensions, metrics, batchSize, 0)

	// create source
	s := &GASource{
		Source: commonSource,
		client: client,
		req:    req,
	}

	// add clean func
	commonSource.AddCleanFunc(func() error {
		if err := s.client.Close(); err != nil {
			return errors.WithStack(err)
		}
		s.Logger().Info("close google analytics client")
		return nil
	})

	// register process, it will immediately start the process
	commonSource.RegisterProcess(s.Process)

	return s, nil
}

func (s *GASource) Process() error {
	// run report
	rowCount := int64(0)

	for rowCount == 0 || s.req.Offset < rowCount {
		// execute request
		resp, err := s.client.RunReport(s.Context(), s.req)
		if err != nil {
			return errors.WithStack(err)
		}
		// send report to sink
		s.Logger().Info(fmt.Sprintf("send %d records", len(resp.Rows)))
		for _, row := range resp.Rows {
			// convert row to record
			record := model.NewRecord()
			for i, dimension := range resp.DimensionHeaders {
				record.Set(dimension.Name, row.DimensionValues[i].GetValue())
			}
			for i, metric := range resp.MetricHeaders {
				record.Set(metric.Name, row.MetricValues[i].GetValue())
			}
			// send record to sink
			if err := s.SendRecord(record); err != nil {
				return errors.WithStack(err)
			}
		}
		// only update rowCount once
		if rowCount == 0 {
			rowCount = int64(resp.RowCount)
		}
		// update offset
		s.req.Offset += s.req.Limit
	}

	s.Logger().Info(fmt.Sprintf("successfully sent %d records", rowCount))
	return nil
}

func createRequest(propertyID string, startDate, endDate string, dimensions, metrics []string, limit, offset int64) *data.RunReportRequest {
	// create dimensions
	dataDimensions := make([]*data.Dimension, len(dimensions))
	for i, d := range dimensions {
		dataDimensions[i] = &data.Dimension{Name: d}
	}
	// create metrics
	dataMetrics := make([]*data.Metric, len(metrics))
	for i, m := range metrics {
		dataMetrics[i] = &data.Metric{Name: m}
	}
	// create request
	return &data.RunReportRequest{
		Property:   "properties/" + propertyID,
		Dimensions: dataDimensions,
		Metrics:    dataMetrics,
		DateRanges: []*data.DateRange{
			{StartDate: startDate, EndDate: endDate},
		},
		Limit:  limit,
		Offset: offset,
	}
}
