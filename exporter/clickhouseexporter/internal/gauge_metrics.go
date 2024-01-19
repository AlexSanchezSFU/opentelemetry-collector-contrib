// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package internal // import "github.com/open-telemetry/opentelemetry-collector-contrib/exporter/clickhouseexporter/internal"

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.uber.org/zap"
)

const (
	// language=ClickHouse SQL
	createGaugeTableSQL = `
CREATE TABLE IF NOT EXISTS %s_gauge_local on cluster '{cluster}' (
    ResourceAttributes Map(LowCardinality(String), String) CODEC(ZSTD(1)),
    ResourceSchemaUrl String CODEC(ZSTD(1)),
    ScopeName String CODEC(ZSTD(1)),
    ScopeVersion String CODEC(ZSTD(1)),
    ScopeAttributes Map(LowCardinality(String), String) CODEC(ZSTD(1)),
    ScopeDroppedAttrCount UInt32 CODEC(ZSTD(1)),
    ScopeSchemaUrl String CODEC(ZSTD(1)),
    MetricName String CODEC(ZSTD(1)),
    MetricDescription String CODEC(ZSTD(1)),
    MetricUnit String CODEC(ZSTD(1)),
    Attributes Map(LowCardinality(String), String) CODEC(ZSTD(1)),
	session_id FixedString(36),
    installation_id FixedString(36),
    app_id LowCardinality(String),
    app_environment LowCardinality(String),
    app_version LowCardinality(String),
    device_name LowCardinality(String),
    device_platform LowCardinality(String),
    device_os_version LowCardinality(String),
    carrier LowCardinality(String),
    network LowCardinality(String),
    ip String CODEC(ZSTD(1)),
    user_id FixedString(36),
    operation LowCardinality(String),
    bf LowCardinality(String),
    type LowCardinality(String),
	status LowCardinality(String),
    reason LowCardinality(String),
    StartTimeUnix DateTime64(9) CODEC(Delta, ZSTD(1)),
    TimeUnix DateTime64(9) CODEC(Delta, ZSTD(1)),
    Value Float64 CODEC(ZSTD(1)),
    Flags UInt32 CODEC(ZSTD(1)),
    Exemplars Nested (
		FilteredAttributes Map(LowCardinality(String), String),
		TimeUnix DateTime64(9),
		Value Float64,
		SpanId String,
		TraceId String
    ) CODEC(ZSTD(1)),
	INDEX idx_res_attr_key mapKeys(ResourceAttributes) TYPE bloom_filter(0.01) GRANULARITY 1,
	INDEX idx_res_attr_value mapValues(ResourceAttributes) TYPE bloom_filter(0.01) GRANULARITY 1,
	INDEX idx_scope_attr_key mapKeys(ScopeAttributes) TYPE bloom_filter(0.01) GRANULARITY 1,
	INDEX idx_scope_attr_value mapValues(ScopeAttributes) TYPE bloom_filter(0.01) GRANULARITY 1,
	INDEX idx_attr_key mapKeys(Attributes) TYPE bloom_filter(0.01) GRANULARITY 1,
	INDEX idx_attr_value mapValues(Attributes) TYPE bloom_filter(0.01) GRANULARITY 1
) ENGINE ReplicatedMergeTree('/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}', '{replica}')
%s
PARTITION BY toDate(TimeUnix)
ORDER BY (MetricName, Attributes, toUnixTimestamp64Nano(TimeUnix))
SETTINGS index_granularity=8192, ttl_only_drop_parts = 1;
`
	// language=ClickHouse SQL
	insertGaugeTableSQL = `INSERT INTO %s_gauge (
    ResourceAttributes,
    ResourceSchemaUrl,
    ScopeName,
    ScopeVersion,
    ScopeAttributes,
    ScopeDroppedAttrCount,
    ScopeSchemaUrl,
    MetricName,
    MetricDescription,
    MetricUnit,
    Attributes,
	session_id,
    installation_id,
    app_id,
    app_environment,
    app_version,
    device_name,
    device_platform,
    device_os_version,
    carrier,
    network,
    ip,
    user_id,
    operation,
    bf,
    type,
	status,
    reason,
    StartTimeUnix,
    TimeUnix,
    Value,
    Flags,
    Exemplars.FilteredAttributes,
	Exemplars.TimeUnix,
    Exemplars.Value,
    Exemplars.SpanId,
    Exemplars.TraceId) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`


	createGaugeTableClusterSQL = `CREATE TABLE %[1]s_gauge on cluster '{cluster}' AS %[1]s_gauge_local
	ENGINE = Distributed('{cluster}', currentDatabase(), %[1]s_gauge_local, rand());`
)

type gaugeModel struct {
	metricName        string
	metricDescription string
	metricUnit        string
	metadata          *MetricsMetaData
	gauge             pmetric.Gauge
}

type gaugeMetrics struct {
	gaugeModels []*gaugeModel
	insertSQL   string
	count       int
}

func (g *gaugeMetrics) insert(ctx context.Context, db *sql.DB) error {
	if g.count == 0 {
		return nil
	}
	start := time.Now()
	err := doWithTx(ctx, db, func(tx *sql.Tx) error {
		statement, err := tx.PrepareContext(ctx, g.insertSQL)
		if err != nil {
			return err
		}

		defer func() {
			_ = statement.Close()
		}()

		for _, model := range g.gaugeModels {
			for i := 0; i < model.gauge.DataPoints().Len(); i++ {
				dp := model.gauge.DataPoints().At(i)
				attrs, times, values, traceIDs, spanIDs := convertExemplars(dp.Exemplars())
				mappedAttributes := attributesToMap(dp.Attributes())

				sessionID := mappedAttributes["session_id"]
				delete(mappedAttributes, "session_id")

				installationID := mappedAttributes["installation_id"]
				delete(mappedAttributes, "installation_id")

				appID := mappedAttributes["app_id"]
				delete(mappedAttributes, "app_id")

				appEnvironment := mappedAttributes["app_environment"]
				delete(mappedAttributes, "app_environment")

				appVersion := mappedAttributes["app_version"]
				delete(mappedAttributes, "app_version")

				deviceName := mappedAttributes["device_name"]
				delete(mappedAttributes, "device_name")

				devicePlatform := mappedAttributes["device_platform"]
				delete(mappedAttributes, "device_platform")

				deviceOsVersion := mappedAttributes["device_os_version"]
				delete(mappedAttributes, "device_os_version")

				carrier := mappedAttributes["carrier"]
				delete(mappedAttributes, "carrier")

				network := mappedAttributes["network"]
				delete(mappedAttributes, "network")

				ip := mappedAttributes["ip"]
				delete(mappedAttributes, "ip")

				userID := mappedAttributes["user_id"]
				delete(mappedAttributes, "user_id")

				operation := mappedAttributes["operation"]
				delete(mappedAttributes, "operation")

				bf := mappedAttributes["bf"]
				delete(mappedAttributes, "bf")

				tipo := mappedAttributes["type"]
				delete(mappedAttributes, "type")

				status := mappedAttributes["status"]
				delete(mappedAttributes, "status")

				reason := mappedAttributes["reason"]
				delete(mappedAttributes, "reason")

				_, err = statement.ExecContext(ctx,
					model.metadata.ResAttr,
					model.metadata.ResURL,
					model.metadata.ScopeInstr.Name(),
					model.metadata.ScopeInstr.Version(),
					attributesToMap(model.metadata.ScopeInstr.Attributes()),
					model.metadata.ScopeInstr.DroppedAttributesCount(),
					model.metadata.ScopeURL,
					model.metricName,
					model.metricDescription,
					model.metricUnit,
					mappedAttributes,
					sessionID,
					installationID,
					appID,
					appEnvironment,
					appVersion,
					deviceName,
					devicePlatform,
					deviceOsVersion,
					carrier,
					network,
					ip,
					userID,
					operation,
					bf,
					tipo,
					status,
					reason,
					dp.StartTimestamp().AsTime(),
					dp.Timestamp().AsTime(),
					getValue(dp.IntValue(), dp.DoubleValue(), dp.ValueType()),
					uint32(dp.Flags()),
					attrs,
					times,
					values,
					traceIDs,
					spanIDs,
				)
				if err != nil {
					return fmt.Errorf("ExecContext:%w", err)
				}
			}
		}
		return err
	})
	duration := time.Since(start)
	if err != nil {
		logger.Debug("insert gauge metrics fail", zap.Duration("cost", duration))
		return fmt.Errorf("insert gauge metrics fail:%w", err)
	}
	return nil
}

func (g *gaugeMetrics) Add(resAttr map[string]string, resURL string, scopeInstr pcommon.InstrumentationScope, scopeURL string, metrics any, name string, description string, unit string) error {
	gauge, ok := metrics.(pmetric.Gauge)
	if !ok {
		return fmt.Errorf("metrics param is not type of Gauge")
	}
	g.count += gauge.DataPoints().Len()
	g.gaugeModels = append(g.gaugeModels, &gaugeModel{
		metricName:        name,
		metricDescription: description,
		metricUnit:        unit,
		metadata: &MetricsMetaData{
			ResAttr:    resAttr,
			ResURL:     resURL,
			ScopeURL:   scopeURL,
			ScopeInstr: scopeInstr,
		},
		gauge: gauge,
	})
	return nil
}
