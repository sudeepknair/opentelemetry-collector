// Copyright  The OpenTelemetry Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package elasticsearch

import (
	"context"
	"fmt"

	"os"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/config/configmodels"
	"go.opentelemetry.io/collector/consumer/pdata"
	"go.opentelemetry.io/collector/exporter/exporterhelper"
	tracetranslator "go.opentelemetry.io/collector/translator/trace"
)

type elasticExporter struct {
	logger   *zap.Logger
	debug    bool
	endpoint string
	esHandle *elasticHandle
}

func createTraceExporter(
	_ context.Context,
	params component.ExporterCreateParams,
	cfg configmodels.Exporter,
) (component.TracesExporter, error) {
	es := cfg.(*Config)

	esExporter, err := createElasticExporter(es)
	if err != nil {
		return nil, err
	}
	return exporterhelper.NewTraceExporter(
		es,
		params.Logger,
		esExporter.pushTraceData,
	)
}

func getElasticEndpoint(cfg *Config) string {

	host := os.Getenv("ELASTIC_SEARCH_SERVER")
	if host == "" {
		host = "127.0.0.1"
	}

	if cfg.Server == "" {
		host = cfg.Server
	}

	port := os.Getenv("ELASTIC_SEARCH_CLIENT")
	if port == "" {
		port = "9200"
	}

	if cfg.Port == "" {
		host = cfg.Port
	}

	endpoint := fmt.Sprintf("%s:%s", host, port)
	fmt.Println("Using elastic endpoint:", endpoint)
	return endpoint
}

func createLogger(cfg *Config) (*zap.Logger, error) {
	var level zapcore.Level
	err := (&level).UnmarshalText([]byte("info"))
	if err != nil {
		return nil, err
	}

	// We take development config as the base since it matches the purpose
	// of logging exporter being used for debugging reasons (so e.g. console encoder)
	conf := zap.NewDevelopmentConfig()
	conf.Level = zap.NewAtomicLevelAt(level)

	logginglogger, err := conf.Build()
	if err != nil {
		return nil, err
	}
	return logginglogger, nil
}

func createElasticExporter(cfg *Config) (*elasticExporter, error) {
	logger, err := createLogger(cfg)
	if err != nil {
		return nil, err
	}


	es := &elasticHandle {

	}
	es.initialize(getElasticEndpoint(cfg),logger)

	ze := &elasticExporter{
		logger:   logger,
		debug:    false,
		endpoint: getElasticEndpoint(cfg),
		esHandle: es,
	}
	return ze, nil
}

func (s *elasticExporter) pushTraceData(
	_ context.Context,
	td pdata.Traces,
) (int, error) {

	s.logger.Info("TracesExporter", zap.Int("#spans", td.SpanCount()))

	resourceSpans := td.ResourceSpans()

	for i := 0; i < resourceSpans.Len(); i++ {
		rs := resourceSpans.At(i)

		resource := rs.Resource()
		ilss := rs.InstrumentationLibrarySpans()

		if ilss.Len() == 0 {
			continue
		}

		svcName := "unknown"
		attrs := resource.Attributes()
		zTags := make(map[string]string)
		if attrs.Len() != 0 {
			attrs.ForEach(func(k string, v pdata.AttributeValue) {
				zTags[k] = tracetranslator.AttributeValueToString(v, false)
				s.logger.Info(fmt.Sprintf("----> Key:%s value:%s",k,tracetranslator.AttributeValueToString(v, false)))
				if k == "service.name" {
					svcName = tracetranslator.AttributeValueToString(v, false)
				}
			})
		}



		for i := 0; i < ilss.Len(); i++ {
			ils := ilss.At(i)

			spans := ils.Spans()
			for j := 0; j < spans.Len(); j++ {
				span := spans.At(j)
			
				//TODO: GET SPAN Info
				st := span.StartTime()
				et := span.EndTime()

				status := "unknown"

				traceId := span.TraceID().HexString()
				spanId := span.SpanID().HexString()
				parentSpanId := span.ParentSpanID().HexString()
				name := span.Name()

				s.logger.Info(fmt.Sprintf("svc:%s span name:%v tid:%v sid:%v , spid: %v status:%v startTs:%d endTs:%d",svcName,name,traceId,spanId,parentSpanId,status,st,et))
				s.esHandle.WriteData(spanId,parentSpanId,traceId,status,svcName,name ,uint64(st),uint64(et))
			}
		}


	}


	return 0, nil
}
