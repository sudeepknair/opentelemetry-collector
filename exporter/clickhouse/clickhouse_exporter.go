package clickhouse

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/ClickHouse/clickhouse-go"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/config/configmodels"
	"go.opentelemetry.io/collector/consumer/pdata"
	"go.opentelemetry.io/collector/exporter/exporterhelper"
	. "go.opentelemetry.io/collector/translator/trace"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

const (
	publishInterval = 5
)


var (
	opsProcessed = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "otel_collector_signals",
		Help: "The total number of processed spans",
	})

	spansPerRequest = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:      "otel_span_count",
		Help:      "CLI application execution in seconds",
		Buckets:   []float64{5, 10, 25,  50, 75, 100, 200, 500, 1000, 10000, 100000},
	})

)



type clickHouseExporter struct {
	logger   *zap.Logger
	debug    bool
	endpoint string
	db *sql.DB
	receiver chan[]pdata.ResourceSpansSlice
}

func createTraceExporter(
	_ context.Context,
	params component.ExporterCreateParams,
	cfg configmodels.Exporter,
) (component.TracesExporter, error) {
	es := cfg.(*Config)

	esExporter, err := createCHExporter(es)
	if err != nil {
		return nil, err
	}
	return exporterhelper.NewTraceExporter(
		es,
		params.Logger,
		esExporter.pushTraceData,
	)
}


func createCHExporter(cfg *Config) (*clickHouseExporter, error) {
	logger, err := createLogger(cfg)
	if err != nil {
		return nil, err
	}

	server := "127.0.0.1"
	if cfg.Server != "" {
		server = cfg.Server
	}
	port := "9000"
	if cfg.Port != "" {
		port = cfg.Port
	}

	endpoint := fmt.Sprintf("tcp://%s:%s?debug=true",server,port)
	connect, err := sql.Open("clickhouse", endpoint)
	if err != nil {
		log.Fatal(err)
	}

	prometheus.Register(opsProcessed)
	prometheus.Register(spansPerRequest)

	go func() {
		http.Handle("/metrics", promhttp.Handler())
		http.ListenAndServe(":8080", nil)
	}()

	if err := connect.Ping(); err != nil {
			if exception, ok := err.(*clickhouse.Exception); ok {
				fmt.Printf("[%d] %s \n%s\n", exception.Code, exception.Message, exception.StackTrace)
			} else {
				logger.Fatal("Err:"+ err.Error())
			}
			os.Exit(0)
	}


	ze := &clickHouseExporter{
		logger:   logger,
		debug:    false,
		endpoint: endpoint,
		db: connect,
		receiver: make(chan[]pdata.ResourceSpansSlice),
	}
	flush(connect,&ze.receiver,logger)
	return ze, nil
}

func flush(connect *sql.DB, receiver *chan[]pdata.ResourceSpansSlice,logger   *zap.Logger) {

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)


	go func() {
		for {
			select {
			case spans := <- *receiver:

				var (
					tx, _   = connect.Begin()
					stmt, _ = tx.Prepare("INSERT INTO sherlockio.traces (svc_name, command, traceid, spanid, pspanid, status ,start_time, end_time, duration ,attributes) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")
				)


				spansPerRequest.Observe(float64(len(spans)))


				for _,resourceSpans := range spans {
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
								zTags[k] = AttributeValueToString(v, false)
								logger.Info(fmt.Sprintf("----> Key:%s value:%s", k, AttributeValueToString(v, false)))
								if k == "service.name" {
									svcName = AttributeValueToString(v, false)
								}
							})
						}


						tags := make([]string,0)
						for k,v := range zTags {
							tags = append(tags, fmt.Sprintf("%s:%s", k , v ))
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

								traceId := span.TraceID().HexString()[16:]
								spanId := span.SpanID().HexString()
								parentSpanId := span.ParentSpanID().HexString()
								name := span.Name()
								duration := int32(et - st)

								opsProcessed.Inc()
								logger.Info(fmt.Sprintf("svc:%s span name:%v tid:%v sid:%v , psid: %v status:%v startTs:%d endTs:%d", svcName, name, traceId, spanId, parentSpanId, status, st, et))
								if _, err := stmt.Exec(
									svcName,
									name,
									traceId,
									spanId,
									parentSpanId,
									status,
									uint64(st/1000000000),
									uint64(et/1000000000),
									duration,
									clickhouse.Array(tags),
								); err != nil {
									log.Fatal(err)
									stmt.Close()
								}
							}
						}
					}
				}
				stmt.Close()
				if err := tx.Commit(); err != nil {
					stmt.Close()
					log.Fatal(err)
				}

			   case <-time.After(time.Second * publishInterval):

			   case <-sigs:
				fmt.Println("Exiting app")
				os.Exit(1)
				break
			}
		}
	}()


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

func (s *clickHouseExporter) pushTraceData(
	_ context.Context,
	td pdata.Traces,
) (int, error) {

	s.logger.Info("TracesExporter", zap.Int("#spans", td.SpanCount()))

	resourceSpans := td.ResourceSpans()

	send := []pdata.ResourceSpansSlice{resourceSpans}
	s.receiver <-send
	return 0, nil
}
