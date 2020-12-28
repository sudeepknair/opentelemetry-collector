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
	"github.com/elastic/go-elasticsearch/v8"
	"github.com/elastic/go-elasticsearch/v8/esapi"
	"go.uber.org/zap"
	"log"
	"strconv"
	"strings"
)


type elasticHandle struct {
	endpoint string
  client *elasticsearch.Client
  logger   *zap.Logger
}


func (h *elasticHandle) initialize(endpoint string,logger   *zap.Logger) {
	h.endpoint = endpoint
	h.logger=logger

	if endpoint != "" {
		cfg := elasticsearch.Config{
			Addresses: []string{
				fmt.Sprintf("http://%s",endpoint),
			},
		}
		es, err := elasticsearch.NewClient(cfg)
		if err != nil {
			log.Fatalf("error:%s \r\n", err.Error())
		}
		h.client = es
	} else {
		es, err := elasticsearch.NewDefaultClient()
		if err != nil {
			log.Fatalf("error:%s \r\n", err.Error())
		}
		h.client = es
	}

	h.logger.Info(fmt.Sprintf("Elastic version: %v",elasticsearch.Version))
	p,_ := h.client.Info()
	h.logger.Info(fmt.Sprintf("Elastic info: %v",p))

}

func (h* elasticHandle) WriteData(spanId, parentId, traceId, status,service,operation string,st ,et uint64) {
	// Build the request body.
	var b strings.Builder
	b.WriteString(`{"spanId" : "`)
	b.WriteString(spanId)
	b.WriteString("\",")
	b.WriteString(`"parentId" : "`)
	b.WriteString(parentId)
	b.WriteString("\",")
	b.WriteString(`"traceId" : "`)
	b.WriteString(traceId)
	b.WriteString("\",")
	b.WriteString(`"operation" : "`)
	b.WriteString(operation)
	b.WriteString("\",")
	b.WriteString(`"status" : "`)
	b.WriteString(status)
	b.WriteString("\",")
	b.WriteString(`"service" : "`)
	b.WriteString(service)
	b.WriteString("\",")
	b.WriteString(`"startTime" : `)
	b.WriteString(strconv.FormatUint(st, 10))
	b.WriteString(",")
	b.WriteString(`"endTime" : `)
	b.WriteString(strconv.FormatUint(et, 10))
	b.WriteString(`}`)

	h.logger.Info("payload"+ b.String())

	// Set up the request object.
	req := esapi.IndexRequest{
		Index:      "tracing",
		Body:       strings.NewReader(b.String()),
		Refresh:    "true",
	}

	// Perform the request with the client.
	res, err := req.Do(context.Background(), h.client)
	if err != nil {
		log.Fatalf("Error getting response: %s", err)
	}
	defer res.Body.Close()
}