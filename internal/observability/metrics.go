package observability

import (
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

const latencyBucketCount = 11

var latencyBuckets = [latencyBucketCount]float64{0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10}

type requestKey struct {
	method string
	route  string
	status int
}

type taskKey struct {
	task   string
	result string
}

type histogram struct {
	count   uint64
	sum     float64
	buckets [latencyBucketCount]uint64
}

type Metrics struct {
	mu       sync.Mutex
	requests map[requestKey]uint64
	latency  map[string]*histogram
	tasks    map[taskKey]uint64
}

func NewMetrics() *Metrics {
	return &Metrics{
		requests: make(map[requestKey]uint64),
		latency:  make(map[string]*histogram),
		tasks:    make(map[taskKey]uint64),
	}
}

func (m *Metrics) ObserveRequest(method, route string, status int, duration time.Duration) {
	if route == "" {
		route = "unmatched"
	}
	switch method {
	case http.MethodGet, http.MethodPost, http.MethodPut, http.MethodPatch, http.MethodDelete,
		http.MethodHead, http.MethodOptions:
	default:
		method = "OTHER"
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.requests[requestKey{method: method, route: route, status: status}]++
	key := method + "\x00" + route
	value := m.latency[key]
	if value == nil {
		value = &histogram{}
		m.latency[key] = value
	}
	seconds := duration.Seconds()
	value.count++
	value.sum += seconds
	for index, bound := range latencyBuckets {
		if seconds <= bound {
			value.buckets[index]++
		}
	}
}

func (m *Metrics) ObserveTask(task, result string) {
	m.mu.Lock()
	m.tasks[taskKey{task: task, result: result}]++
	m.mu.Unlock()
}

func (m *Metrics) Handler(w http.ResponseWriter, _ *http.Request) {
	m.mu.Lock()
	requests := make(map[requestKey]uint64, len(m.requests))
	for key, count := range m.requests {
		requests[key] = count
	}
	latencies := make(map[string]histogram, len(m.latency))
	for key, value := range m.latency {
		latencies[key] = *value
	}
	tasks := make(map[taskKey]uint64, len(m.tasks))
	for key, count := range m.tasks {
		tasks[key] = count
	}
	m.mu.Unlock()
	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	write := func(format string, values ...any) {
		_, _ = fmt.Fprintf(w, format, values...)
	}
	write("# HELP roote_http_requests_total HTTP requests handled by the API.\n")
	write("# TYPE roote_http_requests_total counter\n")
	requestKeys := make([]requestKey, 0, len(requests))
	for key := range requests {
		requestKeys = append(requestKeys, key)
	}
	sort.Slice(requestKeys, func(i, j int) bool {
		left, right := requestKeys[i], requestKeys[j]
		return left.method+left.route+strconv.Itoa(left.status) <
			right.method+right.route+strconv.Itoa(right.status)
	})
	for _, key := range requestKeys {
		write("roote_http_requests_total{method=%q,route=%q,status=%q} %d\n",
			key.method, key.route, strconv.Itoa(key.status), requests[key])
	}

	write("# HELP roote_http_request_duration_seconds HTTP request duration.\n")
	write("# TYPE roote_http_request_duration_seconds histogram\n")
	latencyKeys := make([]string, 0, len(latencies))
	for key := range latencies {
		latencyKeys = append(latencyKeys, key)
	}
	sort.Strings(latencyKeys)
	for _, key := range latencyKeys {
		parts := strings.SplitN(key, "\x00", 2)
		value := latencies[key]
		for index, bound := range latencyBuckets {
			write("roote_http_request_duration_seconds_bucket{method=%q,route=%q,le=%q} %d\n",
				parts[0], parts[1], strconv.FormatFloat(bound, 'f', -1, 64), value.buckets[index])
		}
		write("roote_http_request_duration_seconds_bucket{method=%q,route=%q,le=\"+Inf\"} %d\n",
			parts[0], parts[1], value.count)
		write("roote_http_request_duration_seconds_sum{method=%q,route=%q} %g\n",
			parts[0], parts[1], value.sum)
		write("roote_http_request_duration_seconds_count{method=%q,route=%q} %d\n",
			parts[0], parts[1], value.count)
	}

	write("# HELP roote_worker_tasks_total Asynq tasks completed by task type and result.\n")
	write("# TYPE roote_worker_tasks_total counter\n")
	taskKeys := make([]taskKey, 0, len(tasks))
	for key := range tasks {
		taskKeys = append(taskKeys, key)
	}
	sort.Slice(taskKeys, func(i, j int) bool {
		return taskKeys[i].task+taskKeys[i].result < taskKeys[j].task+taskKeys[j].result
	})
	for _, key := range taskKeys {
		write("roote_worker_tasks_total{task=%q,result=%q} %d\n",
			key.task, key.result, tasks[key])
	}
}
