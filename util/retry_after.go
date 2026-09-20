package util

import (
	"errors"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// RetryAfter extracts a future HTTP retry deadline from structured error details.
func RetryAfter(err error, now time.Time) time.Time {
	var detailed interface{ DeepSearch(string) interface{} }
	if !errors.As(err, &detailed) {
		return time.Time{}
	}
	var value string
	switch headers := detailed.DeepSearch("headers").(type) {
	case http.Header:
		value = headers.Get("Retry-After")
	case map[string]interface{}:
		for key, v := range headers {
			if strings.EqualFold(key, "Retry-After") {
				value, _ = v.(string)
				break
			}
		}
	case map[string]string:
		for key, v := range headers {
			if strings.EqualFold(key, "Retry-After") {
				value = v
				break
			}
		}
	}
	if seconds, err := strconv.ParseInt(strings.TrimSpace(value), 10, 64); err == nil {
		if seconds <= 0 || seconds > (1<<63-1-now.UnixNano())/int64(time.Second) {
			return time.Time{}
		}
		return now.Add(time.Duration(seconds) * time.Second)
	}
	until, err := http.ParseTime(value)
	if err != nil || !until.After(now) || until.Year() >= 2262 {
		return time.Time{}
	}
	return until
}
