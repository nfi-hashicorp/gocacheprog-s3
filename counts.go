package main

import (
	"encoding/csv"
	"fmt"
	"io"
	"strconv"
	"sync/atomic"
	"time"

	uberatomic "go.uber.org/atomic"
)

// Counts keeps counts cache events
type Counts struct {
	gets          atomic.Int64
	hits          atomic.Int64
	misses        atomic.Int64
	puts          atomic.Int64
	getErrors     atomic.Int64
	putErrors     atomic.Int64
	totalGetBytes atomic.Int64
	totalGetDur   uberatomic.Duration
	totalPutBytes atomic.Int64
	totalPutDur   uberatomic.Duration
}

func (c *Counts) Summary() string {
	getsLine := fmt.Sprintf("%d gets: %d hits, %d misses, %d errors, %s total dur",
		c.gets.Load(), c.hits.Load(), c.misses.Load(), c.getErrors.Load(), c.totalGetDur.Load().Round(100*time.Millisecond))
	if c.totalGetBytes.Load() > 0 {
		getsLine += fmt.Sprintf("; total %.2f MB; avg %.2f MB/s",
			float64(c.totalGetBytes.Load())/1_000_000.0, float64(c.totalGetBytes.Load())/1_000_000.0/c.totalGetDur.Load().Seconds())
	}
	putsLine := fmt.Sprintf("%d puts: %d errors, %s total dur",
		c.puts.Load(), c.putErrors.Load(), c.totalPutDur.Load().Round(100*time.Millisecond))
	if c.totalPutBytes.Load() > 0 {
		putsLine += fmt.Sprintf("; total %.2f MB; avg %.2f MB/s",
			float64(c.totalPutBytes.Load())/1_000_000.0, float64(c.totalPutBytes.Load())/1_000_000.0/c.totalPutDur.Load().Seconds())
	}
	return fmt.Sprintf("%s\n%s", getsLine, putsLine)
}

// TODO: maybe there's a way to do this in stdlib, but I couldn't find it
// this should give us a timestamp that at the very least Google Sheets supports,
// like
func csvDuration(d time.Duration) string {
	return time.Unix(0, 0).UTC().Add(d).Format("15:04:05.000")
}

func (c *Counts) CSV(f io.Writer, header bool) error {
	w := csv.NewWriter(f)
	if header {
		err := w.Write([]string{"gets", "hits", "misses", "puts", "getErrors", "putErrors", "totalGetBytes", "totalGetDur", "totalPutBytes", "totalPutDur"})
		if err != nil {
			return err
		}
	}
	err := w.Write([]string{
		strconv.Itoa(int(c.gets.Load())),
		strconv.Itoa(int(c.hits.Load())),
		strconv.Itoa(int(c.misses.Load())),
		strconv.Itoa(int(c.puts.Load())),
		strconv.Itoa(int(c.getErrors.Load())),
		strconv.Itoa(int(c.putErrors.Load())),
		strconv.Itoa(int(c.totalGetBytes.Load())),
		csvDuration(c.totalGetDur.Load()),
		strconv.Itoa(int(c.totalPutBytes.Load())),
		csvDuration(c.totalPutDur.Load()),
	})
	if err != nil {
		return err
	}
	w.Flush()
	return w.Error()
}
