package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go/logging"
	"github.com/nfi-hashicorp/gocacheprog-s3/go-tool-cache/cacheproc"

	"github.com/aws/aws-sdk-go-v2/config"
)

const defaultS3Prefix = "go-cache"

var userCacheDir, _ = os.UserCacheDir()
var defaultLocalCacheDir = filepath.Join(userCacheDir, "go-cacher")

var (
	flagVerbose       = flag.Int("v", 0, "logging verbosity; 0=error, 1=warn, 2=info, 3=debug, 4=trace")
	flagS3Prefix      = flag.String("s3-prefix", defaultS3Prefix, "s3 prefix")
	flagLocalCacheDir = flag.String("local-cache-dir", defaultLocalCacheDir, "local cache directory")
	bucket            string
	flagQueueLen      = flag.Int("queue-len", 0, "length of the queue for async s3 cache (0=synchronous)")
	flagWorkers       = flag.Int("workers", 1, "number of workers for async s3 cache (1=synchronous)")
	flagMetCSV        = flag.String("metrics-csv", "", "write s3 Get/Put metrics to a CSV file (empty=disabled)")
	flagBucket        = flag.String("bucket", "", "s3 bucket to use (empty=use $GOCACHEPROGS3_BUCKET)")
)

// logHandler implements slog.Handler to print logs nicely
// mostly this was an exercise to use slog, probably not the best choice here TBH
type logHandler struct {
	Out    io.Writer
	Level  slog.Level
	attrs  []slog.Attr
	groups []string
}

func (h *logHandler) Enabled(_ context.Context, l slog.Level) bool {
	return l >= h.Level
}

func (h *logHandler) Handle(_ context.Context, r slog.Record) error {
	s := r.Level.String()[:1]
	if len(h.groups) > 0 {
		s += " " + strings.Join(h.groups, ".") + ":"
	}
	s += " " + r.Message
	attrs := h.attrs
	r.Attrs(func(a slog.Attr) bool {
		attrs = append(attrs, a)
		return true
	})
	for i, a := range attrs {
		if i == 0 {
			s += " {"
		}
		s += fmt.Sprintf("%s=%q", a.Key, a.Value)
		if i < len(attrs)-1 {
			s += " "
		} else {
			s += "}"
		}
	}
	s += "\n"
	_, err := h.Out.Write([]byte(s))
	return err
}

func (h *logHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return &logHandler{
		Out:    h.Out,
		Level:  h.Level,
		attrs:  append(h.attrs, attrs...),
		groups: h.groups,
	}
}

func (h *logHandler) WithGroup(name string) slog.Handler {
	if h.groups == nil {
		h.groups = []string{}
	}
	return &logHandler{
		Out:    h.Out,
		Level:  h.Level,
		attrs:  h.attrs,
		groups: append(h.groups, name),
	}
}

// Logf allows us to also implement AWS's logging.Logger
func (h *logHandler) Logf(cls logging.Classification, format string, args ...interface{}) {
	var l slog.Level
	switch cls {
	case logging.Debug:
		l = slog.LevelDebug
	case logging.Warn:
		l = slog.LevelWarn
	default:
		l = slog.LevelDebug
	}

	h.Handle(context.Background(), slog.Record{
		Level:   l,
		Message: fmt.Sprintf(format, args...),
	})
}

var levelTrace = slog.Level(slog.LevelDebug - 4)

func main() {
	flag.Parse()
	bucket = *flagBucket
	if bucket == "" {
		bucket = os.Getenv("GOCACHEPROGS3_BUCKET")
		if bucket == "" {
			log.Fatal("neither --bucket nor GOCACHEPROGS3_BUCKET environment variable set")
		}
	}
	logLevel := slog.Level(*flagVerbose*-4 + 8)
	h := &logHandler{
		Level: logLevel,
		Out:   os.Stderr,
	}

	slog.SetDefault(slog.New(h))

	slog.Debug(fmt.Sprintf("Log level: %s", logLevel))
	slog.Debug("starting cache")
	var clientLogMode aws.ClientLogMode
	if logLevel <= levelTrace {
		clientLogMode = aws.LogRetries | aws.LogRequest
	}
	awsConfig, err := config.LoadDefaultConfig(context.TODO(), config.WithClientLogMode(clientLogMode), config.WithLogger(h))
	if err != nil {
		log.Fatal("S3 cache disabled; failed to load AWS config: ", err)
	}
	diskCacher := NewDiskCache(*flagLocalCacheDir)
	cacher := NewDiskAsyncS3Cache(
		diskCacher,
		s3.NewFromConfig(awsConfig),
		bucket,
		*flagS3Prefix,
		*flagQueueLen,
		*flagWorkers,
	)
	// TODO: not too sure we need this context
	startCtx, cancel := context.WithCancel(context.Background())
	defer cancel()

	start := time.Now()
	err = cacher.Start(startCtx)
	if err != nil {
		log.Fatalf("failed to start cache: %v", err)
	}
	proc := cacheproc.Process{
		Get:   cacher.Get,
		Put:   cacher.Put,
		Close: cacher.Close,
	}
	if err := proc.Run(); err != nil {
		slog.Error(err.Error())
		os.Exit(1)
	}
	if logLevel <= slog.LevelInfo {
		fmt.Fprintln(os.Stderr, "disk stats: \n"+diskCacher.Counts.Summary())
		fmt.Fprintln(os.Stderr, "s3 stats: \n"+cacher.Counts.Summary())
		fmt.Fprintln(os.Stderr, "total time: ", time.Since(start).Round(time.Second))
	}
	if *flagMetCSV != "" {
		f, err := os.Create(*flagMetCSV)
		if err != nil {
			slog.Error(fmt.Sprintf("failed to create metrics file: %v", err))
		} else {
			defer f.Close()
			cacher.Counts.CSV(f, true)
		}
	}
}
