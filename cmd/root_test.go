package cmd_test

import (
	"github.com/hashicorp/go-hclog"
	rotatelogs "github.com/lestrrat-go/file-rotatelogs"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/pkg/errors"
	"io"
	"os"
	"time"
)

func CreateLog() (hclog.Logger, error) {
	logf, err := rotatelogs.New(
		"./__test_data__/log.%Y%m%d%H%M%S",
		// rotatelogs.WithLinkName("./__test_data__/log"),
		rotatelogs.WithMaxAge(1*time.Minute),
		rotatelogs.WithRotationTime(time.Second),
	)
	if err != nil {
		return nil, errors.Wrap(err, "log file")
	}

	log := hclog.New(&hclog.LoggerOptions{
		Level:      hclog.Trace,
		Output:     io.MultiWriter(os.Stderr, logf),
		JSONFormat: true,
	})

	return log, nil
}

var _ = Describe("Root", func() {

	It("should be able to start multiple instances together", func() {

		var count = 5
		loggerCh := make(chan hclog.Logger, count)
		errCh := make(chan error, count)
		var loggers []hclog.Logger

		for i := 0; i < count; i++ {
			go func() {
				logger, err := CreateLog()
				if err != nil {
					errCh <- err
				}
				loggerCh <- logger
			}()
		}

		go func() {

			for i := 0; i < count; i++ {
				logger := <-loggerCh
				if logger != nil{

				loggers = append(loggers, logger)
				logger.Info("Message", "index", i)
				}
			}
			<-time.After(100*time.Millisecond)
			close(loggerCh)
			close(errCh)
		}()

		Consistently(errCh).ShouldNot(Receive(HaveOccurred()))
		Eventually(loggerCh).Should(BeClosed())
	})
})
