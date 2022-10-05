package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/kelseyhightower/envconfig"
	"github.com/sebastiaopamplona/dbsidekick/pkg"
)

type DBSidekickConfig = struct {
	FeatureToggleDBBackup                      bool `split_words:"true"`
	FeatureToggleDBBackupCronIntervalInMinutes int  `split_words:"true"`
}

type op struct {
	FeatureFlag           bool
	CronIntervalInMinutes int
	Operation             func(context.Context)
}

func (o op) Do(ctx context.Context, stopSignals chan os.Signal) {
	if o.FeatureFlag {
		o.Operation(ctx)
		if o.CronIntervalInMinutes != 0 {
			ticker := time.NewTicker(time.Duration(o.CronIntervalInMinutes) * time.Minute)
			go func() {
				for {
					select {
					case <-stopSignals:
						return
					case <-ticker.C:
						o.Operation(ctx)
					}
				}
			}()
		}
	}
}

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var cfg DBSidekickConfig
	if err := envconfig.Process("dbsidekick", &cfg); err != nil {
		log.Fatalln(err)
	}

	stopSignals := make(chan os.Signal, 1)
	signal.Notify(stopSignals, syscall.SIGINT, syscall.SIGTERM)

	ops := []op{
		{
			FeatureFlag:           cfg.FeatureToggleDBBackup,
			CronIntervalInMinutes: cfg.FeatureToggleDBBackupCronIntervalInMinutes,
			Operation:             pkg.DBBackup,
		},
	}

	for _, o := range ops {
		o.Do(ctx, stopSignals)
	}

	<-stopSignals
}
