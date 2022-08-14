package main

import (
	"context"
	"log"

	"github.com/kelseyhightower/envconfig"
	"github.com/sebastiaopamplona/dbsidekick/pkg"
)

type DBSidekickConfig = struct {
	FeatureToggleDBBackup bool `split_words:"true"`
}

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var cfg DBSidekickConfig
	if err := envconfig.Process("dbsidekick", &cfg); err != nil {
		log.Fatalln(err)
	}

	if cfg.FeatureToggleDBBackup {
		pkg.DBBackup(ctx)
	}
}
