package logger

import (
	log "github.com/sirupsen/logrus"
)

var Log *log.Logger

func Init(level string) {
	Log = log.New()

	lvl, err := log.ParseLevel(level)
	if err != nil {
		lvl = log.InfoLevel
	}
	Log.SetLevel(lvl)

	Log.SetFormatter(&log.TextFormatter{
		FullTimestamp: true,
	})
}
