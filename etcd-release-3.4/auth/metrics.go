package auth

import (
	"github.com/prometheus/client_golang/prometheus"
	"sync"
)

var (
	currentAuthRevision = prometheus.NewGaugeFunc(prometheus.GaugeOpts{
		Namespace: "etcd_debugging",
		Subsystem: "auth",
		Name:      "revision",
		Help:      "The current revision of auth store.",
	},
		func() float64 {
			reportCurrentAuthRevMu.RLock()
			defer reportCurrentAuthRevMu.RUnlock()
			return reportCurrentAuthRev()
		},
	)
	// overridden by auth store initialization
	reportCurrentAuthRevMu sync.RWMutex
	reportCurrentAuthRev   = func() float64 { return 0 }
)

func init() {
	prometheus.MustRegister(currentAuthRevision)
}
