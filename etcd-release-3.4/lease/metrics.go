package lease

import (
	"github.com/prometheus/client_golang/prometheus"
)

var (
	leaseGranted = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "etcd_debugging",
		Subsystem: "lease",
		Name:      "granted_total",
		Help:      "The total number of granted leases.",
	})

	leaseRevoked = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "etcd_debugging",
		Subsystem: "lease",
		Name:      "revoked_total",
		Help:      "The total number of revoked leases.",
	})

	leaseRenewed = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "etcd_debugging",
		Subsystem: "lease",
		Name:      "renewed_total",
		Help:      "The number of renewed leases seen by the leader.",
	})

	leaseTotalTTLs = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "etcd_debugging",
			Subsystem: "lease",
			Name:      "ttl_total",
			Help:      "Bucketed histogram of lease TTLs.",
			// 1 second -> 3 months
			Buckets: prometheus.ExponentialBuckets(1, 2, 24),
		})
)

func init() {
	prometheus.MustRegister(leaseGranted)
	prometheus.MustRegister(leaseRevoked)
	prometheus.MustRegister(leaseRenewed)
	prometheus.MustRegister(leaseTotalTTLs)
}
