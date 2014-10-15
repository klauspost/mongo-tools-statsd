package mongostat

import (
	"fmt"
	"github.com/peterbourgon/g2s"
)

// sendCount will send a count value to the statter.
// This is a convenience wrapper.
// Only counts > 0  are sent.
func sendCount(s g2s.Statter, name string, count int64) {
	if count > 0 {
		//fmt.Printf("Count %s:%v\n", name, count)
		s.Counter(1.0, name, int(count))
	}
}

// sendGauge will send a gauge value to the statter.
// All gauge values are sent.
func sendGauge(s g2s.Statter, name string, value int64) {
	//fmt.Printf("Gauge %s:%v\n", name, value)
	s.Gauge(1.0, name, fmt.Sprintf("%v", value))
}

func SendLines(lines []StatLine, s g2s.Statter, prefix string, discover bool) {
	all := false
	// if any of the nodes being monitored are part of a replset,
	// enable the printing of replset-specific columns
	for _, line := range lines {
		if line.NonMapped >= 0 {
			all = true
		}
	}
	for _, line := range lines {
		base := prefix
		if discover {
			base += line.Host + "."
		}
		sendCount(s, base+"ops.insert", line.Insert)
		sendCount(s, base+"ops.query", line.Query)
		sendCount(s, base+"ops.update", line.Update)
		sendCount(s, base+"ops.delete", line.Delete)
		sendCount(s, base+"ops.getmore", line.GetMore)
		sendCount(s, base+"ops.command", line.Command)
		sendCount(s, base+"ops.command.r", line.CommandR)
		sendCount(s, base+"flushes", line.Flushes)
		sendCount(s, base+"faults", line.Faults)

		sendGauge(s, base+"virtual.megs", line.Virtual)
		sendGauge(s, base+"resident.megs", line.Resident)
		if line.Mapped > 0 {
			sendGauge(s, base+"mapped.megs", line.Mapped)
		}
		if all && line.NonMapped >= 0 {
			sendGauge(s, base+"nonmapped.megs", line.NonMapped)
		}
		s.Gauge(1.0, base+"index.miss.percent", fmt.Sprintf("%v", line.IndexMissPercent))
		sendGauge(s, base+"queued.readers", line.QueuedReaders)
		sendGauge(s, base+"queued.writers", line.QueuedWriters)
		sendGauge(s, base+"active.readers", line.ActiveReaders)
		sendGauge(s, base+"active.writers", line.ActiveWriters)
		sendCount(s, base+"net.in", line.NetIn)
		sendCount(s, base+"net.out", line.NetOut)
		sendGauge(s, base+"num.connections", line.NumConnections)
	}
}
