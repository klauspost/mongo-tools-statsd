package output

import (
	"fmt"
	"github.com/mongodb/mongo-tools/mongotop/command"
	"github.com/peterbourgon/g2s"
	"time"
)

// StatsdOutputter is an Outputter sends top information to a statsd server.
// Times are normalized to milliseconds per second
type StatsdOutputter struct {
	Statter    g2s.Statter
	Multiplier float32
	Prefix     string
}

type statOutput struct {
	name  string
	value float32
}

func (self *StatsdOutputter) Output(diff command.Diff) error {

	tableRows := diff.ToRows()

	if len(tableRows) < 2 {
		return fmt.Errorf("outputter: not enough data recieved")
	}

	// First row is the name, ignore that.
	output := make([]statOutput, len(tableRows[1])-1)

	for ri, row := range tableRows {
		if ri > 0 {
			outputAny := false

			for idx, rowEl := range row {
				if idx > 0 {
					collection := row[0]
					if collection == "" {
						collection = "(main)"
					}
					name := fmt.Sprintf("%s.%s", collection, tableRows[0][idx])
					var time int
					// We rely on the formatting of the "ToRows" functions.
					n, err := fmt.Sscanf(rowEl, "%dms", &time)
					if n == 1 && err == nil {
						// We only add data when time is >1ms.
						if time > 0 {
							outputAny = true
						}
						output[idx-1] = statOutput{name: name, value: float32(time) * self.Multiplier}
					} else {
						fmt.Printf("Error parsing %s\n", rowEl)
						output[idx-1] = statOutput{}
					}
				}
			}
			if outputAny {
				for _, val := range output {
					if val.name != "" {
						self.Statter.Timing(1.0, self.Prefix+val.name, time.Duration(float32(time.Millisecond)*val.value))
						//fmt.Printf("Measured:%s:%f\n", val.name, val.value)
					}
				}
			}
		}
	}
	return nil
}
