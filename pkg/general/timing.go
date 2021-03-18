package general

import (
	"log"
	"time"
)

// use by putting something like the following into the start of a function:
// defer general.TimeTrack(time.Now(), "functionName")
func TimeTrack(start time.Time, name string) {
	elapsed := time.Since(start)
	log.Printf("%s took %s", name, elapsed)
}
