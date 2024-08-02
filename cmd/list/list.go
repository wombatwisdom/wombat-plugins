package main

import (
	"github.com/redpanda-data/benthos/v4/public/service"
	"log"
	"os"
	"plugin"
)

func main() {
	for _, arg := range os.Args[1:] {
		_, err := plugin.Open(arg)
		if err != nil {
			log.Fatalf("Failed to open plugin %s: %v", arg, err)
		}
	}

	var inputs []string
	var outputs []string
	service.GlobalEnvironment().WalkInputs(func(name string, config *service.ConfigView) {
		inputs = append(inputs, name)
	})

	service.GlobalEnvironment().WalkOutputs(func(name string, config *service.ConfigView) {
		outputs = append(outputs, name)
	})

	log.Printf("Inputs: %v\n", inputs)
	log.Printf("Outputs: %v\n", outputs)
}
