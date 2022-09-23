package main

import (
	"fmt"

	"go.uber.org/zap"
)

func main() {
	logger, _ := zap.NewProduction()
	logger.Warn("warning test")
	fmt.Println(logger)
}
