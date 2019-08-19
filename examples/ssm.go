package main

import (
	"fmt"

	"github.com/alwaysbespoke/aws/ssm"
)

// GetParam ...
func main() {
	key := "/analytics-test"
	value, err := ssm.GetParam(&key, false)
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println(*value)
}
