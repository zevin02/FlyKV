package common

import "fmt"

func NewWrongNumberofArry(cmd string) error {
	return fmt.Errorf("ERR wrong number of argument for '%s' command", cmd)
}
