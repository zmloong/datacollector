// Package builtin does nothing but import all builtin readers to execute their init functions.
package builtin

import (
	_ "datacollector/reader/autofile"
	_ "datacollector/reader/bufreader"
	_ "datacollector/reader/socket"
)
