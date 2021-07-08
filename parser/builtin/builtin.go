// Package builtin does nothing but import all builtin parsers to execute their init functions.
package builtin

import (
	_ "datacollector/parser/csv"
	_ "datacollector/parser/raw"
)
