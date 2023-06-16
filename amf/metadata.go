package amf

import (
	"fmt"
	"strings"
)

type Metadata map[string]any

func (m Metadata) Get(key string) any {
	for k, _ := range m {
		if strings.EqualFold(k, key) {
			return m[k]
		}
	}

	return nil
}

func (m Metadata) GetString(key string) (string, error) {
	result := m.Get(key)

	if result == nil {
		return "", fmt.Errorf("could not find key '%s' in metadata", key)
	}

	str, ok := result.(string)

	if !ok {
		return "", fmt.Errorf("metadata value forkey '%s' is not a string", key)
	}

	return str, nil
}
