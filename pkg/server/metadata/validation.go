package metadata

import (
	"fmt"
	"regexp"
	"strings"
	"unicode/utf8"
)

const (
	// MaxLabelKeyLength is the maximum allowed length for a metadata label key (following Kubernetes conventions)
	MaxLabelKeyLength = 63
	// MaxLabelValueLength is the maximum allowed length for a metadata label value
	MaxLabelValueLength = 253
	// MaxMetadataSize is the maximum total size for all metadata in bytes
	MaxMetadataSize = 32 * 1024 // 32KB
	// MaxLabelsCount is the maximum number of labels allowed
	MaxLabelsCount = 64
)

var (
	// labelKeyRegex defines the valid format for label keys
	// Must be a valid DNS-1123 label format (lowercase alphanumeric, dashes, dots)
	// Cannot have consecutive dots or dashes, must start/end with alphanumeric
	labelKeyRegex = regexp.MustCompile(`^[a-z0-9]([a-z0-9\-]*[.]?[a-z0-9\-]*)*[a-z0-9]$|^[a-z0-9]$`)
	
	// reservedKeyPrefixes are key prefixes reserved for system use
	reservedKeyPrefixes = []string{
		"openfga.dev/",
		"kubernetes.io/",
		"k8s.io/",
	}
)

// ValidateMetadata validates a metadata map according to OpenFGA metadata rules
func ValidateMetadata(metadata map[string]string) error {
	if metadata == nil {
		return nil
	}

	// Check maximum number of labels
	if len(metadata) > MaxLabelsCount {
		return fmt.Errorf("metadata cannot have more than %d labels, got %d", MaxLabelsCount, len(metadata))
	}

	// Calculate total size
	totalSize := 0
	for key, value := range metadata {
		totalSize += utf8.RuneCountInString(key) + utf8.RuneCountInString(value)
	}
	
	if totalSize > MaxMetadataSize {
		return fmt.Errorf("metadata total size cannot exceed %d bytes, got %d", MaxMetadataSize, totalSize)
	}

	// Validate each key-value pair
	for key, value := range metadata {
		if err := validateLabelKey(key); err != nil {
			return fmt.Errorf("invalid metadata key '%s': %w", key, err)
		}
		
		if err := validateLabelValue(value); err != nil {
			return fmt.Errorf("invalid metadata value for key '%s': %w", key, err)
		}
	}

	return nil
}

// validateLabelKey validates a single label key
func validateLabelKey(key string) error {
	if key == "" {
		return fmt.Errorf("key cannot be empty")
	}
	
	if utf8.RuneCountInString(key) > MaxLabelKeyLength {
		return fmt.Errorf("key length cannot exceed %d characters, got %d", MaxLabelKeyLength, utf8.RuneCountInString(key))
	}
	
	// Check for reserved prefixes first
	for _, prefix := range reservedKeyPrefixes {
		if strings.HasPrefix(key, prefix) {
			return fmt.Errorf("key cannot use reserved prefix '%s'", prefix)
		}
	}
	
	// Check for consecutive dots or dashes
	if strings.Contains(key, "..") || strings.Contains(key, "--") {
		return fmt.Errorf("key cannot contain consecutive dots or dashes")
	}
	
	// Check for uppercase letters
	if key != strings.ToLower(key) {
		return fmt.Errorf("key must be lowercase")
	}
	
	// Validate format using simplified rules
	// Must start and end with alphanumeric, can contain dashes and dots in between
	if len(key) == 1 {
		if !regexp.MustCompile(`^[a-z0-9]$`).MatchString(key) {
			return fmt.Errorf("key must be alphanumeric")
		}
	} else {
		if !regexp.MustCompile(`^[a-z0-9]`).MatchString(key) {
			return fmt.Errorf("key must start with alphanumeric character")
		}
		if !regexp.MustCompile(`[a-z0-9]$`).MatchString(key) {
			return fmt.Errorf("key must end with alphanumeric character")
		}
		if !regexp.MustCompile(`^[a-z0-9\-\.]+$`).MatchString(key) {
			return fmt.Errorf("key must contain only lowercase alphanumeric characters, dashes, and dots")
		}
	}
	
	return nil
}

// validateLabelValue validates a single label value
func validateLabelValue(value string) error {
	if utf8.RuneCountInString(value) > MaxLabelValueLength {
		return fmt.Errorf("value length cannot exceed %d characters, got %d", MaxLabelValueLength, utf8.RuneCountInString(value))
	}
	
	// Values can be empty and have more flexible format requirements than keys
	// We allow most UTF-8 characters but restrict control characters
	for _, r := range value {
		if r < 32 && r != 9 && r != 10 && r != 13 { // Allow tab, newline, carriage return
			return fmt.Errorf("value contains invalid control character")
		}
	}
	
	return nil
}

// NormalizeMetadata normalizes metadata by trimming whitespace and removing empty values
func NormalizeMetadata(metadata map[string]string) map[string]string {
	if metadata == nil {
		return nil
	}
	
	normalized := make(map[string]string, len(metadata))
	for key, value := range metadata {
		key = strings.TrimSpace(key)
		value = strings.TrimSpace(value)
		
		if key != "" { // Skip empty keys
			normalized[key] = value
		}
	}
	
	if len(normalized) == 0 {
		return nil
	}
	
	return normalized
}
