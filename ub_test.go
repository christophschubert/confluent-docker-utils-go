package main

import (
	"fmt"
	"testing"
)

func assertEqual(a string, b string, t *testing.T) {
	if a != b {
		t.Error(a + " != " + b)
	}
}

func TestContains(t *testing.T) {
	if Contains([]string{"hello", "world"}, "hi") {
		t.Error(` Contains([]string{"hello", "world"}, "hi") = true`)
	}
	if !Contains([]string{"hello", "world"}, "hello") {
		t.Error(`Contains([]string{"hello", "world"}, "hello") = false`)
	}
}

func TestConvertKey(t *testing.T) {
	assertEqual(ConvertKey("KEY"), "key", t)
	assertEqual(ConvertKey("KEY_FOO"), "key.foo", t)
	assertEqual(ConvertKey("KEY__UNDERSCORE"), "key_underscore", t)
	assertEqual(ConvertKey("KEY_WITH__UNDERSCORE_AND__MORE"), "key.with_underscore.and_more", t)
	assertEqual(ConvertKey("KEY___DASH"), "key-dash", t)
	assertEqual(ConvertKey("KEY_WITH___DASH_AND___MORE__UNDERSCORE"), "key.with-dash.and-more_underscore", t)
}

func TestBuildProperties(t *testing.T) {
	var testEnv = map[string]string{
		"PATH":                    "thePath",
		"KAFKA_BOOTSTRAP_SERVERS": "localhost:9092",
		"CONFLUENT_METRICS":       "metricsValue",
		"KAFKA_IGNORED":           "ignored",
	}

	var onlyDefaultsCS = ConfigSpec{
		Prefixes: map[string]bool{},
		Excludes: []string{},
		Renamed:  map[string]string{},
		Defaults: map[string]string{
			"default.property.key": "default.property.value",
			"bootstrap.servers":    "unknown",
		},
	}

	var onlyDefaults = BuildProperties(onlyDefaultsCS, testEnv)
	fmt.Println(onlyDefaults)
	if len(onlyDefaults) != 2 {
		t.Error("Failed to parse defaults.")
	}
	if onlyDefaults["default.property.key"] != "default.property.value" {
		t.Error("default.property.key not parsed correctly")
	}

	var serverCS = ConfigSpec{
		Prefixes: map[string]bool{"KAFKA": false, "CONFLUENT": true},
		Excludes: []string{"KAFKA_IGNORED"},
		Renamed:  map[string]string{},
		Defaults: map[string]string{
			"default.property.key": "default.property.value",
			"bootstrap.servers":    "unknown",
		},
	}
	var serverProps = BuildProperties(serverCS, testEnv)
	if len(serverProps) != 3 {
		t.Error("Server props size != 3")
	}
	if serverProps["bootstrap.servers"] != "localhost:9092" {
		t.Error("Dropped prefixed not parsed correctly")
	}
	if serverProps["confluent.metrics"] != "metricsValue" {
		t.Error("Kept prefix not parsed correctly")
	}

	var kafkaEnv = map[string]string{
		"KAFKA_FOO":                       "foo",
		"KAFKA_FOO_BAR":                   "bar",
		"KAFKA_IGNORED":                   "ignored",
		"KAFKA_WITH__UNDERSCORE":          "with underscore",
		"KAFKA_WITH__UNDERSCORE_AND_MORE": "with underscore and more",
		"KAFKA_WITH___DASH":               "with dash",
		"KAFKA_WITH___DASH_AND_MORE":      "with dash and more",
	}

	var kafkaProperties = BuildProperties(serverCS, kafkaEnv)

	if len(kafkaProperties) != 8 {
		t.Error("Wrong number of properties")
	}
	assertEqual(kafkaProperties["foo"], "foo", t)
	assertEqual(kafkaProperties["foo.bar"], "bar", t)
	assertEqual(kafkaProperties["with_underscore"], "with underscore", t)
	assertEqual(kafkaProperties["with_underscore.and.more"], "with underscore and more", t)
	assertEqual(kafkaProperties["with-dash"], "with dash", t)
	assertEqual(kafkaProperties["with-dash.and.more"], "with dash and more", t)
}

