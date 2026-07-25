package queue

import "testing"


func TestBuildAlertConfig(t *testing.T) {
	defaults := map[string]AlertConfig{
		"topic-a": {LagThreshold: 10, FetchErrorThreshold: 2}, 
	}
	overrides := map[ConsumerKey]AlertConfig{
		{Topic: "topic-a", GroupID: "group-1"}: {LagThreshold: 999},
	}
	consumers := []*Consumer{
		{topic: "topic-a", groupID: "group-1"}, 
		{topic: "topic-a", groupID: "group-2"}, 
		{topic: "topic-unknown", groupID: "group-3"},
	}

	tests := []struct {
		name string
		key ConsumerKey 
		want AlertConfig
	}{
		{"override default", ConsumerKey{"topic-a", "group-1"}, AlertConfig{LagThreshold: 999}}, 
		{"using default when there is no override", ConsumerKey{"topic-a", "group-2"}, defaults["topic-a"]}, 
		{"unknown topic use fallback", ConsumerKey{"topic-unknown", "group-3"}, fallbackAlertConfig}, 
	}

	result := BuildAlertConfigs(consumers, defaults, overrides)

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, ok := result[tt.key]
			if !ok {
				t.Fatalf("key %v does not exist in result", tt.key)
			}
			if got != tt.want {
				t.Errorf("got %+v, want %+v", got, tt.want)
			}
		})
	}
}