package queue

import (
	"slices"
	"testing"
)

func TestQueuesIsolateWorkClassesAndDrainLegacyQueue(t *testing.T) {
	queues := Queues("notifications")
	want := map[string]int{
		"notifications":               1,
		"notifications-transactional": 6,
		"notifications-webhooks":      2,
		"notifications-marketing":     2,
		"notifications-system":        1,
	}
	if len(queues) != len(want) {
		t.Fatalf("queues = %v, want %v", queues, want)
	}
	for name, weight := range want {
		if queues[name] != weight {
			t.Errorf("queue %s weight = %d, want %d", name, queues[name], weight)
		}
	}
	names := QueueNames("notifications")
	if names[0] != "notifications-transactional" || !slices.Contains(names, "notifications") {
		t.Fatalf("queue names = %v", names)
	}
}

func TestNotificationQueueFollowsTrafficClass(t *testing.T) {
	for trafficClass, want := range map[string]string{
		"":              "notifications-transactional",
		"transactional": "notifications-transactional",
		"marketing":     "notifications-marketing",
	} {
		if got := notificationQueue("notifications", trafficClass); got != want {
			t.Errorf("notificationQueue(%q) = %q, want %q", trafficClass, got, want)
		}
	}
}
