package kafka

import (
	"context"
	"fmt"
	"sort"

	kafkago "github.com/segmentio/kafka-go"
)

// PartitionLag is how far a consumer group is behind on one partition.
type PartitionLag struct {
	Partition int
	Committed int64 // last offset the group committed
	HighWater int64 // newest offset on the partition
	Lag       int64 // HighWater - Committed
}

// GroupLag reports per-partition and total lag for a consumer group on a topic:
// how many messages have been written but not yet processed. This is the number
// that climbs under a slow consumer and drains when the group scales out.
func GroupLag(ctx context.Context, brokers []string, group, topic string) ([]PartitionLag, int64, error) {
	addr := brokers[0]
	client := &kafkago.Client{Addr: kafkago.TCP(addr)}

	conn, err := kafkago.DialContext(ctx, "tcp", addr)
	if err != nil {
		return nil, 0, fmt.Errorf("kafka: dial %s: %w", addr, err)
	}
	parts, err := conn.ReadPartitions(topic)
	conn.Close()
	if err != nil {
		return nil, 0, fmt.Errorf("kafka: read partitions: %w", err)
	}

	partIDs := make([]int, 0, len(parts))
	offReq := make([]kafkago.OffsetRequest, 0, len(parts))
	for _, p := range parts {
		partIDs = append(partIDs, p.ID)
		offReq = append(offReq, kafkago.OffsetRequest{Partition: p.ID, Timestamp: kafkago.LastOffset})
	}

	// High-water mark (latest offset) per partition.
	lo, err := client.ListOffsets(ctx, &kafkago.ListOffsetsRequest{
		Topics: map[string][]kafkago.OffsetRequest{topic: offReq},
	})
	if err != nil {
		return nil, 0, fmt.Errorf("kafka: list offsets: %w", err)
	}
	highWater := make(map[int]int64, len(parts))
	for _, po := range lo.Topics[topic] {
		highWater[po.Partition] = po.LastOffset
	}

	// Committed offsets for the group.
	of, err := client.OffsetFetch(ctx, &kafkago.OffsetFetchRequest{
		GroupID: group,
		Topics:  map[string][]int{topic: partIDs},
	})
	if err != nil {
		return nil, 0, fmt.Errorf("kafka: offset fetch: %w", err)
	}
	committed := make(map[int]int64, len(parts))
	for _, o := range of.Topics[topic] {
		committed[o.Partition] = o.CommittedOffset
	}

	lags, total := computeLag(partIDs, committed, highWater)
	return lags, total, nil
}

// computeLag turns committed/high-water maps into per-partition lag. A committed
// offset of -1 means the group has not committed there yet, so it counts as 0
// (the whole partition is pending). Kept pure for testing.
func computeLag(partIDs []int, committed, highWater map[int]int64) ([]PartitionLag, int64) {
	sort.Ints(partIDs)
	out := make([]PartitionLag, 0, len(partIDs))
	var total int64
	for _, id := range partIDs {
		c := committed[id]
		if c < 0 {
			c = 0
		}
		h := highWater[id]
		lag := h - c
		if lag < 0 {
			lag = 0
		}
		out = append(out, PartitionLag{Partition: id, Committed: c, HighWater: h, Lag: lag})
		total += lag
	}
	return out, total
}
