package redissub

import (
	"context"
	"fmt"
	red "github.com/go-redis/redis/v8"
	jsoniter "github.com/json-iterator/go"
	"strconv"
	"time"
)

const (
	offlinePrefix = "redissub:offline:zset:%v"
)

type (
	OffLine struct {
		ExpireTime time.Duration // Key ttl
		Rdb        *red.Client
		Key        string
	}
)

func (o *OffLine) AddToOffline(ctx context.Context, data []byte) {
	var event Event
	err := jsoniter.Unmarshal(data, &event)

	if err == nil {
		o.Rdb.ZAdd(ctx, o.Key, &red.Z{
			Score:  float64(event.Time),
			Member: string(data),
		})
		o.Rdb.Expire(ctx, o.Key, o.ExpireTime)
	}

}

func (o *OffLine) MessageByOffset(ctx context.Context, offset int64) ([]string, error) {
	max := time.Now().UnixMilli()
	result, err := o.Rdb.ZRangeByScore(ctx, o.Key, &red.ZRangeBy{
		Min:    strconv.Itoa(int(offset)),
		Max:    strconv.Itoa(int(max)),
		Offset: 0,
		Count:  1<<63 - 1,
	}).Result()
	if err != nil {
		return nil, err
	}

	fmt.Println("MessageByOffset", result)

	return result, nil
}

func (o *OffLine) PullOffLine(ctx context.Context, online *Online) {
	offset := online.Offset.Offset(ctx)
	datas, err := o.MessageByOffset(ctx, offset)
	if err != nil {
		return
	}
	if datas != nil && len(datas) > 0 {
		for _, item := range datas {
			event := []byte(item)
			if !online.Receiver.IsReceived(ctx, event) {
				online.Waiter.Push(ctx, event)
			}
		}
	}
}

func GenOfflineKey(channel string) string {
	return fmt.Sprintf(offlinePrefix, channel)
}
