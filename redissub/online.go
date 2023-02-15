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
	waiterPrefix   = "redissub:online:waiter:hash:%v:%v"
	receiverPrefix = "redissub:online:receiver:hash:%v:%v"
	offsetPrefix   = "redissub:online:offset:%v:%v"
)

type (
	Online struct {
		Offset   *Offset
		Waiter   *Waiter
		Receiver *Receiver
	}

	Offset struct {
		Key        string
		Rdb        *red.Client
		ExpireTime time.Duration // Key ttl
	}

	Waiter struct {
		Key        string
		Rdb        *red.Client
		ExpireTime time.Duration // Key ttl
	}

	Receiver struct {
		Key        string
		Rdb        *red.Client
		ExpireTime time.Duration // Key ttl
	}
)

func (r *Online) Ack(ctx context.Context, data *Event) {
	r.Waiter.Del(ctx, data)
	r.Receiver.Received(ctx, data)
	r.Offset.UpdateOffset(ctx, data)
}

func (w *Waiter) Push(ctx context.Context, data []byte) {
	var event Event
	err := jsoniter.Unmarshal(data, &event)
	if err != nil {
		return
	}
	key := w.Key
	field := event.Id
	value := string(data)
	w.Rdb.HSet(ctx, key, field, value).Err()
	w.Rdb.Expire(ctx, w.Key, w.ExpireTime)
}

func (w *Waiter) All(ctx context.Context) []interface{} {
	strings := []interface{}{}
	result, err := w.Rdb.HGetAll(ctx, w.Key).Result()
	if err != nil {
		return strings
	}
	for _, item := range result {
		strings = append(strings, item)
	}

	Sort(strings, func(a, b interface{}) int {
		var s1 Event
		var s2 Event
		jsoniter.Unmarshal([]byte(a.(string)), &s1)
		jsoniter.Unmarshal([]byte(b.(string)), &s2)
		return int(s1.Time - s2.Time)
	})

	return strings
}

func (w *Waiter) Del(ctx context.Context, data *Event) {
	w.Rdb.HDel(ctx, w.Key, data.Id)
}

func (r *Receiver) Received(ctx context.Context, data *Event) {
	byteData, err := jsoniter.Marshal(data)
	if err != nil {
		return
	}
	r.Rdb.HSet(ctx, r.Key, data.Id, string(byteData))
	r.Rdb.Expire(ctx, r.Key, r.ExpireTime)
}

func (r *Receiver) IsReceived(ctx context.Context, data []byte) bool {
	var event Event
	err := jsoniter.Unmarshal(data, &event)
	if err != nil {
		return false
	}
	result, err := r.Rdb.HGet(ctx, r.Key, event.Id).Result()
	if err != nil {
		return false
	}

	if result != "" {
		fmt.Println("IsReceived result is empty", result)
		return true
	}

	return false
}

func (o *Offset) UpdateOffset(ctx context.Context, data *Event) {
	old := o.Rdb.Get(ctx, o.Key).Val()
	oldTime, err := strconv.Atoi(old)
	if err != nil {
		o.Rdb.Set(ctx, o.Key, strconv.Itoa(int(data.Time)), o.ExpireTime)
		return
	}
	if int64(oldTime) > data.Time {
		return
	}
	o.Rdb.Set(ctx, o.Key, strconv.Itoa(int(data.Time)), o.ExpireTime)
}

func (o *Offset) Offset(ctx context.Context) int64 {
	result, err := o.Rdb.Get(ctx, o.Key).Result()
	if err != nil {
		return 0
	}
	convResult, err := strconv.Atoi(result)
	if err != nil {
		return 0
	}
	return int64(convResult)
}

func GenWaiterKey(channel, id string) string {
	return fmt.Sprintf(waiterPrefix, channel, id)
}

func GenReceiverKey(channel, id string) string {
	return fmt.Sprintf(receiverPrefix, channel, id)
}

func GenOffsetKey(channel, id string) string {
	return fmt.Sprintf(offsetPrefix, channel, id)
}
