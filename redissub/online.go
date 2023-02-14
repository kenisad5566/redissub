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
	waiterPrefix = "redissub:online:waiter:hash:%v:%v"
	receiverPrefix = "redissub:online:receiver:hash:%v:%v"
	offsetPrefix = "redissub:online:offset:%v:%v"
)

type (
	Online struct {
		Offset *Offset
		Waiter *Waiter
		Receiver *Receiver
	}
	
	Offset struct {
		Key string
		Rdb *red.Client
		ExpireTime time.Duration // Key ttl
	}



	Waiter struct {
		Key string
		Rdb *red.Client
		ExpireTime time.Duration // Key ttl
	}

	Receiver struct {
		Key string
		Rdb *red.Client
		ExpireTime time.Duration // Key ttl
	}
)

// AddToWaiter add new message to waiter
func (r *Online) AddToWaiter(ctx context.Context, data *Event)  {
	r.Waiter.Push(ctx, data)
}


func (r *Online) Ack(ctx context.Context, data *Event)  {
	r.Waiter.Del(ctx, data)
	r.Receiver.Received(ctx, data)
	r.Offset.UpdateOffset(ctx, data)
}


func (w *Waiter) Push(ctx context.Context, data *Event)  {
	byteData, err := jsoniter.Marshal(data); if err != nil {
		return
	}
	w.Rdb.HSet(ctx, w.Key, data.Id, string(byteData))
}

func (w *Waiter) All(ctx context.Context) []string {
	result, err := w.Rdb.HGetAll(ctx, w.Key,).Result(); if err != nil {
		return []string{}
	}
	fmt.Println("waiter All", result)

	return []string{}
}

func (w *Waiter) Del(ctx context.Context,data *Event)  {
	w.Rdb.HDel(ctx, w.Key, data.Id)
}


func (r *Receiver) Received(ctx context.Context, data *Event)  {
	byteData, err := jsoniter.Marshal(data); if err != nil {
		return
	}
	r.Rdb.HSet(ctx, r.Key, data.Id, string(byteData))
}

func (r *Receiver) IsReceived(ctx context.Context, data *Event) bool {
	result,err := r.Rdb.HGet(ctx, r.Key, data.Id).Result(); if err != nil {
		return false
	}

	if result != "" {
		return true
	}

	return false
}

func (o *Offset) UpdateOffset(ctx context.Context, data *Event)  {
	o.Rdb.Set(ctx, o.Key, strconv.Itoa(int(data.Time)), o.ExpireTime)
}

func (o *Offset) Offset(ctx context.Context) int64 {
	result, err := o.Rdb.Get(ctx, o.Key).Result(); if err != nil {
		return 0
	}
	convResult, err :=  strconv.Atoi(result); if err != nil {
		return 0
	}
	return  int64(convResult)
}

func GenWaiterKey(channel, id string) string {
	return fmt.Sprintf(waiterPrefix,channel, id)
}

func GenReceiverKey(channel,id string) string {
	return fmt.Sprintf(receiverPrefix,channel, id)
}

func GenOffsetKey(channel,id string) string {
	return fmt.Sprintf(offsetPrefix,channel, id)
}







