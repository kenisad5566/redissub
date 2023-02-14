package redissub

import (
	"context"
	red "github.com/go-redis/redis/v8"
	jsoniter "github.com/json-iterator/go"
	"strconv"
	"time"
)

type (
	Online struct {
		ExpireTime time.Duration // key ttl
		FreshTime time.Duration // fresh time scope
		Offset *Offset
		Waiter *Waiter
		Receiver *Receiver
		rdb *red.Client
	}
	
	Offset struct {
		key string
		rdb *red.Client
		ExpireTime time.Duration // key ttl
	}



	Waiter struct {
		key string
		rdb *red.Client
		ExpireTime time.Duration // key ttl
	}

	Receiver struct {
		key string
		rdb *red.Client
		ExpireTime time.Duration // key ttl
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
	w.rdb.HSet(ctx, w.key, data.Id, string(byteData))
}

func (w *Waiter) Del(ctx context.Context,data *Event)  {
	w.rdb.HDel(ctx, w.key, data.Id)
}


func (r *Receiver) Received(ctx context.Context, data *Event)  {
	byteData, err := jsoniter.Marshal(data); if err != nil {
		return
	}
	r.rdb.HSet(ctx, r.key, data.Id, string(byteData))
}

func (r *Receiver) IsReceived(ctx context.Context, data *Event) bool {
	result,err := r.rdb.HGet(ctx, r.key, data.Id).Result(); if err != nil {
		return false
	}

	if result != "" {
		return true
	}

	return false
}

func (o *Offset) UpdateOffset(ctx context.Context, data *Event)  {
	o.rdb.Set(ctx, o.key, strconv.Itoa(int(data.Time)), o.ExpireTime)
}

func (o *Offset) Offset(ctx context.Context) int64 {
	result, err := o.rdb.Get(ctx, o.key).Result(); if err != nil {
		return 0
	}
	convResult, err :=  strconv.Atoi(result); if err != nil {
		return 0
	}
	return  int64(convResult)
}









