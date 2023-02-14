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
		ExpireTime time.Duration // key ttl
		FreshTime time.Duration // fresh time scope
		rdb *red.Client
		key string
	}
)

func (o *OffLine) AddToOffline (ctx context.Context, data *Event)  {
	byteData, err := jsoniter.Marshal(data); if err != nil {
		return
	}
	o.rdb.ZAdd(ctx, o.key, &red.Z{
		Score: float64(data.Time),
		Member: string(byteData),
	})
}

func (o *OffLine) MessageByOffset (ctx context.Context, offset int64) ([]*Event,  error) {
	max := time.Now().UnixMilli()
	result, err := o.rdb.ZRangeByScore(ctx, o.key, &red.ZRangeBy{
		Min: strconv.Itoa(int(offset)) ,
		Max: strconv.Itoa(int(max)),
		Offset: 0,
		Count: 1<<64 - 1,
	}).Result(); if err != nil {
		return nil, err
	}

	fmt.Println("MessageByOffset", result)

	return nil, nil
}


func PullOffLine(ctx context.Context, offLine *OffLine, online *Online)  {
	offset := online.Offset.Offset(ctx)
	datas, err := offLine.MessageByOffset(ctx, offset); if err != nil {
		return
	}
	if datas != nil && len(datas) > 0 {
		for _, item := range datas {
		  if !online.Receiver.IsReceived(ctx, item) {
		  	online.Waiter.Push(ctx, item)
		  }
		}
	}
}


func GenOfflineKey(id string) string {
	return fmt.Sprintf(offlinePrefix, id)
}


