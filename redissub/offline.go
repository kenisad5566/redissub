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
	offlinePrefix = "redissub:offline:zset:%v:%v"
)

type (
	OffLine struct {
		ExpireTime time.Duration // Key ttl
		Rdb *red.Client
		Key string
	}
)

func (o *OffLine) AddToOffline (ctx context.Context, data *Event)  {
	byteData, err := jsoniter.Marshal(data); if err != nil {
		return
	}
	o.Rdb.ZAdd(ctx, o.Key, &red.Z{
		Score: float64(data.Time),
		Member: string(byteData),
	})
}

func (o *OffLine) MessageByOffset (ctx context.Context, offset int64) ([]*Event,  error) {
	max := time.Now().UnixMilli()
	result, err := o.Rdb.ZRangeByScore(ctx, o.Key, &red.ZRangeBy{
		Min: strconv.Itoa(int(offset)) ,
		Max: strconv.Itoa(int(max)),
		Offset: 0,
		Count: 1<<63 - 1,
	}).Result(); if err != nil {
		return nil, err
	}

	fmt.Println("MessageByOffset", result)

	return nil, nil
}


func (o *OffLine) PullOffLine(ctx context.Context, online *Online)  {
	offset := online.Offset.Offset(ctx)
	datas, err := o.MessageByOffset(ctx, offset); if err != nil {
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


func GenOfflineKey(channel string) string {
	return fmt.Sprintf(offlinePrefix,channel)
}


