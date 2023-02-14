package redissub

import (
	"context"
	"fmt"
	red "github.com/go-redis/redis/v8"
	jsoniter "github.com/json-iterator/go"
	"math"
	"sync"
	"sync/atomic"
)

type Reviver func(key string, value string) interface{}

type PubSubRedisOptions struct {
	Publisher  *red.Client
	Subscriber *red.Client
	SolidOption *SolidOption
}




type OnMessage func(client *Client, data []byte)

type Listener struct {
	Client    *Client
	Channel   string
	OnMessage OnMessage
}

type PubSubClient struct {
	Publisher   *red.Client
	Subscriber  *red.Client
	subMap      map[int64]*Listener
	subsRefsMap map[string][]int64
	subId       int64
	PubSub      *red.PubSub
	mu          sync.Mutex
	DropRun     chan struct{}
	SolidOption *SolidOption
}

func NewPubSubClient(pubSubRedisOptions PubSubRedisOptions) *PubSubClient {
	pubSubClient := &PubSubClient{
		Publisher:   pubSubRedisOptions.Publisher,
		Subscriber:  pubSubRedisOptions.Subscriber,
		subMap:      map[int64]*Listener{},
		subsRefsMap: map[string][]int64{},
		subId:       int64(0),
		PubSub:      pubSubRedisOptions.Subscriber.Subscribe(context.Background()),
		DropRun:     make(chan struct{}, 0),
		SolidOption:pubSubRedisOptions.SolidOption, // Key ttl
	}

	go pubSubClient.Run()
	return pubSubClient
}

func (p *PubSubClient) Subscribe(client *Client, channel string, onMessage OnMessage) int64 {
	if p.subId >= math.MaxInt64 {
		p.subId = 0
	}
	atomic.AddInt64(&p.subId, 1)
	p.mu.Lock()
	defer func() {
		p.reSubscribe()
		p.mu.Unlock()
	}()

	p.subMap[p.subId] = &Listener{client, channel, onMessage}
	if _, ok := p.subsRefsMap[channel]; ok {
		p.subsRefsMap[channel] = append(p.subsRefsMap[channel], p.subId)
		return p.subId
	}

	p.subsRefsMap[channel] = []int64{p.subId}
	return p.subId
}

func (p *PubSubClient) UnSubscribe(id int64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if listener, ok := p.subMap[id]; ok {
		channel := listener.Channel
		if subIds, ok := p.subsRefsMap[channel]; ok {
			for idx, idv := range subIds {
				// update subId slice
				if idv == id {
					p.subsRefsMap[channel] = append(p.subsRefsMap[channel][:idx], p.subsRefsMap[channel][idx+1:]...)
				}
			}

			// empty
			if len(p.subsRefsMap[channel]) == 0 {
				delete(p.subsRefsMap, channel)
				go p.reSubscribe()
			}
		}
	}
	delete(p.subMap, id)
}

func (p *PubSubClient) Publish(ctx context.Context, channel string, message []byte) {
	p.Publisher.Publish(ctx, channel, message)
	offline := &OffLine{
		ExpireTime:p.SolidOption.ExpireTime,
		Rdb:p.Publisher,
		Key: GenOfflineKey(channel),
	}
	fmt.Println("Publish", string(message))
	fmt.Println("offline",offline)
	offline.AddToOffline(ctx, message)
}

func (p *PubSubClient) reSubscribe() {
	channels := getKeys(p.subsRefsMap)
	p.PubSub.Close()
	p.PubSub = p.Subscriber.Subscribe(context.Background(), channels...)
	p.DropRun <- struct{}{}
}

func (p *PubSubClient) Run() {
	for {
		select {
		case msg, ok := <-p.PubSub.Channel():
			if ok {
				channel := msg.Channel
				payLoad := msg.Payload

				if ids, ok := p.subsRefsMap[channel]; ok {
					for _, id := range ids {
						if listener := p.subMap[id]; ok {
							listener.OnMessage(listener.Client, []byte(payLoad))

							var event Event
							err := jsoniter.Unmarshal([]byte(payLoad), &event); if err == nil {
								listener.Client.Solid.Push(context.Background(), channel, &event)
							}
						}
					}
				}
			}
		case <-p.DropRun:
			break
		}
	}
}

func getKeys(m map[string][]int64) []string {
	var ret = []string{}
	for k := range m {
		ret = append(ret, k)
	}
	return ret
}
