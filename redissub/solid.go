package redissub

import (
	"context"
	red "github.com/go-redis/redis/v8"
	jsoniter "github.com/json-iterator/go"
	"time"
)

type (
	SolidOption struct {
		ExpireTime time.Duration // Key ttl
		Duration   time.Duration // resend message interval
		Rdb        *red.Client
	}

	Solid struct {
		Client     *Client
		ExpireTime time.Duration // Key ttl
		Duration   time.Duration // resend message interval
		Rdb        *red.Client
	}
)

func MustNewSolid(solidOption *SolidOption, client *Client) *Solid {
	return &Solid{
		Client:     client,
		ExpireTime: solidOption.ExpireTime,
		Duration:   solidOption.Duration,
		Rdb:        solidOption.Rdb,
	}
}

func (s *Solid) PullOfflineMessage() {
	ctx := context.Background()
	for _, channel := range s.Client.Channels {
		rdb := s.Rdb
		expireTime := s.ExpireTime
		id := s.Client.Id

		online := &Online{
			Waiter: &Waiter{
				Key:        GenWaiterKey(channel, id),
				Rdb:        rdb,
				ExpireTime: expireTime,
			},
			Receiver: &Receiver{
				Key:        GenReceiverKey(channel, id),
				Rdb:        rdb,
				ExpireTime: expireTime,
			},
			Offset: &Offset{
				Key:        GenOffsetKey(channel, id),
				Rdb:        rdb,
				ExpireTime: expireTime,
			},
		}

		offline := &OffLine{
			ExpireTime: expireTime,
			Rdb:        rdb,
			Key:        GenOfflineKey(channel),
		}
		offline.PullOffLine(ctx, online)
	}
}

func (s *Solid) Push(ctx context.Context, channel string, event []byte) {
	rdb := s.Rdb
	expireTime := s.ExpireTime
	id := s.Client.Id
	online := &Online{
		Waiter: &Waiter{
			Key:        GenWaiterKey(channel, id),
			Rdb:        rdb,
			ExpireTime: expireTime,
		},
		Receiver: &Receiver{
			Key:        GenReceiverKey(channel, id),
			Rdb:        rdb,
			ExpireTime: expireTime,
		},
		Offset: &Offset{
			Key:        GenOffsetKey(channel, id),
			Rdb:        rdb,
			ExpireTime: expireTime,
		},
	}
	online.Waiter.Push(ctx, event)
}

func (s *Solid) Ack(ctx context.Context, event *Event) {
	for _, channel := range s.Client.Channels {
		rdb := s.Rdb
		expireTime := s.ExpireTime
		id := s.Client.Id

		online := &Online{
			Waiter: &Waiter{
				Key:        GenWaiterKey(channel, id),
				Rdb:        rdb,
				ExpireTime: expireTime,
			},
			Receiver: &Receiver{
				Key:        GenReceiverKey(channel, id),
				Rdb:        rdb,
				ExpireTime: expireTime,
			},
			Offset: &Offset{
				Key:        GenOffsetKey(channel, id),
				Rdb:        rdb,
				ExpireTime: expireTime,
			},
		}
		online.Ack(ctx, event)
	}
}

func (s *Solid) MonitorReSend() {
	if s.Duration == 0 {
		return
	}
	ticker := time.NewTicker(s.Duration)

	defer func() {
		ticker.Stop()
	}()

	for {
		select {
		case <-ticker.C:
			for _, channel := range s.Client.Channels {
				c := channel
				GoSafe(func() {
					rdb := s.Rdb
					expireTime := s.ExpireTime
					id := s.Client.Id

					online := &Online{
						Waiter: &Waiter{
							Key:        GenWaiterKey(c, id),
							Rdb:        rdb,
							ExpireTime: expireTime,
						},
						Receiver: &Receiver{
							Key:        GenReceiverKey(c, id),
							Rdb:        rdb,
							ExpireTime: expireTime,
						},
						Offset: &Offset{
							Key:        GenOffsetKey(c, id),
							Rdb:        rdb,
							ExpireTime: expireTime,
						},
					}
					ctx := context.Background()
					strings := online.Waiter.All(ctx)
					for _, item := range strings {
						str := item.(string)
						if s.IsFresh(str) {
							continue
						}
						s.Client.Send <- []byte(str)
					}
				})
			}
		}
	}
}

// IsFresh message time pass through less than duration, fresh, resend after time duration
func (s *Solid) IsFresh(data string) bool {
	var event Event
	now := time.Now().UnixMilli()
	err := jsoniter.Unmarshal([]byte(data), &event)
	if err != nil {
		return false
	}
	if now-s.Duration.Milliseconds() < event.Time {
		return true
	}
	return false
}
