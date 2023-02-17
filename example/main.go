package main

import (
	"context"
	"flag"
	"fmt"
	"github.com/go-redis/redis/v8"
	jsoniter "github.com/json-iterator/go"
	"github.com/kenisad5566/redissub/redissub"
	"github.com/zeromicro/go-zero/core/logx"
	"github.com/zeromicro/go-zero/core/service"
	"github.com/zeromicro/go-zero/rest"
	"net/http"
	"os"
	"sync/atomic"
	"time"
)

var (
	port    = flag.Int("port", 3333, "the port to listen")
	timeout = flag.Int64("timeout", 0, "timeout of milliseconds")
	cpu     = flag.Int64("cpu", 500, "cpu threshold")
)

// go run main.go
// visit localhost:3333 in browser
// click joinRoom button
// visit localhost:3333/push
// message will send to you

func main() {
	flag.Parse()

	logx.Disable()
	engine := rest.MustNewServer(rest.RestConf{
		ServiceConf: service.ServiceConf{
			Log: logx.LogConf{
				Mode: "console",
			},
		},
		Host:         "localhost",
		Port:         *port,
		Timeout:      *timeout,
		CpuThreshold: *cpu,
	})
	defer engine.Stop()

	sub := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", // no password set
		DB:       0,  // use default DB
	})

	pub := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", // no password set
		DB:       0,  // use default DB
	})
	solidOption := &redissub.SolidOption{
		ExpireTime: 3600 * time.Second,
		Duration:   3 * time.Second,
		Rdb:        pub,
	}

	PubSubClient := redissub.NewPubSubClient(redissub.PubSubRedisOptions{Publisher: pub, Subscriber: sub, SolidOption: solidOption})

	channel := "joinRoom"
	redissub.AddWsEvent("joinRoom", func(ctx context.Context, data []byte) string {
		return channel
	}, func(client *redissub.Client, data []byte) {
		client.Send <- data
	})

	engine.AddRoute(rest.Route{
		Method: http.MethodGet,
		Path:   "/",
		Handler: func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path != "/" {
				http.Error(w, "Not found", http.StatusNotFound)
				return
			}
			if r.Method != "GET" {
				http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
				return
			}

			http.ServeFile(w, r, "home.html")
		},
	})

	engine.AddRoute(rest.Route{
		Method: http.MethodGet,
		Path:   "/push",
		Handler: func(w http.ResponseWriter, r *http.Request) {
			event2 := &redissub.Event{
				Id:        GenUuid(time.Now()),
				EventName: "you event",
				Data:      "1",
				Time:      time.Now().UnixMilli(),
			}
			data2, _ := jsoniter.Marshal(event2)
			PubSubClient.Publish(context.Background(), channel, data2)
		},
	})

	engine.AddRoute(rest.Route{
		Method: http.MethodGet,
		Path:   "/ws",
		Handler: func(w http.ResponseWriter, r *http.Request) {
			redissub.ServeWs(PubSubClient, w, r, func(r *http.Request) string {
				return GenUuid(time.Now())
			})
		},
	})

	fmt.Println("listen")
	engine.Start()
}

var num int64

func GenUuid(t time.Time) string {
	s := t.Format("20060102150405")
	m := t.UnixNano()/1e6 - t.UnixNano()/1e9*1e3
	ms := sup(m, 3)
	p := os.Getpid() % 1000
	ps := sup(int64(p), 3)
	i := atomic.AddInt64(&num, 1)
	r := i % 10000
	rs := sup(r, 4)
	n := fmt.Sprintf("%s%s%s%s", s, ms, ps, rs)
	return n
}

func sup(i int64, n int) string {
	m := fmt.Sprintf("%d", i)
	for len(m) < n {
		m = fmt.Sprintf("0%s", m)
	}
	return m
}
