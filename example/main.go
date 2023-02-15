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
		Duration:   5 * time.Second,
		Rdb:        pub,
	}

	PubSubClient := redissub.NewPubSubClient(redissub.PubSubRedisOptions{Publisher: pub, Subscriber: sub, SolidOption: solidOption})

	mockChannelKey := "mockChannel"
	redissub.AddWsEvent("Room", func(ctx context.Context, data []byte) string {
		return mockChannelKey
	}, func(client *redissub.Client, data []byte) {
		client.Send <- data
	})

	redissub.AddWsEvent("joinRoom", func(ctx context.Context, data []byte) string {
		return "sendMsg"
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
			event := &redissub.Event{
				Id:        GenUuid(time.Now()),
				EventName: "test",
				Data:      "211",
				Time:      time.Now().UnixMilli(),
			}
			data, _ := jsoniter.Marshal(event)

			PubSubClient.Publish(context.Background(), mockChannelKey, data)

			event2 := &redissub.Event{
				Id:        GenUuid(time.Now()),
				EventName: "sendMsg",
				Data:      "211",
				Time:      time.Now().UnixMilli(),
			}
			data2, _ := jsoniter.Marshal(event2)
			PubSubClient.Publish(context.Background(), "sendMsg", data2)
		},
	})

	engine.AddRoute(rest.Route{
		Method: http.MethodGet,
		Path:   "/ws",
		Handler: func(w http.ResponseWriter, r *http.Request) {
			redissub.ServeWs(PubSubClient, w, r, func(r *http.Request) string {
				return GenUuid(time.Now())
			})
			w.Write([]byte("hello"))
		},
	})

	go func() {
		ticker := time.NewTicker(10 * time.Second)
		for {
			select {
			case <-ticker.C:
				fmt.Printf("PubSubClient %+v \n", PubSubClient)
				//PubSubClient.Publish(context.Background(), mockChannelKey, []byte("welcome someone"))
				//PubSubClient.Publish(context.Background(), "sendMsg", []byte("hello world"))
			}
		}
	}()
	fmt.Println("listen ")
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
