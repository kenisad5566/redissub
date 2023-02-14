package redissub

import (
	"context"
	"github.com/gorilla/websocket"
	"log"
	"net/http"
)

type (
	OnMessageWrapper struct {
		OnMessage  OnMessage
		ChannelFun ChannelFun
	}
	SubScribeFuncs map[string]OnMessageWrapper
	ChannelFun     func(ctx context.Context, data []byte) string

	Event struct {
		Id        string `json:"Id"`
		EventName string `json:"EventName"`
		From      string `json:"From"`
		To        string `json:"To"`
		Data      string `json:"Data"`
		Time      int64 `json:"Time"`
	}
)

var (
	newline        = []byte{'\n'}
	space          = []byte{' '}
	subScribeFuncs = SubScribeFuncs{}
)

var upgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
	CheckOrigin: func(r *http.Request) bool {
		return true
	},
}

func AddWsEvent(eventName string, channelFun ChannelFun, onMessage OnMessage) {
	if _, ok := subScribeFuncs[eventName]; !ok {
		subScribeFuncs[eventName] = OnMessageWrapper{
			OnMessage:  onMessage,
			ChannelFun: channelFun,
		}
	}
}

// ServeWs handles websocket requests from the peer.
func ServeWs(pubSubClient *PubSubClient, w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Println(err)
		return
	}

	id := "1"
	ctx := r.Context()
	client := MustNewClient(ctx, conn, id, pubSubClient.SolidOption)

	go client.ReadPump(pubSubClient)
	go client.writePump(pubSubClient)
	go client.Solid.PullOfflineMessage() // pull offline message to waiter for resend
	go client.Solid.MonitorReSend()
}