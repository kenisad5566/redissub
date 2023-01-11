package src

import (
	"bytes"
	"context"
	"fmt"
	"github.com/gorilla/websocket"
	"github.com/zeromicro/go-zero/core/jsonx"
	"log"
	"net/http"
	"time"
)

type (
	OnMessageWrapper struct {
		OnMessage  OnMessage
		ChannelFun ChannelFun
	}
	SubScribeFuncs map[string]OnMessageWrapper
	ChannelFun     func(ctx context.Context, data []byte) string

	Event struct {
		EventName string `json:"EventName"`
		From string `json:"From"`
		To string `json:"To"`
		Data []byte `json:"Data"`
	}

)

var (
	newline = []byte{'\n'}
	space   = []byte{' '}
	subScribeFuncs = SubScribeFuncs{}
)

var upgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
}

func (c *Client) ReadPump(pubSubClient *PubSubClient ) {
	defer func() {
		c.conn.Close()
		c = nil // 关闭连接，取消所有订阅
	}()
	c.conn.SetReadLimit(maxMessageSize)
	c.conn.SetReadDeadline(time.Now().Add(pongWait))
	c.conn.SetPongHandler(func(string) error { c.conn.SetReadDeadline(time.Now().Add(pongWait)); return nil })
	for {
		_, message, err := c.conn.ReadMessage()
		if err != nil {
			fmt.Printf("read error % v \n", err)
			if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
				log.Printf("error: %v", err)
			}
			break
		}
		message = bytes.TrimSpace(bytes.Replace(message, newline, space, -1))
		var event Event
		 _ = jsonx.Unmarshal(message, &event)
		if onMessageWrapper, ok := subScribeFuncs[event.EventName]; ok {
			onMessage := onMessageWrapper.OnMessage
			channelKeyFun := onMessageWrapper.ChannelFun
			var channel = event.EventName
			if channelKeyFun != nil {
				channel = channelKeyFun(c.Ctx, event.Data)
			}

			go func() {
				err = c.Subscribe(channel)
				if err != nil {
					return
				}
				subId := pubSubClient.Subscribe(c, channel, onMessage)
				c.BindChannelWithSubId(channel, subId)
			}()
		}

	}
}


// writePump pumps messages from the hub to the websocket connection.
//
// A goroutine running writePump is started for each connection. The
// application ensures that there is at most one writer to a connection by
// executing all writes from this goroutine.
func (c *Client) writePump() {
	ticker := time.NewTicker(pingPeriod)

	defer func() {
		ticker.Stop()
		c.conn.Close()
	}()

	for {
		select {
		case message, ok := <-c.Send:
			c.conn.SetWriteDeadline(time.Now().Add(writeWait))
			if !ok {
				// The hub closed the channel.
				c.conn.WriteMessage(websocket.CloseMessage, []byte{})
				return
			}

			w, err := c.conn.NextWriter(websocket.TextMessage)
			if err != nil {
				return
			}
			w.Write(message)

			// Add queued chat messages to the current websocket message.
			n := len(c.Send)
			for i := 0; i < n; i++ {
				w.Write(newline)
				w.Write(<-c.Send)
			}

			if err := w.Close(); err != nil {
				return
			}
		case <-ticker.C:
			c.conn.SetWriteDeadline(time.Now().Add(writeWait))
			if err := c.conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				return
			}
		}
	}
}

func AddWsEvent(eventName string, channelFun ChannelFun, onMessage OnMessage)  {
	fmt.Printf("add ws event")
	if _, ok := subScribeFuncs[eventName]; !ok {
		subScribeFuncs[eventName] = OnMessageWrapper{
			OnMessage: onMessage,
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

	ctx := r.Context()
	client := MustNewClient(ctx, conn)

	go client.ReadPump(pubSubClient)
	go client.writePump()
}
