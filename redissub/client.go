package redissub

import (
	"bytes"
	"context"
	"errors"
	"github.com/gorilla/websocket"
	jsoniter "github.com/json-iterator/go"
	"io"
	"log"
	"sync"
	"time"
)

const (
	// Time allowed to write a message to the peer.
	writeWait = 10 * time.Second

	// Time allowed to read the next pong message from the peer.
	//pongWait = 60 * time.Second
	pongWait = 60 * time.Second

	// Send pings to peer with this period. Must be less than pongWait.
	pingPeriod = (pongWait * 9) / 10

	// Maximum message size allowed from peer.
	maxMessageSize = 2048

	// Send buffer size
	bufSize = 256

	ackEvent = "ack"
)

type (
	Client struct {
		// The client subscribe channels
		Channels []string
		// The websocket connection.
		conn *websocket.Conn
		// The channel subId map
		SubIds map[string]int64
		// Buffered channel of outbound messages.
		Send chan []byte
		// ctx
		Ctx context.Context

		// uuid
		Id string

		Solid *Solid

		mu sync.Mutex
	}

	HandlerSubId func(subId int64) error
	GetSubId     func(channel string) int64
)

func MustNewClient(ctx context.Context, conn *websocket.Conn, id string, solidOption *SolidOption) *Client {
	client := &Client{
		Channels: []string{},
		conn:     conn,
		SubIds:   map[string]int64{},
		Send:     make(chan []byte, bufSize),
		Ctx:      ctx,
		Id:       id,
	}
	client.Solid = MustNewSolid(solidOption, client)

	return client
}

func (c *Client) Subscribe(channel string) error {
	if Contains(c.Channels, channel) {
		return errors.New("duplicate subscribe")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.Channels = append(c.Channels, channel)
	return nil
}

func (c *Client) BindChannelWithSubId(channel string, subId int64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.SubIds[channel] = subId
}

func (c *Client) UnSubscribe(channel string) int64 {
	c.mu.Lock()
	for idx, v := range c.Channels {
		// remove channel from channel subId slice
		if v == channel {
			c.Channels = append(c.Channels[:idx], c.Channels[idx+1:]...)
		}
	}
	c.mu.Unlock()

	// remove channel subId
	subId := c.SubIds[channel]
	c.mu.Lock()
	delete(c.SubIds, channel)
	c.mu.Unlock()
	return subId
}

func (c *Client) close(pubSubClient *PubSubClient) {
	for _, subId := range c.SubIds {
		pubSubClient.UnSubscribe(subId)
	}
	c.conn.Close()
	c = nil
}

func (c *Client) ReadPump(pubSubClient *PubSubClient) {
	defer func() {
		c.close(pubSubClient)
	}()
	c.conn.SetReadLimit(maxMessageSize)
	c.conn.SetReadDeadline(time.Now().Add(pongWait))
	c.conn.SetPongHandler(func(string) error { c.conn.SetReadDeadline(time.Now().Add(pongWait)); return nil })
	for {
		_, message, err := c.conn.ReadMessage()

		if err != nil {
			if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
				log.Printf("error: %v", err)
			}
			break
		}
		message = bytes.TrimSpace(bytes.Replace(message, newline, space, -1))
		var event Event
		_ = jsoniter.Unmarshal(message, &event)
		if onMessageWrapper, ok := subScribeFuncs[event.EventName]; ok {
			onMessage := onMessageWrapper.OnMessage
			channelKeyFun := onMessageWrapper.ChannelFun
			var channel = event.EventName
			if channelKeyFun != nil {
				channel = channelKeyFun(c.Ctx, []byte(event.Data))
			}

			go func() {
				err = c.Subscribe(channel)
				if err != nil {
					return
				}
				subId := pubSubClient.Subscribe(c, channel, onMessage)
				c.BindChannelWithSubId(channel, subId)

				go c.Solid.PullOfflineMessage() // pull offline message to waiter for resend
			}()
		}

		if event.EventName == ackEvent {
			var ackEvent Event
			_ = jsoniter.Unmarshal([]byte(event.Data), &ackEvent)
			c.Solid.Ack(context.Background(), &ackEvent)
		}

	}
}

// writePump pumps messages from the hub to the websocket connection.
//
// A goroutine running writePump is started for each connection. The
// application ensures that there is at most one writer to a connection by
// executing all writes from this goroutine.
func (c *Client) writePump(pubSubClient *PubSubClient) {
	ticker := time.NewTicker(pingPeriod)

	defer func() {
		ticker.Stop()
		c.close(pubSubClient)
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
			c.WriteData(w)

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

func (c *Client) WriteData(w io.WriteCloser) {
	c.mu.Lock()
	defer c.mu.Unlock()
	n := len(c.Send)
	for i := 0; i < n; i++ {
		w.Write(newline)
		w.Write(<-c.Send)
	}
	return
}
