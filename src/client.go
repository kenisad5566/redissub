package src

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"github.com/gorilla/websocket"
	jsoniter "github.com/json-iterator/go"
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
	maxMessageSize = 512

	// Send buffer size
	bufSize = 256
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

		mu sync.Mutex
	}

	HandlerSubId func(subId int64) error
	GetSubId     func(channel string) int64

	ChannelClients struct {
		SubIdClient map[string][]*Client
		mu          sync.Mutex
	}
)

func (ncc *ChannelClients) subscribe(channel string, client *Client) {
	ncc.mu.Lock()
	defer ncc.mu.Unlock()
	if _, ok := ncc.SubIdClient[channel]; !ok {
		ncc.SubIdClient[channel] = append(ncc.SubIdClient[channel], client)
	}
}

func (ncc *ChannelClients) unSubscribe(channel string, client *Client) {
	ncc.mu.Lock()
	defer ncc.mu.Unlock()
	if _, ok := ncc.SubIdClient[channel]; !ok {
		for idx, v := range ncc.SubIdClient[channel] {
			// remove client from channel'subId map
			if v == client {
				ncc.SubIdClient[channel] = append(ncc.SubIdClient[channel][:idx], ncc.SubIdClient[channel][idx+1:]...)
			}
		}

		// empty
		if len(ncc.SubIdClient[channel]) == 0 {
			delete(ncc.SubIdClient, channel)
		}
	}
}

func MustNewClient(ctx context.Context, conn *websocket.Conn) *Client {
	return &Client{
		Channels: []string{},
		conn:     conn,
		SubIds:   map[string]int64{},
		Send:     make(chan []byte, bufSize),
		Ctx:      ctx,
	}
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
			fmt.Printf("read error % v \n", err)
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
