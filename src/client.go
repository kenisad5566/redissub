package src

import (
	"context"
	"errors"
	"github.com/gorilla/websocket"
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
		// ChannelClients
		ChannelClients *ChannelClients
		// ctx
		Ctx context.Context

		mu sync.Mutex
	}


	HandlerSubId func(subId int64) error
	GetSubId func(channel string) int64


	ChannelClients struct {
		SubIdClient map[string][]*Client
		mu          sync.Mutex
	}
)
var channelClients = newChannelClients()
func newChannelClients() *ChannelClients {
	return &ChannelClients{
		SubIdClient: map[string][]*Client{},
	}
}

func (ncc *ChannelClients) subscribe(channel string, client *Client)  {
	ncc.mu.Lock()
	defer ncc.mu.Unlock()
	if _, ok := ncc.SubIdClient[channel]; !ok {
		ncc.SubIdClient[channel] = append(ncc.SubIdClient[channel], client)
	}
}

func (ncc *ChannelClients) unSubscribe(channel string, client *Client)  {
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
		Channels:       []string{},
		conn:           conn,
		SubIds:         map[string]int64{},
		Send:           make(chan []byte, bufSize),
		ChannelClients: channelClients,
		Ctx:            ctx,
	}
}

func (c *Client) Subscribe(channel string) error {
	if Contains(c.Channels, channel) {
		return errors.New("duplicate subscribe")
	}
	c.ChannelClients.subscribe(channel, c)
	c.mu.Lock()
	defer c.mu.Unlock()
	c.Channels = append(c.Channels, channel)
	return nil
}

func (c *Client) BindChannelWithSubId(channel string, subId int64)   {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.SubIds[channel] = subId
}



func (c *Client) UnSubscribe(channel string) int64 {
	c.ChannelClients.unSubscribe(channel, c)

	c.mu.Lock()
	for idx, v := range c.Channels {
		// remove channel from channel subId slice
		if v == channel {
			c.Channels  = append(c.Channels [:idx], c.Channels [idx+1:]...)
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
