package api

import (
	"encoding/json"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

// wsWriteMsg 用于将写操作串行化到每个连接专用的写 goroutine。
type wsWriteMsg struct {
	data []byte
}

// wsClient 封装一个 WebSocket 连接及其专用的写 channel。
type wsClient struct {
	conn     *websocket.Conn
	writeCh  chan wsWriteMsg
	done     chan struct{}
}

var (
	// 修复：CheckOrigin 校验来源，拒绝跨站点 WebSocket 劫持。
	// 允许 localhost 和本机地址（开发/演示用），生产环境应配置允许的域名。
	upgrader = websocket.Upgrader{
		CheckOrigin: func(r *http.Request) bool {
			origin := r.Header.Get("Origin")
			if origin == "" {
				return true // 同源请求无 Origin 头
			}
			// 允许 localhost（所有端口）和 127.0.0.1
			return len(origin) > 16 && (origin[:16] == "http://localhost" || origin[:17] == "http://127.0.0.1")
		},
		ReadBufferSize:  1024,
		WriteBufferSize: 1024,
	}
	wsClientsMu sync.Mutex
	wsClients   = make(map[*wsClient]struct{})
)

// WsHandler WebSocket 端点，实时推送任务状态变更。
func WsHandler(w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}

	client := &wsClient{
		conn:    conn,
		writeCh: make(chan wsWriteMsg, 16),
		done:    make(chan struct{}),
	}

	wsClientsMu.Lock()
	wsClients[client] = struct{}{}
	wsClientsMu.Unlock()

	// 启动专用写 goroutine，串行化所有写操作（修复 WriteMessage 竞态条件）
	go client.writeLoop()

	// 读取循环：接收 Ping/Pong，检测客户端断开
	go func() {
		defer func() {
			wsClientsMu.Lock()
			delete(wsClients, client)
			wsClientsMu.Unlock()
			close(client.done)
			client.conn.Close()
		}()

		// 设置 Pong 处理器用于心跳检测
		client.conn.SetPongHandler(func(string) error {
			client.conn.SetReadDeadline(time.Now().Add(60 * time.Second))
			return nil
		})
		// 设置初始读超时
		client.conn.SetReadDeadline(time.Now().Add(60 * time.Second))

		for {
			_, _, err := client.conn.ReadMessage()
			if err != nil {
				break
			}
		}
	}()
}

// writeLoop 是连接专用的写 goroutine，串行化所有写操作。
func (c *wsClient) writeLoop() {
	defer c.conn.Close()
	for {
		select {
		case msg := <-c.writeCh:
			c.conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
			if err := c.conn.WriteMessage(websocket.TextMessage, msg.data); err != nil {
				return // 写入失败，退出
			}
		case <-c.done:
			return // 读取循环已退出
		}
	}
}

// BroadcastTaskUpdate 向所有 WebSocket 客户端广播任务更新。
func BroadcastTaskUpdate(task interface{}) {
	data, err := json.Marshal(task)
	if err != nil {
		log.Printf("[WebSocket] 序列化任务失败: %v", err)
		return
	}

	wsClientsMu.Lock()
	defer wsClientsMu.Unlock()

	for client := range wsClients {
		select {
		case client.writeCh <- wsWriteMsg{data: data}:
		default:
			// 写 channel 满，跳过此客户端（避免阻塞广播）
		}
	}
}
