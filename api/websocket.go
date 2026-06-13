package api

import (
	"encoding/json"
	"net/http"
	"sync"

	"github.com/gorilla/websocket"
)

var upgrader = websocket.Upgrader{CheckOrigin: func(r *http.Request) bool { return true }}
var wsClients sync.Map

// WsHandler WebSocket 端点，实时推送任务状态变更。
func WsHandler(w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	wsClients.Store(conn, true)
	defer func() { wsClients.Delete(conn); conn.Close() }()
	// 保持连接，等待客户端关闭
	for { if _, _, err := conn.ReadMessage(); err != nil { break } }
}

// BroadcastTaskUpdate 向所有 WebSocket 客户端广播任务更新。
func BroadcastTaskUpdate(task interface{}) {
	data, _ := json.Marshal(task)
	wsClients.Range(func(key, _ interface{}) bool {
		if conn, ok := key.(*websocket.Conn); ok {
			conn.WriteMessage(websocket.TextMessage, data)
		}
		return true
	})
}
