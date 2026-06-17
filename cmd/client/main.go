// Package main 是任务调度系统的命令行客户端。
// 可以通过命令行快速创建、查看和管理任务，无需浏览器。
//
// 用法示例：
//
//	go run cmd/client/main.go create -name "测试" -type echo -payload "hello"
//	go run cmd/client/main.go list
//	go run cmd/client/main.go get -id task-1
//	go run cmd/client/main.go stats
package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"time"
)

// 修复：默认端口 8888 与服务端默认端口一致
const defaultServer = "http://localhost:8888"

// 修复：使用带超时的 HTTP 客户端
var httpClient = &http.Client{Timeout: 30 * time.Second}

func main() {
	if len(os.Args) < 2 {
		printUsage()
		os.Exit(1)
	}

	server := flag.String("server", defaultServer, "调度服务器地址")
	flag.CommandLine.Parse(os.Args[2:])

	switch os.Args[1] {
	case "create":
		cmdCreate(*server)
	case "list":
		cmdList(*server)
	case "get":
		cmdGet(*server)
	case "delete":
		cmdDelete(*server)
	case "stats":
		cmdStats(*server)
	case "health":
		cmdHealth(*server)
	case "types":
		cmdTypes(*server)
	default:
		fmt.Printf("未知命令: %s\n\n", os.Args[1])
		printUsage()
		os.Exit(1)
	}
}

func printUsage() {
	fmt.Println(`TaskScheduler 命令行客户端

用法:
  client create  -name <名称> -type <类型> [-payload <负载>] [-delay <秒>] [-retry <次数>]
  client list    列出所有任务
  client get     -id <任务ID>
  client delete  -id <任务ID>
  client stats   查看调度系统统计
  client health  健康检查
  client types   查看支持的任务类型

示例:
  client create -name "发送邮件" -type email -payload '{"to":"a@b.com"}'
  client create -name "延迟任务" -type echo -payload "hello" -delay 10
  client list
  client get -id task-1`)
}

func cmdCreate(server string) {
	// 修复：移除废弃的 flag.Lookup 代码
	fs := flag.NewFlagSet("create", flag.ExitOnError)
	n := fs.String("name", "", "任务名称")
	t := fs.String("type", "", "任务类型")
	p := fs.String("payload", "", "任务负载(JSON)")
	d := fs.Int("delay", 0, "延迟执行秒数")
	r := fs.Int("retry", 3, "最大重试次数")
	to := fs.Int("timeout", 30, "超时秒数")
	fs.Parse(os.Args[2:])

	if *n == "" || *t == "" {
		log.Fatal("必须指定 -name 和 -type 参数")
	}

	body := map[string]interface{}{
		"name":        *n,
		"type":        *t,
		"payload":     *p,
		"delay":       *d,
		"max_retries": *r,
		"timeout":     *to,
	}
	data, err := json.Marshal(body)
	if err != nil {
		log.Fatalf("序列化请求失败: %v", err)
	}

	// 修复：使用带超时的 httpClient
	resp, err := httpClient.Post(server+"/api/tasks", "application/json", bytes.NewReader(data))
	if err != nil {
		log.Fatalf("请求失败: %v", err)
	}
	defer resp.Body.Close()

	printResponse(resp)
}

func cmdList(server string) {
	resp, err := httpClient.Get(server + "/api/tasks")
	if err != nil {
		log.Fatalf("请求失败: %v", err)
	}
	defer resp.Body.Close()
	printResponse(resp)
}

func cmdGet(server string) {
	fs := flag.NewFlagSet("get", flag.ExitOnError)
	i := fs.String("id", "", "任务ID")
	fs.Parse(os.Args[2:])

	if *i == "" {
		log.Fatal("必须指定 -id 参数")
	}

	resp, err := httpClient.Get(server + "/api/tasks/" + *i)
	if err != nil {
		log.Fatalf("请求失败: %v", err)
	}
	defer resp.Body.Close()
	printResponse(resp)
}

func cmdDelete(server string) {
	fs := flag.NewFlagSet("delete", flag.ExitOnError)
	i := fs.String("id", "", "任务ID")
	fs.Parse(os.Args[2:])

	if *i == "" {
		log.Fatal("必须指定 -id 参数")
	}

	req, err := http.NewRequest("DELETE", server+"/api/tasks/"+*i, nil)
	if err != nil {
		log.Fatalf("构造请求失败: %v", err)
	}
	resp, err := httpClient.Do(req)
	if err != nil {
		log.Fatalf("请求失败: %v", err)
	}
	defer resp.Body.Close()
	printResponse(resp)
}

func cmdStats(server string) {
	resp, err := httpClient.Get(server + "/api/stats")
	if err != nil {
		log.Fatalf("请求失败: %v", err)
	}
	defer resp.Body.Close()
	printResponse(resp)
}

func cmdHealth(server string) {
	resp, err := httpClient.Get(server + "/api/health")
	if err != nil {
		log.Fatalf("请求失败: %v", err)
	}
	defer resp.Body.Close()
	printResponse(resp)
}

func cmdTypes(server string) {
	resp, err := httpClient.Get(server + "/api/task-types")
	if err != nil {
		log.Fatalf("请求失败: %v", err)
	}
	defer resp.Body.Close()
	printResponse(resp)
}

func printResponse(resp *http.Response) {
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		log.Fatalf("读取响应失败: %v", err)
	}
	var pretty bytes.Buffer
	if err := json.Indent(&pretty, data, "", "  "); err != nil {
		fmt.Println(string(data))
		return
	}
	fmt.Println(pretty.String())
}
