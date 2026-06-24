// Package main 提供 Swagger 2.0 文档解析和任务自动生成功能。
// 读取 Swag 生成的 swagger.json，解析所有 API 接口，
// 按规则自动创建 http_call 监控任务。
package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"task-scheduler/scheduler"
)

// ─────────────────── Swagger 2.0 数据结构 ───────────────────

// swaggerDoc 是 Swagger 2.0 文档的顶层结构。
type swaggerDoc struct {
	Swagger  string                  `json:"swagger"`
	Info     swaggerInfo             `json:"info"`
	Host     string                  `json:"host"`
	BasePath string                  `json:"basePath"`
	Schemes  []string                `json:"schemes"`
	Paths    map[string]swaggerPath  `json:"paths"`
	Defs     map[string]swaggerDef   `json:"definitions"`
}

type swaggerInfo struct {
	Title       string `json:"title"`
	Description string `json:"description"`
	Version     string `json:"version"`
}

// swaggerPath 映射 HTTP 方法到具体操作（GET/POST/PUT/DELETE...）。
type swaggerPath map[string]swaggerOp

// swaggerOp 表示一个 API 操作。
type swaggerOp struct {
	Summary     string              `json:"summary"`
	Description string              `json:"description"`
	Tags        []string            `json:"tags"`
	Security    []map[string][]string `json:"security"`
	Parameters  []swaggerParam      `json:"parameters"`
	Consumes    []string            `json:"consumes"`
}

// swaggerParam 表示一个 API 参数（路径参数、查询参数、请求体等）。
type swaggerParam struct {
	Name        string          `json:"name"`
	In          string          `json:"in"` // path / query / body / formData
	Required    bool            `json:"required"`
	Type        string          `json:"type"` // string / integer / number / file / array
	Description string          `json:"description"`
	Default     interface{}     `json:"default"`
	Schema      *swaggerSchemaRef `json:"schema"`
	Items       *swaggerParam   `json:"items"` // 用于 array 类型的元素定义
}

// swaggerSchemaRef 引用 definitions 中的定义。
type swaggerSchemaRef struct {
	Ref string `json:"$ref"`
}

// swaggerDef 表示 definitions 中的一个类型定义。
type swaggerDef struct {
	Type       string                   `json:"type"`
	Required   []string                 `json:"required"`
	Properties map[string]swaggerProp   `json:"properties"`
}

// swaggerProp 表示类型定义中的一个属性。
type swaggerProp struct {
	Type        string        `json:"type"`
	Description string        `json:"description"`
	Minimum     *float64       `json:"minimum"`
	MinLength   *int          `json:"minLength"`
	MaxLength   *int          `json:"maxLength"`
	MinItems    *int          `json:"minItems"`
	Enum        []interface{} `json:"enum"`
	Items       *swaggerProp  `json:"items"`
	Ref         string        `json:"$ref"`
}

// ─────────────────── 固定路径 ───────────────────

// SwaggerDir 是 swagger.json 文件的固定存放目录。
// 所有对 swagger.json 的读写操作均基于此目录。
const SwaggerDir = `D:\DEMO\api test`

// ─────────────────── 跳过列表 ───────────────────

// skipPaths 定义不需要生成任务的接口路径（上传、外部回调等）。
var skipPaths = map[string]bool{
	"/admin/upload": true, // multipart/form-data 上传，无法用简单 JSON 测试
	"/alipay/notify": true, // 支付宝异步通知，外部回调
}

// ─────────────────── 核心函数 ───────────────────

// LoadTasksFromSwagger 解析 swagger.json 文件，自动生成任务并提交到调度器。
// 返回项目名称（来自 swagger info.title）和创建的任务数量。
func LoadTasksFromSwagger(sched *scheduler.Scheduler, path string) (string, int, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return "", 0, fmt.Errorf("读取 swagger 文件失败: %w", err)
	}

	var doc swaggerDoc
	if err := json.Unmarshal(data, &doc); err != nil {
		return "", 0, fmt.Errorf("解析 swagger 文件失败: %w", err)
	}

	// 项目名 = 文件名 + info.title 的 ASCII 部分（HTTP Header 仅支持 ASCII）
	fileStem := strings.TrimSuffix(filepath.Base(path), filepath.Ext(path))
	titlePart := sanitizeProjectName(doc.Info.Title)
	projectName := fileStem
	if titlePart != "" && titlePart != fileStem {
		projectName = fileStem + "_" + titlePart
	}
	if projectName == "" {
		projectName = "imported_project"
	}

	log.Printf("[Swagger] 开始加载项目「%s」，共 %d 个接口路径", projectName, len(doc.Paths))

	// 用 map 记录已生成的 POST 任务 ID，供 DELETE 关联
	// key: 资源路径（如 "/admin/product"），value: 任务 ID
	postTaskIDs := make(map[string]string)

	count := 0
	for path, methods := range doc.Paths {
		for method, op := range methods {
			method = strings.ToUpper(method)

			// 跳过不支持的 HTTP 方法
			if method != "GET" && method != "POST" && method != "PUT" && method != "DELETE" {
				continue
			}

			// 跳过黑名单
			if skipPaths[path] {
				log.Printf("[Swagger] 跳过 %s %s（黑名单）", method, path)
				continue
			}
			// 跳过 multipart/form-data
			if hasMultipart(op.Consumes) {
				log.Printf("[Swagger] 跳过 %s %s（文件上传）", method, path)
				continue
			}

			task := buildTask(doc, path, method, op, projectName, postTaskIDs)
			if task == nil {
				continue
			}

			if err := sched.Submit(task); err != nil {
				log.Printf("[Swagger] 创建任务失败 %s %s: %v", method, path, err)
				continue
			}

			// 记录 POST 任务 ID，供同资源 DELETE 关联
			if method == "POST" {
				resourcePath := extractResourcePath(path)
				postTaskIDs[resourcePath] = task.ID
			}

			count++
			authMark := ""
			if hasAuth(op.Security) {
				authMark = " [需认证]"
			}
			log.Printf("[Swagger] 已创建%s: %s %s (%s)", authMark, method, path, task.Name)
		}
	}

	log.Printf("[Swagger] 项目「%s」加载完成，共创建 %d 个任务", projectName, count)
	return projectName, count, nil
}

// ─────────────────── 任务构建 ───────────────────

// buildTask 根据 Swagger 操作构造一个 http_call 任务。
func buildTask(doc swaggerDoc, path, method string, op swaggerOp, projectName string, postTaskIDs map[string]string) *scheduler.Task {
	// 构建完整 URL
	scheme := "http"
	if len(doc.Schemes) > 0 {
		scheme = doc.Schemes[0]
	}
	baseURL := fmt.Sprintf("%s://%s%s", scheme, doc.Host, doc.BasePath)

	// payload 结构
	payload := map[string]interface{}{
		"url":    baseURL + path,
		"method": method,
	}

	// 是否需要认证
	if hasAuth(op.Security) {
		payload["need_auth"] = true
	}

	// 处理路径参数：替换 {xxx} 为默认值
	finalURL := baseURL + path

	// 收集路径参数名
	var pathParamNames []string
	for _, p := range op.Parameters {
		if p.In == "path" {
			pathParamNames = append(pathParamNames, p.Name)
		}
	}

	// 处理 query 参数
	var queryParams []string
	for _, p := range op.Parameters {
		if p.In == "query" {
			val := paramDefaultValue(p)
			if val != "" {
				queryParams = append(queryParams, fmt.Sprintf("%s=%s", p.Name, val))
			}
		}
	}
	if len(queryParams) > 0 {
		finalURL += "?" + strings.Join(queryParams, "&")
	}
	payload["url"] = finalURL

	// 处理 DELETE：尝试关联同资源 POST，动态注入 ID
	if method == "DELETE" {
		resourcePath := extractResourcePath(path)
		if postID, ok := postTaskIDs[resourcePath]; ok {
			// 有对应的 POST 任务 → 设置依赖 + ID 继承
			task := &scheduler.Task{
				Name:        fmt.Sprintf("[%s] %s %s", projectName, op.Summary, path),
				Type:        "http_call",
				Payload:     mustMarshal(payload),
				MaxRetries:  1,
				Timeout:     15,
				Namespace:   projectName,
				ScheduledAt: time.Now().Add(5 * time.Second), // 延迟 5 秒
				DependsOn:   postID,
				Status:      scheduler.StatusPending,
			}
			// 标记 URL 中的 {id} 需要从父任务注入
			task.Payload = injectInheritFlag(task.Payload, postID, pathParamNames)
			return task
		}
		// 无对应 POST → 用默认 ID=1，降低调度优先级
		finalURL = replacePathParams(finalURL, pathParamNames, "1")
		payload["url"] = finalURL
	}

	// 处理 POST/PUT 的请求体
	if method == "POST" || method == "PUT" {
		for _, p := range op.Parameters {
			if p.In == "body" && p.Schema != nil {
				bodyJSON := generateBody(doc, p.Schema.Ref)
				if bodyJSON != "" {
					payload["body"] = bodyJSON
				}
			}
		}
	}

	// 处理 PUT 的路径参数（默认填 "1"）
	if method == "PUT" && len(pathParamNames) > 0 {
		finalURL = replacePathParams(finalURL, pathParamNames, "1")
		payload["url"] = finalURL
	}

	// 处理 GET 的路径参数（默认填 "1"）
	if method == "GET" && len(pathParamNames) > 0 {
		finalURL = replacePathParams(finalURL, pathParamNames, "1")
		payload["url"] = finalURL
	}

	delay := int64(0)
	// 破坏性操作给更长延迟，给创建类任务留执行时间
	if method == "DELETE" || method == "PUT" {
		delay = 3
	}

	return &scheduler.Task{
		Name:        fmt.Sprintf("[%s] %s %s", projectName, op.Summary, path),
		Type:        "http_call",
		Payload:     mustMarshal(payload),
		MaxRetries:  1,
		Timeout:     15,
		Namespace:   projectName,
		ScheduledAt: time.Now().Add(time.Duration(delay) * time.Second),
		Status:      scheduler.StatusPending,
	}
}

// ─────────────────── 辅助函数 ───────────────────

// hasAuth 检查操作是否需要认证。
func hasAuth(security []map[string][]string) bool {
	return len(security) > 0
}

// hasMultipart 检查 consumes 是否包含 multipart/form-data。
func hasMultipart(consumes []string) bool {
	for _, c := range consumes {
		if strings.Contains(c, "multipart") {
			return true
		}
	}
	return false
}

// sanitizeProjectName 清理项目名，仅保留 ASCII 安全字符（HTTP Header 不支持中文）。
func sanitizeProjectName(name string) string {
	// 提取所有 ASCII 字母、数字、连字符、下划线
	re := regexp.MustCompile(`[^a-zA-Z0-9_-]`)
	result := re.ReplaceAllString(name, "_")
	// 合并连续下划线
	result = regexp.MustCompile(`_+`).ReplaceAllString(result, "_")
	// 去掉首尾下划线
	result = strings.Trim(result, "_")
	if result == "" {
		return "imported_project"
	}
	return result
}

// extractResourcePath 从 API 路径中提取资源路径（去掉 {id} 等参数后缀）。
// 例如：/admin/product/{id} → /admin/product
//       /admin/flash/{id}/warmup → /admin/flash  (取到第一个 {xx} 之前)
var pathParamRE = regexp.MustCompile(`/\{[^}]+\}.*$`)

func extractResourcePath(path string) string {
	return pathParamRE.ReplaceAllString(path, "")
}

// replacePathParams 替换 URL 中的路径参数为给定的默认值。
func replacePathParams(url string, paramNames []string, defaultVal string) string {
	result := url
	for _, name := range paramNames {
		result = strings.Replace(result, "{"+name+"}", defaultVal, 1)
	}
	return result
}

// paramDefaultValue 根据参数定义生成合理的默认值。
func paramDefaultValue(p swaggerParam) string {
	if p.Default != nil {
		return fmt.Sprintf("%v", p.Default)
	}
	switch p.Type {
	case "integer", "number":
		return "1"
	case "string":
		return "test"
	default:
		return ""
	}
}

// injectInheritFlag 在 payload JSON 中注入 inherit_id_from_parent 和 parent_task_id 标记。
// runner 执行时检测到这些标记，会从依赖任务 Result 中提取 ID 替换 URL 中的 {xxx}。
func injectInheritFlag(payloadJSON, parentTaskID string, pathParamNames []string) string {
	var m map[string]interface{}
	if err := json.Unmarshal([]byte(payloadJSON), &m); err != nil {
		return payloadJSON
	}
	m["inherit_id_from_parent"] = true
	m["parent_task_id"] = parentTaskID
	// 将 URL 中的 {id} 替换为占位符，runner 执行时动态替换
	if url, ok := m["url"].(string); ok {
		for _, name := range pathParamNames {
			url = strings.Replace(url, "{"+name+"}", "__FROM_PARENT__", 1)
		}
		m["url"] = url
	}
	b, _ := json.Marshal(m)
	return string(b)
}

// ─────────────────── 请求体自动生成 ───────────────────

// generateBody 根据 definitions 引用生成合法的 JSON 请求体示例。
// ref 格式："#/definitions/request.CreateProductRequest"
func generateBody(doc swaggerDoc, ref string) string {
	if ref == "" {
		return "{}"
	}
	// 提取定义名：从 "#/definitions/request.CreateProductRequest" → "request.CreateProductRequest"
	defName := strings.TrimPrefix(ref, "#/definitions/")
	if defName == ref {
		return "{}"
	}

	def, ok := doc.Defs[defName]
	if !ok {
		return "{}"
	}

	body := make(map[string]interface{})
	for name, prop := range def.Properties {
		body[name] = propToValue(prop, doc)
	}
	// 确保必填字段存在
	for _, req := range def.Required {
		if _, ok := body[req]; !ok {
			body[req] = propToValue(def.Properties[req], doc)
		}
	}

	b, err := json.Marshal(body)
	if err != nil {
		return "{}"
	}
	return string(b)
}

// propToValue 根据属性定义生成合理的示例值。
func propToValue(prop swaggerProp, doc swaggerDoc) interface{} {
	// 嵌套引用
	if prop.Ref != "" {
		return json.RawMessage(generateBody(doc, prop.Ref))
	}

	switch prop.Type {
	case "string":
		if len(prop.Enum) > 0 {
			return prop.Enum[0]
		}
		minLen := 3
		if prop.MinLength != nil {
			minLen = *prop.MinLength
		}
		// 生成满足最小长度的字符串
		s := "test"
		for len(s) < minLen {
			s += "x"
		}
		return s
	case "integer":
		min := 1
		if prop.Minimum != nil {
			min = int(*prop.Minimum)
		}
		return min
	case "number":
		min := 1.0
		if prop.Minimum != nil {
			min = *prop.Minimum
		}
		return min
	case "boolean":
		return true
	case "array":
		minItems := 1
		if prop.MinItems != nil {
			minItems = *prop.MinItems
		}
		arr := make([]interface{}, minItems)
		if prop.Items != nil {
			itemVal := propToValue(*prop.Items, doc)
			for i := 0; i < minItems; i++ {
				arr[i] = itemVal
			}
		}
		return arr
	default:
		return "test"
	}
}

// mustMarshal 将对象序列化为 JSON 字符串，失败时返回 "{}"。
func mustMarshal(v interface{}) string {
	b, err := json.Marshal(v)
	if err != nil {
		return "{}"
	}
	return string(b)
}

// ─────────────────── 项目列表功能 ───────────────────

// ScanAndLoadSwaggerDir 扫描固定目录下的所有 .json 文件，逐个加载为测试项目。
// 返回已加载的项目名称列表。
func ScanAndLoadSwaggerDir(sched *scheduler.Scheduler) ([]string, error) {
	// 确保目录存在
	if err := os.MkdirAll(SwaggerDir, 0755); err != nil {
		return nil, fmt.Errorf("创建 swagger 目录失败: %w", err)
	}

	entries, err := os.ReadDir(SwaggerDir)
	if err != nil {
		return nil, fmt.Errorf("读取 swagger 目录失败: %w", err)
	}

	var loaded []string
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		if !strings.HasSuffix(strings.ToLower(entry.Name()), ".json") {
			continue
		}

		filePath := filepath.Join(SwaggerDir, entry.Name())
		log.Printf("[Swagger] 发现文件: %s", filePath)

		projectName, count, err := LoadTasksFromSwagger(sched, filePath)
		if err != nil {
			log.Printf("[Swagger] 加载 %s 失败: %v", entry.Name(), err)
			continue
		}
		loaded = append(loaded, projectName)
		log.Printf("[Swagger] 项目「%s」加载完成，%d 个任务", projectName, count)
	}

	if len(loaded) == 0 {
		log.Printf("[Swagger] 目录 %s 下未找到可加载的 swagger 文件", SwaggerDir)
	}

	return loaded, nil
}

// GetProjectNames 从调度器获取所有不同的项目名（namespace）。
func GetProjectNames(sched *scheduler.Scheduler) ([]string, error) {
	// 用空 namespace 获取全量任务
	tasks, err := sched.ListTasks("")
	if err != nil {
		return nil, err
	}

	seen := make(map[string]bool)
	var projects []string
	for _, t := range tasks {
		ns := t.Namespace
		if ns == "" {
			ns = "default"
		}
		if !seen[ns] {
			seen[ns] = true
			projects = append(projects, ns)
		}
	}

	// 确保至少有一个 default
	if len(projects) == 0 {
		projects = append(projects, "default")
	}

	return projects, nil
}

// DeleteProjectTasks 删除指定项目（namespace）下的所有任务。
func DeleteProjectTasks(sched *scheduler.Scheduler, namespace string) (int, error) {
	tasks, err := sched.ListTasks(namespace)
	if err != nil {
		return 0, err
	}

	deleted := 0
	for _, t := range tasks {
		if err := sched.DeleteTask(t.ID); err != nil {
			log.Printf("[Swagger] 删除旧任务 %s 失败: %v", t.ID, err)
			continue
		}
		deleted++
	}
	return deleted, nil
}

// ReloadSwaggerProject 重载指定路径的 swagger 文件。
// 先删除同名项目的旧任务，再重新加载。
func ReloadSwaggerProject(sched *scheduler.Scheduler, path string) (string, int, error) {
	// 先解析出项目名
	data, err := os.ReadFile(path)
	if err != nil {
		return "", 0, fmt.Errorf("读取 swagger 文件失败: %w", err)
	}
	var doc swaggerDoc
	if err := json.Unmarshal(data, &doc); err != nil {
		return "", 0, fmt.Errorf("解析 swagger 文件失败: %w", err)
	}
	projectName := sanitizeProjectName(doc.Info.Title)
	if projectName == "" {
		projectName = "未命名项目"
	}

	// 删除同名项目的旧任务
	deleted, _ := DeleteProjectTasks(sched, projectName)
	if deleted > 0 {
		log.Printf("[Swagger] 已删除项目「%s」的 %d 个旧任务", projectName, deleted)
	}

	// 重新加载
	return LoadTasksFromSwagger(sched, path)
}

// CopySwaggerToProject 将上传的 swagger.json 内容写入固定目录 D:\DEMO\api test\。
// 文件名基于 swagger info.title 自动生成，返回写入的完整文件路径。
func CopySwaggerToProject(data []byte, fileName string) (string, error) {
	// 确保目录存在
	if err := os.MkdirAll(SwaggerDir, 0755); err != nil {
		return "", fmt.Errorf("创建 swagger 目录失败: %w", err)
	}

	if fileName == "" {
		fileName = "swagger.json"
	}
	// 确保文件名以 .json 结尾
	if !strings.HasSuffix(strings.ToLower(fileName), ".json") {
		fileName += ".json"
	}

	destPath := filepath.Join(SwaggerDir, fileName)
	if err := os.WriteFile(destPath, data, 0644); err != nil {
		return "", fmt.Errorf("写入 swagger.json 失败: %w", err)
	}
	log.Printf("[Swagger] 文件已保存到: %s", destPath)
	return destPath, nil
}
