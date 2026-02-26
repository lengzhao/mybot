package admin

import (
	"context"
	"embed"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/lengzhao/mybot"
	"gopkg.in/yaml.v3"
)

//go:embed static/*
var staticFiles embed.FS

// Service 管理服务
type Service struct {
	server         *http.Server
	stateStore     mybot.StateStore
	port           string
	ctx            context.Context
	cancel         context.CancelFunc
	mu             sync.RWMutex
	enabled        bool
	configPath     string
	cronAPIURLs    []string
	logPath        string
}

// Config 管理服务配置
type Config struct {
	Enabled     bool     `yaml:"enabled"`
	Port        string   `yaml:"port"`
	ConfigPath  string   `yaml:"config_path"`
	CronAPIURLs []string `yaml:"cron_api_urls"`
	LogPath     string   `yaml:"log_path"`
}

// NewService 创建新的管理服务
func NewService(config Config, stateStore mybot.StateStore) *Service {
	if !config.Enabled {
		return &Service{enabled: false}
	}

	if config.Port == "" {
		config.Port = "8081" // 默认端口
	}

	return &Service{
		stateStore:  stateStore,
		port:        config.Port,
		enabled:     true,
		configPath:  config.ConfigPath,
		cronAPIURLs: config.CronAPIURLs,
		logPath:     config.LogPath,
	}
}

// Start 启动管理服务
func (s *Service) Start(ctx context.Context) error {
	if !s.enabled {
		slog.Info("Admin service is disabled")
		return nil
	}

	s.ctx, s.cancel = context.WithCancel(ctx)

	// 设置路由
	mux := http.NewServeMux()
	mux.HandleFunc("/", s.handleIndex)
	mux.HandleFunc("/config", s.handleIndex)
	mux.HandleFunc("/cron", s.handleIndex)
	mux.HandleFunc("/logs", s.handleIndex)
	mux.HandleFunc("/api/messages", s.handleListMessages)
	mux.HandleFunc("/api/stats", s.handleGetStats)
	mux.HandleFunc("/api/config", s.handleConfigAPI)
	mux.HandleFunc("/api/config/structured", s.handleGetConfigStructured)
	mux.HandleFunc("/api/cron/adapters", s.handleCronAdapters)
	mux.HandleFunc("/api/cron/jobs", s.handleCronJobsProxy)
	mux.HandleFunc("/api/logs", s.handleGetLogs)
	mux.HandleFunc("/api/heartbeat", s.handleHeartbeatState)
	mux.HandleFunc("/api/heartbeat/feedback", s.handleHeartbeatFeedback)
	mux.HandleFunc("/health", s.handleHealth)

	s.server = &http.Server{
		Addr:    ":" + s.port,
		Handler: mux,
	}

	slog.Info("Starting Admin service", "port", s.port)

	go func() {
		if err := s.server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.Error("Admin server error", "err", err)
		}
	}()

	// 启动后默认自动打开浏览器
	go s.openBrowserAfterStart()

	return nil
}

// openBrowserAfterStart 延迟后打开默认浏览器
func (s *Service) openBrowserAfterStart() {
	time.Sleep(1500 * time.Millisecond)
	url := "http://127.0.0.1:" + s.port
	var cmd *exec.Cmd
	switch runtime.GOOS {
	case "darwin":
		cmd = exec.Command("open", url)
	case "windows":
		cmd = exec.Command("cmd", "/c", "start", url)
	default:
		cmd = exec.Command("xdg-open", url)
	}
	if err := cmd.Start(); err != nil {
		slog.Debug("Admin: open browser failed", "url", url, "err", err)
	}
}

// Stop 停止管理服务
func (s *Service) Stop() error {
	if !s.enabled {
		return nil
	}

	if s.cancel != nil {
		s.cancel()
	}

	if s.server != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		if err := s.server.Shutdown(ctx); err != nil {
			return fmt.Errorf("failed to shutdown admin server: %w", err)
		}
	}

	slog.Info("Admin service stopped")
	return nil
}

// handleIndex 处理首页及 /config、/cron、/logs（SPA 同页）
func (s *Service) handleIndex(rw http.ResponseWriter, r *http.Request) {
	p := r.URL.Path
	if p != "/" && p != "/config" && p != "/cron" && p != "/logs" {
		http.NotFound(rw, r)
		return
	}

	content, err := staticFiles.ReadFile("static/index.html")
	if err != nil {
		http.Error(rw, "File not found", http.StatusNotFound)
		slog.Error("Failed to read index.html", "err", err)
		return
	}

	rw.Header().Set("Content-Type", "text/html; charset=utf-8")
	rw.Write(content)
}

// handleListMessages 处理消息列表查询
func (s *Service) handleListMessages(rw http.ResponseWriter, r *http.Request) {
	if s.stateStore == nil {
		http.Error(rw, "StateStore not available", http.StatusServiceUnavailable)
		return
	}

	// 解析查询参数
	filter := mybot.MessageFilter{
		SourceAdapter: r.URL.Query().Get("source_adapter"),
		TargetAdapter: r.URL.Query().Get("target_adapter"),
		UserID:        r.URL.Query().Get("user_id"),
		Channel:       r.URL.Query().Get("channel"),
		Type:          r.URL.Query().Get("type"),
	}

	// 解析时间范围
	if startTime := r.URL.Query().Get("start_time"); startTime != "" {
		if ts, err := strconv.ParseInt(startTime, 10, 64); err == nil {
			filter.StartTime = ts
		}
	}
	if endTime := r.URL.Query().Get("end_time"); endTime != "" {
		if ts, err := strconv.ParseInt(endTime, 10, 64); err == nil {
			filter.EndTime = ts
		}
	}

	// 解析分页参数
	if limit := r.URL.Query().Get("limit"); limit != "" {
		if l, err := strconv.Atoi(limit); err == nil && l > 0 {
			filter.Limit = l
		}
	}
	if offset := r.URL.Query().Get("offset"); offset != "" {
		if o, err := strconv.Atoi(offset); err == nil && o >= 0 {
			filter.Offset = o
		}
	}

	// 默认限制
	if filter.Limit == 0 {
		filter.Limit = 50
	}
	if filter.Limit > 100 {
		filter.Limit = 100 // 最多100条
	}

	// 查询消息
	messages, err := s.stateStore.QueryMessages(filter)
	if err != nil {
		slog.Error("Failed to query messages", "err", err)
		http.Error(rw, "Failed to query messages: "+err.Error(), http.StatusInternalServerError)
		return
	}

	rw.Header().Set("Content-Type", "application/json")
	json.NewEncoder(rw).Encode(map[string]interface{}{
		"messages": messages,
		"count":    len(messages),
		"filter":   filter,
	})
}

// handleGetStats 处理统计信息查询
func (s *Service) handleGetStats(rw http.ResponseWriter, r *http.Request) {
	if s.stateStore == nil {
		http.Error(rw, "StateStore not available", http.StatusServiceUnavailable)
		return
	}

	stats, err := s.stateStore.GetStats()
	if err != nil {
		slog.Error("Failed to get stats", "err", err)
		http.Error(rw, "Failed to get stats: "+err.Error(), http.StatusInternalServerError)
		return
	}

	rw.Header().Set("Content-Type", "application/json")
	json.NewEncoder(rw).Encode(stats)
}

// handleHeartbeatState 处理心跳状态只读查询（供 Admin 展示）
func (s *Service) handleHeartbeatState(rw http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(rw, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if s.stateStore == nil {
		rw.Header().Set("Content-Type", "application/json")
		json.NewEncoder(rw).Encode(map[string]interface{}{
			"available": false,
			"reason":    "StateStore not available",
		})
		return
	}
	hs, ok := s.stateStore.(mybot.HeartbeatStateStore)
	if !ok {
		rw.Header().Set("Content-Type", "application/json")
		json.NewEncoder(rw).Encode(map[string]interface{}{
			"available": false,
			"reason":    "StateStore does not support heartbeat state",
		})
		return
	}
	state, err := hs.GetHeartbeatState()
	if err != nil {
		slog.Error("Failed to get heartbeat state", "err", err)
		http.Error(rw, "Failed to get heartbeat state: "+err.Error(), http.StatusInternalServerError)
		return
	}
	rw.Header().Set("Content-Type", "application/json")
	json.NewEncoder(rw).Encode(map[string]interface{}{
		"available": true,
		"state":     state,
	})
}

// handleHeartbeatFeedback 处理「该 channel 本轮无处理」反馈，延长该 channel 下次心跳间隔
func (s *Service) handleHeartbeatFeedback(rw http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(rw, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if s.stateStore == nil {
		http.Error(rw, "StateStore not available", http.StatusServiceUnavailable)
		return
	}
	hs, ok := s.stateStore.(mybot.HeartbeatStateStore)
	if !ok {
		http.Error(rw, "heartbeat state not supported", http.StatusNotImplemented)
		return
	}
	var body struct {
		Channel string  `json:"channel"`
		UserID  string  `json:"user_id"`
		NoWork  bool    `json:"no_work"`
		Multiplier float64 `json:"multiplier"` // 可选，默认 2
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		http.Error(rw, "invalid json: "+err.Error(), http.StatusBadRequest)
		return
	}
	if !body.NoWork || body.Channel == "" {
		rw.Header().Set("Content-Type", "application/json")
		json.NewEncoder(rw).Encode(map[string]interface{}{"ok": false, "reason": "no_work must be true and channel required"})
		return
	}
	mult := body.Multiplier
	if mult <= 0 {
		mult = 2
	}
	if err := hs.LengthenChannelInterval(body.Channel, body.UserID, mult); err != nil {
		slog.Error("heartbeat feedback: lengthen interval failed", "channel", body.Channel, "err", err)
		http.Error(rw, err.Error(), http.StatusInternalServerError)
		return
	}
	rw.Header().Set("Content-Type", "application/json")
	json.NewEncoder(rw).Encode(map[string]interface{}{"ok": true})
}

// handleHealth 处理健康检查
func (s *Service) handleHealth(rw http.ResponseWriter, r *http.Request) {
	rw.Header().Set("Content-Type", "application/json")
	json.NewEncoder(rw).Encode(map[string]interface{}{
		"status":  "ok",
		"port":    s.port,
		"enabled": s.enabled,
	})
}

// resolveConfigPath 返回用于只读展示的配置文件路径；未显式配置时尝试环境变量或当前目录常见文件名
func (s *Service) resolveConfigPath() string {
	if s.configPath != "" {
		return s.configPath
	}
	if p := os.Getenv("MYBOT_CONFIG"); p != "" {
		return p
	}
	if p := os.Getenv("MYBOT_CONFIG_PATH"); p != "" {
		return p
	}
	// 常见相对路径，按工作目录尝试
	for _, name := range []string{"config.yaml", "config.yml", "./config.yaml", "./config.yml"} {
		if _, err := os.Stat(name); err == nil {
			if abs, err := filepath.Abs(name); err == nil {
				return abs
			}
			return name
		}
	}
	return ""
}

// handleConfigAPI 处理 GET（返回原始内容）与 PATCH（合并并写回）
func (s *Service) handleConfigAPI(rw http.ResponseWriter, r *http.Request) {
	path := s.resolveConfigPath()
	if path == "" {
		if r.Method == http.MethodPatch {
			http.Error(rw, "config path not available, cannot save", http.StatusBadRequest)
			return
		}
		rw.Header().Set("Content-Type", "application/json")
		json.NewEncoder(rw).Encode(map[string]interface{}{
			"content": "", "path": "",
			"hint": "未配置 config_path，且未设置 MYBOT_CONFIG/MYBOT_CONFIG_PATH，当前目录下也未找到 config.yaml",
		})
		return
	}
	switch r.Method {
	case http.MethodGet:
		data, err := os.ReadFile(path)
		if err != nil {
			http.Error(rw, "read config failed: "+err.Error(), http.StatusInternalServerError)
			return
		}
		rw.Header().Set("Content-Type", "application/json")
		json.NewEncoder(rw).Encode(map[string]interface{}{"content": string(data), "path": path})
		return
	case http.MethodPatch:
		s.handlePatchConfig(rw, r, path)
		return
	default:
		http.Error(rw, "method not allowed", http.StatusMethodNotAllowed)
	}
}

// handlePatchConfig 读取请求体 JSON，与当前配置合并后写回 YAML 文件
func (s *Service) handlePatchConfig(rw http.ResponseWriter, r *http.Request, path string) {
	var patch map[string]interface{}
	if err := json.NewDecoder(r.Body).Decode(&patch); err != nil {
		http.Error(rw, "invalid json: "+err.Error(), http.StatusBadRequest)
		return
	}
	data, err := os.ReadFile(path)
	if err != nil {
		http.Error(rw, "read config failed: "+err.Error(), http.StatusInternalServerError)
		return
	}
	var current map[string]interface{}
	if err := yaml.Unmarshal(data, &current); err != nil {
		http.Error(rw, "config is not valid yaml: "+err.Error(), http.StatusBadRequest)
		return
	}
	mergeMap(current, patch)
	out, err := yaml.Marshal(current)
	if err != nil {
		http.Error(rw, "marshal config failed: "+err.Error(), http.StatusInternalServerError)
		return
	}
	if err := os.WriteFile(path, out, 0o644); err != nil {
		http.Error(rw, "write config failed: "+err.Error(), http.StatusInternalServerError)
		return
	}
	rw.Header().Set("Content-Type", "application/json")
	json.NewEncoder(rw).Encode(map[string]interface{}{"ok": true, "path": path})
}

// mergeMap 将 src 的键值递归合并到 dst（同键时用 src 覆盖）；adapters 按索引合并以保留未编辑字段
func mergeMap(dst, src map[string]interface{}) {
	for k, v := range src {
		if v == nil {
			dst[k] = nil
			continue
		}
		if k == "adapters" {
			if srcSlice, ok := v.([]interface{}); ok {
				if dstSlice, ok := dst[k].([]interface{}); ok {
					for i := 0; i < len(srcSlice) && i < len(dstSlice); i++ {
						if sm, ok := srcSlice[i].(map[string]interface{}); ok {
							if dm, ok := dstSlice[i].(map[string]interface{}); ok {
								mergeMap(dm, sm)
							} else {
								dstSlice[i] = srcSlice[i]
							}
						}
					}
					continue
				}
			}
		}
		if srcMap, ok := v.(map[string]interface{}); ok {
			if dstMap, ok := dst[k].(map[string]interface{}); ok {
				mergeMap(dstMap, srcMap)
				continue
			}
		}
		dst[k] = v
	}
}

// handleGetConfigStructured 返回解析后的配置（供表单编辑）
func (s *Service) handleGetConfigStructured(rw http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(rw, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	path := s.resolveConfigPath()
	if path == "" {
		rw.Header().Set("Content-Type", "application/json")
		json.NewEncoder(rw).Encode(map[string]interface{}{"path": "", "config": nil})
		return
	}
	data, err := os.ReadFile(path)
	if err != nil {
		http.Error(rw, "read config failed: "+err.Error(), http.StatusInternalServerError)
		return
	}
	var cfg map[string]interface{}
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		http.Error(rw, "parse config failed: "+err.Error(), http.StatusInternalServerError)
		return
	}
	rw.Header().Set("Content-Type", "application/json")
	json.NewEncoder(rw).Encode(map[string]interface{}{"path": path, "config": cfg})
}

// handleCronAdapters 返回已配置的 Cron API 地址列表
func (s *Service) handleCronAdapters(rw http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(rw, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	urls := s.cronAPIURLs
	if urls == nil {
		urls = []string{}
	}
	rw.Header().Set("Content-Type", "application/json")
	json.NewEncoder(rw).Encode(map[string]interface{}{"adapters": urls})
}

// handleCronJobsProxy 代理请求到指定 Cron API 获取任务列表（前端传 adapter_url）
func (s *Service) handleCronJobsProxy(rw http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(rw, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	adapterURL := r.URL.Query().Get("adapter_url")
	if adapterURL == "" {
		http.Error(rw, "adapter_url required", http.StatusBadRequest)
		return
	}
	// 只允许请求配置中的 cron API
	allowed := false
	for _, u := range s.cronAPIURLs {
		if u == adapterURL {
			allowed = true
			break
		}
	}
	if !allowed {
		http.Error(rw, "adapter_url not in allowed list", http.StatusForbidden)
		return
	}
	targetURL := adapterURL
	if !strings.HasSuffix(strings.TrimSuffix(targetURL, "/"), "/cron/jobs") {
		targetURL = strings.TrimSuffix(targetURL, "/") + "/cron/jobs"
	}
	req, err := http.NewRequestWithContext(r.Context(), http.MethodGet, targetURL, nil)
	if err != nil {
		http.Error(rw, err.Error(), http.StatusBadRequest)
		return
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		http.Error(rw, "request cron api failed: "+err.Error(), http.StatusBadGateway)
		return
	}
	defer resp.Body.Close()
	for k, v := range resp.Header {
		for _, vv := range v {
			rw.Header().Add(k, vv)
		}
	}
	rw.WriteHeader(resp.StatusCode)
	_, _ = io.Copy(rw, resp.Body)
}

// handleGetLogs 返回日志文件最后 N 行
func (s *Service) handleGetLogs(rw http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(rw, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if s.logPath == "" {
		rw.Header().Set("Content-Type", "application/json")
		json.NewEncoder(rw).Encode(map[string]interface{}{"lines": []string{}, "path": ""})
		return
	}
	data, err := os.ReadFile(s.logPath)
	if err != nil {
		http.Error(rw, "read log failed: "+err.Error(), http.StatusInternalServerError)
		return
	}
	lines := splitLines(string(data))
	tail := 500
	if n := r.URL.Query().Get("tail"); n != "" {
		if v, err := strconv.Atoi(n); err == nil && v > 0 && v <= 5000 {
			tail = v
		}
	}
	start := 0
	if len(lines) > tail {
		start = len(lines) - tail
	}
	result := lines[start:]
	rw.Header().Set("Content-Type", "application/json")
	json.NewEncoder(rw).Encode(map[string]interface{}{
		"lines": result,
		"path":  s.logPath,
		"total": len(lines),
	})
}

func splitLines(s string) []string {
	var out []string
	var line []rune
	for _, r := range s {
		if r == '\n' {
			out = append(out, string(line))
			line = nil
			continue
		}
		line = append(line, r)
	}
	if len(line) > 0 {
		out = append(out, string(line))
	}
	return out
}
