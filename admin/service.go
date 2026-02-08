package admin

import (
	"context"
	"embed"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/lengzhao/mybot"
)

//go:embed static/*
var staticFiles embed.FS

// Service 管理服务
type Service struct {
	server     *http.Server
	stateStore mybot.StateStore
	port       string
	ctx        context.Context
	cancel     context.CancelFunc
	mu         sync.RWMutex
	enabled    bool
}

// Config 管理服务配置
type Config struct {
	Enabled bool   `yaml:"enabled"`
	Port    string `yaml:"port"`
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
		stateStore: stateStore,
		port:       config.Port,
		enabled:    true,
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
	mux.HandleFunc("/api/messages", s.handleListMessages)
	mux.HandleFunc("/api/stats", s.handleGetStats)
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

	return nil
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

// handleIndex 处理首页
func (s *Service) handleIndex(rw http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		http.NotFound(rw, r)
		return
	}

	// 从嵌入的文件系统中读取index.html
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
	if filter.Limit > 1000 {
		filter.Limit = 1000 // 最大1000条
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

// handleHealth 处理健康检查
func (s *Service) handleHealth(rw http.ResponseWriter, r *http.Request) {
	rw.Header().Set("Content-Type", "application/json")
	json.NewEncoder(rw).Encode(map[string]interface{}{
		"status":  "ok",
		"port":    s.port,
		"enabled": s.enabled,
	})
}
