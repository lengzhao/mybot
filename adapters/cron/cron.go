package cron

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/google/uuid"
	mybot "github.com/lengzhao/mybot"
	"github.com/robfig/cron/v3"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

func init() {
	mybot.RegisterAdapterType("cron", func(id string, config map[string]interface{}) (mybot.Adapter, error) {
		return NewAdapter(id, config)
	})
}

const statusActive = "active"
const statusCancelled = "cancelled"

// cronJobModel GORM 模型，对应表 cron_jobs
type cronJobModel struct {
	ID        string `gorm:"primaryKey"`
	Schedule  string `gorm:"not null"`
	Payload   string `gorm:"type:text;not null"` // JSON
	ReplyTo   string `gorm:"type:text;not null"` // JSON
	Status    string `gorm:"index;default:active"`
	CreatedAt int64
	LastRunAt int64
	RunCount  int64
}

func (cronJobModel) TableName() string { return "cron_jobs" }

// Adapter Cron 定时任务适配器：接收 Message/HTTP 创建任务，按 schedule 触发时通过 inbound 投递回 Dispatcher（与其它 adapter 一致）。
type Adapter struct {
	id            string
	defaultTarget string

	inbound chan<- mybot.Message

	directory string // adapter_dir，用于默认 cron 数据库路径

	apiAddr  string // 如 ":9090" 或 "127.0.0.1:9090"
	apiToken string

	dbPath string
	db     *gorm.DB
	dbMu   sync.Mutex

	cron      *cron.Cron
	entryByID map[string]cron.EntryID // job_id -> cron entry id，用于取消
	cronMu    sync.Mutex

	httpServer *http.Server
}

// NewAdapter 从 config 创建 Cron adapter。触发时通过 Start 注入的 inbound 投递消息，无需主程序额外注入。
func NewAdapter(id string, config map[string]interface{}) (*Adapter, error) {
	defaultTarget, _ := config["default_target"].(string)
	directory, _ := config["adapter_dir"].(string)
	dbPath, _ := config["cron_db_path"].(string)
	if dbPath == "" && directory != "" {
		dbPath = filepath.Join(directory, "cron.db")
	}
	if dbPath == "" {
		dbPath = filepath.Join(os.TempDir(), "mybot_cron_"+id+".db")
	}

	apiAddr, _ := config["api_addr"].(string)
	apiToken, _ := config["api_token"].(string)

	return &Adapter{
		id:            id,
		defaultTarget: defaultTarget,
		directory:     directory,
		apiAddr:       apiAddr,
		apiToken:      apiToken,
		dbPath:        dbPath,
		entryByID:     make(map[string]cron.EntryID),
	}, nil
}

func (a *Adapter) GetID() string            { return "cron." + a.id }
func (a *Adapter) GetDefaultTarget() string { return a.defaultTarget }

func (a *Adapter) Start(ctx context.Context, inbound chan<- mybot.Message) error {
	a.inbound = inbound

	if err := a.openDB(); err != nil {
		return err
	}

	a.cron = cron.New()
	a.cron.Start()
	// 从 DB 加载 active 任务并加入调度
	var jobs []cronJobModel
	if err := a.db.Where("status = ?", statusActive).Find(&jobs).Error; err != nil {
		slog.Error("cron: load jobs failed", "adapter", a.id, "err", err)
	} else {
		for _, j := range jobs {
			a.scheduleJob(j)
		}
	}

	if a.apiAddr != "" {
		a.httpServer = &http.Server{Addr: a.apiAddr, Handler: a.apiHandler()}
		go func() {
			slog.Info("cron: API listening", "adapter", a.id, "addr", a.apiAddr)
			if err := a.httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
				slog.Error("cron: API server error", "adapter", a.id, "err", err)
			}
		}()
	}

	return nil
}

func (a *Adapter) openDB() error {
	a.dbMu.Lock()
	defer a.dbMu.Unlock()
	if a.db != nil {
		return nil
	}
	dir := filepath.Dir(a.dbPath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("cron: create db dir: %w", err)
	}
	db, err := gorm.Open(sqlite.Open(a.dbPath), &gorm.Config{})
	if err != nil {
		return fmt.Errorf("cron: open db: %w", err)
	}
	if err := db.AutoMigrate(&cronJobModel{}); err != nil {
		return fmt.Errorf("cron: migrate: %w", err)
	}
	a.db = db
	return nil
}

// scheduleJob 将任务加入 robfig/cron 调度（不落库，调用方已保证 DB 有记录）。
func (a *Adapter) scheduleJob(j cronJobModel) {
	jobID := j.ID
	a.cronMu.Lock()
	defer a.cronMu.Unlock()
	eid, err := a.cron.AddFunc(j.Schedule, func() { a.triggerJob(jobID) })
	if err != nil {
		slog.Warn("cron: add func failed", "job_id", jobID, "schedule", j.Schedule, "err", err)
		return
	}
	a.entryByID[jobID] = eid
}

func (a *Adapter) triggerJob(jobID string) {
	a.dbMu.Lock()
	db := a.db
	a.dbMu.Unlock()
	if db == nil {
		return
	}
	var j cronJobModel
	if err := db.Where("id = ? AND status = ?", jobID, statusActive).First(&j).Error; err != nil {
		slog.Debug("cron: job not found or not active", "job_id", jobID)
		return
	}

	var replyTo replyToSpec
	if err := json.Unmarshal([]byte(j.ReplyTo), &replyTo); err != nil {
		slog.Error("cron: unmarshal reply_to failed", "job_id", jobID, "err", err)
		return
	}
	var payload payloadSpec
	if err := json.Unmarshal([]byte(j.Payload), &payload); err != nil {
		slog.Error("cron: unmarshal payload failed", "job_id", jobID, "err", err)
		return
	}

	msg := a.buildMessage(replyTo, payload)
	if a.inbound == nil {
		slog.Warn("cron: inbound not set, drop triggered message", "job_id", jobID)
		return
	}
	select {
	case a.inbound <- msg:
	default:
		slog.Warn("cron: inbound full, drop triggered message", "job_id", jobID)
	}

	// 更新 last_run_at, run_count
	now := time.Now().UnixMilli()
	db.Model(&cronJobModel{}).Where("id = ?", jobID).Updates(map[string]interface{}{
		"last_run_at": now,
		"run_count":   gorm.Expr("run_count + 1"),
	})
}

type replyToSpec struct {
	SourceAdapter string `json:"source_adapter"`
	UserID        string `json:"user_id"`
	Channel       string `json:"channel"`
	ParentID      string `json:"parent_id,omitempty"`
}

type payloadSpec struct {
	TargetAdapter string                 `json:"target_adapter"`
	Content       string                 `json:"content"`
	Type          string                 `json:"type"`
	Files         []mybot.File           `json:"files,omitempty"`
	Extra         map[string]interface{} `json:"extra,omitempty"`
}

func (a *Adapter) buildMessage(replyTo replyToSpec, payload payloadSpec) mybot.Message {
	msgType := mybot.TypeText
	if payload.Type != "" {
		msgType = mybot.MessageType(payload.Type)
	}
	return mybot.Message{
		ID:            "cron-" + uuid.New().String(),
		ParentID:      replyTo.ParentID,
		SourceAdapter: replyTo.SourceAdapter,
		TargetAdapter: payload.TargetAdapter,
		UserID:        replyTo.UserID,
		Channel:       replyTo.Channel,
		Content:       payload.Content,
		Type:          msgType,
		Timestamp:     time.Now().UnixMilli(),
		Files:         payload.Files,
		Extra:         payload.Extra,
	}
}

// ReceiveMessage 处理发给本 adapter 的消息：解析 Extra 创建定时任务并回复确认。
func (a *Adapter) ReceiveMessage(ctx context.Context, msg mybot.Message) error {
	if msg.TargetAdapter != "" && msg.TargetAdapter != a.id {
		return nil
	}

	extra := msg.Extra
	if extra == nil {
		return nil
	}
	action, _ := extra["cron_action"].(string)
	if action != "create" {
		return nil
	}

	schedule, _ := extra["schedule"].(string)
	if schedule == "" {
		a.sendReply(msg, "创建失败：缺少 schedule（cron 表达式）")
		return nil
	}

	payloadMap, _ := extra["payload"].(map[string]interface{})
	if payloadMap == nil {
		a.sendReply(msg, "创建失败：缺少 payload")
		return nil
	}
	payload := a.extraToPayload(payloadMap)
	if payload.TargetAdapter == "" {
		a.sendReply(msg, "创建失败：payload 缺少 target_adapter")
		return nil
	}

	replyTo := replyToSpec{
		SourceAdapter: msg.SourceAdapter,
		UserID:        msg.UserID,
		Channel:       msg.Channel,
		ParentID:      msg.ParentID,
	}
	if rt, ok := extra["reply_to"].(map[string]interface{}); ok {
		if v, _ := rt["source_adapter"].(string); v != "" {
			replyTo.SourceAdapter = v
		}
		if v, _ := rt["user_id"].(string); v != "" {
			replyTo.UserID = v
		}
		if v, _ := rt["channel"].(string); v != "" {
			replyTo.Channel = v
		}
		if v, _ := rt["parent_id"].(string); v != "" {
			replyTo.ParentID = v
		}
	}

	// 校验 cron 表达式（5 段：分 时 日 月 周）
	c := cron.New()
	_, err := c.AddFunc(schedule, func() {})
	if err != nil {
		a.sendReply(msg, "创建失败：无效的 cron 表达式: "+err.Error())
		return nil
	}

	jobID := uuid.New().String()
	payloadJSON, _ := json.Marshal(payload)
	replyToJSON, _ := json.Marshal(replyTo)

	a.dbMu.Lock()
	db := a.db
	a.dbMu.Unlock()
	if db == nil {
		a.sendReply(msg, "创建失败：存储未就绪")
		return nil
	}

	job := cronJobModel{
		ID:        jobID,
		Schedule:  schedule,
		Payload:   string(payloadJSON),
		ReplyTo:   string(replyToJSON),
		Status:    statusActive,
		CreatedAt: time.Now().UnixMilli(),
	}
	if err := db.Create(&job).Error; err != nil {
		slog.Error("cron: create job failed", "err", err)
		a.sendReply(msg, "创建失败：写入存储错误")
		return nil
	}

	a.scheduleJob(job)
	a.sendReply(msg, fmt.Sprintf("已创建定时任务，job_id=%s，schedule=%s", jobID, schedule))
	return nil
}

func (a *Adapter) extraToPayload(m map[string]interface{}) payloadSpec {
	var p payloadSpec
	if v, _ := m["target_adapter"].(string); v != "" {
		p.TargetAdapter = v
	}
	if v, _ := m["content"].(string); v != "" {
		p.Content = v
	}
	if v, _ := m["type"].(string); v != "" {
		p.Type = v
	}
	if files, ok := m["files"].([]interface{}); ok {
		for _, f := range files {
			if fm, ok := f.(map[string]interface{}); ok {
				var file mybot.File
				if v, _ := fm["name"].(string); v != "" {
					file.Name = v
				}
				if v, _ := fm["url"].(string); v != "" {
					file.URL = v
				}
				if v, _ := fm["mime_type"].(string); v != "" {
					file.MimeType = v
				}
				p.Files = append(p.Files, file)
			}
		}
	}
	if extra, ok := m["extra"].(map[string]interface{}); ok {
		p.Extra = extra
	}
	return p
}

func (a *Adapter) sendReply(orig mybot.Message, content string) {
	target := orig.SourceAdapter
	if target == "" {
		target = a.defaultTarget
	}
	reply := mybot.Message{
		ID:            "cron-reply-" + uuid.New().String(),
		ParentID:      orig.ID,
		SourceAdapter: a.id,
		TargetAdapter: target,
		UserID:        orig.UserID,
		Channel:       orig.Channel,
		Content:       content,
		Type:          mybot.TypeText,
		Timestamp:     time.Now().UnixMilli(),
	}
	select {
	case a.inbound <- reply:
	default:
		slog.Warn("cron: send reply blocked", "target", target)
	}
}

func (a *Adapter) Status() string {
	if a.db == nil {
		return "not_started"
	}
	return "running"
}

// API 与存储方法供 HTTP handler 使用
func (a *Adapter) createJobFromAPI(schedule string, payload payloadSpec, replyTo replyToSpec) (jobID string, err error) {
	c := cron.New()
	if _, err = c.AddFunc(schedule, func() {}); err != nil {
		return "", err
	}
	jobID = uuid.New().String()
	payloadJSON, _ := json.Marshal(payload)
	replyToJSON, _ := json.Marshal(replyTo)
	a.dbMu.Lock()
	db := a.db
	a.dbMu.Unlock()
	if db == nil {
		return "", fmt.Errorf("store not ready")
	}
	job := cronJobModel{
		ID:        jobID,
		Schedule:  schedule,
		Payload:   string(payloadJSON),
		ReplyTo:   string(replyToJSON),
		Status:    statusActive,
		CreatedAt: time.Now().UnixMilli(),
	}
	if err = db.Create(&job).Error; err != nil {
		return "", err
	}
	a.scheduleJob(job)
	return jobID, nil
}

func (a *Adapter) getJob(jobID string) (*cronJobModel, error) {
	a.dbMu.Lock()
	db := a.db
	a.dbMu.Unlock()
	if db == nil {
		return nil, fmt.Errorf("store not ready")
	}
	var j cronJobModel
	if err := db.Where("id = ?", jobID).First(&j).Error; err != nil {
		return nil, err
	}
	return &j, nil
}

func (a *Adapter) listJobs(status string) ([]cronJobModel, error) {
	a.dbMu.Lock()
	db := a.db
	a.dbMu.Unlock()
	if db == nil {
		return nil, fmt.Errorf("store not ready")
	}
	var jobs []cronJobModel
	q := db.Model(&cronJobModel{})
	if status != "" {
		q = q.Where("status = ?", status)
	}
	if err := q.Order("created_at DESC").Find(&jobs).Error; err != nil {
		return nil, err
	}
	return jobs, nil
}

func (a *Adapter) cancelJob(jobID string) error {
	a.dbMu.Lock()
	db := a.db
	a.dbMu.Unlock()
	if db == nil {
		return fmt.Errorf("store not ready")
	}
	res := db.Model(&cronJobModel{}).Where("id = ? AND status = ?", jobID, statusActive).Update("status", statusCancelled)
	if res.Error != nil {
		return res.Error
	}
	if res.RowsAffected == 0 {
		return fmt.Errorf("job not found or already cancelled")
	}
	a.cronMu.Lock()
	if eid, ok := a.entryByID[jobID]; ok {
		a.cron.Remove(eid)
		delete(a.entryByID, jobID)
	}
	a.cronMu.Unlock()
	return nil
}

// apiHandler 返回 Cron HTTP API 的 handler（POST/GET/DELETE /cron/jobs）。
func (a *Adapter) apiHandler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/cron/jobs", a.handleJobsListOrCreate)
	mux.HandleFunc("/cron/jobs/", a.handleJobGetOrCancel)
	return a.authMiddleware(mux)
}

func (a *Adapter) authMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if a.apiToken != "" {
			token := r.Header.Get("Authorization")
			if len(token) > 7 && token[:7] == "Bearer " {
				token = token[7:]
			}
			if token == "" {
				token = r.Header.Get("X-API-Key")
			}
			if token != a.apiToken {
				http.Error(w, `{"error":"unauthorized"}`, http.StatusUnauthorized)
				return
			}
		}
		next.ServeHTTP(w, r)
	})
}

func (a *Adapter) handleJobsListOrCreate(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		a.handleListJobs(w, r)
	case http.MethodPost:
		a.handleCreateJob(w, r)
	default:
		http.Error(w, `{"error":"method not allowed"}`, http.StatusMethodNotAllowed)
	}
}

func (a *Adapter) handleListJobs(w http.ResponseWriter, r *http.Request) {
	status := r.URL.Query().Get("status")
	jobs, err := a.listJobs(status)
	if err != nil {
		http.Error(w, `{"error":"`+err.Error()+`"}`, http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	enc := json.NewEncoder(w)
	enc.Encode(map[string]interface{}{"jobs": jobs, "count": len(jobs)})
}

type createJobRequest struct {
	Schedule string      `json:"schedule"`
	Payload  payloadSpec `json:"payload"`
	ReplyTo  replyToSpec `json:"reply_to"`
}

func (a *Adapter) handleCreateJob(w http.ResponseWriter, r *http.Request) {
	var req createJobRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, `{"error":"invalid json"}`, http.StatusBadRequest)
		return
	}
	if req.Schedule == "" || req.Payload.TargetAdapter == "" || req.ReplyTo.SourceAdapter == "" {
		http.Error(w, `{"error":"schedule, payload.target_adapter, reply_to.source_adapter required"}`, http.StatusBadRequest)
		return
	}
	jobID, err := a.createJobFromAPI(req.Schedule, req.Payload, req.ReplyTo)
	if err != nil {
		http.Error(w, `{"error":"`+err.Error()+`"}`, http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(map[string]string{"job_id": jobID, "schedule": req.Schedule, "message": "created"})
}

func (a *Adapter) handleJobGetOrCancel(w http.ResponseWriter, r *http.Request) {
	// /cron/jobs/{id} 或 /cron/jobs/{id}/cancel
	path := r.URL.Path
	if len(path) <= len("/cron/jobs/") {
		http.NotFound(w, r)
		return
	}
	path = path[len("/cron/jobs/"):]
	jobID := path
	if len(path) > 7 && path[len(path)-7:] == "/cancel" {
		jobID = path[:len(path)-7]
		if r.Method != http.MethodPost {
			http.Error(w, `{"error":"method not allowed"}`, http.StatusMethodNotAllowed)
			return
		}
		if err := a.cancelJob(jobID); err != nil {
			http.Error(w, `{"error":"`+err.Error()+`"}`, http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{"job_id": jobID, "message": "cancelled"})
		return
	}

	if r.Method == http.MethodGet {
		j, err := a.getJob(jobID)
		if err != nil {
			http.Error(w, `{"error":"`+err.Error()+`"}`, http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(j)
		return
	}
	if r.Method == http.MethodDelete {
		if err := a.cancelJob(jobID); err != nil {
			http.Error(w, `{"error":"`+err.Error()+`"}`, http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{"job_id": jobID, "message": "cancelled"})
		return
	}
	http.Error(w, `{"error":"method not allowed"}`, http.StatusMethodNotAllowed)
}
