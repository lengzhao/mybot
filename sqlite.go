package mybot

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"time"

	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

// MessageModel GORM 消息模型
type MessageModel struct {
	ID            string `gorm:"primaryKey" json:"id"`
	ParentID      string `gorm:"index" json:"parent_id"`
	SourceAdapter string `gorm:"index" json:"source_adapter"`
	TargetAdapter string `gorm:"index" json:"target_adapter"`
	UserID        string `gorm:"index" json:"user_id"`
	Channel       string `gorm:"index" json:"channel"`
	Content       string `json:"content"`
	Type          string `gorm:"index" json:"type"`
	Timestamp     int64  `gorm:"index" json:"timestamp"`
	Files         string `json:"files"` // JSON 格式存储
	Extra         string `json:"extra"` // JSON 格式存储
	ProcessedAt   int64  `gorm:"index" json:"processed_at"`
}

// TableName 指定表名
func (MessageModel) TableName() string {
	return "messages"
}

// HeartbeatStateModel 心跳状态表（单行，与消息日志分离）
type HeartbeatStateModel struct {
	ID            string `gorm:"primaryKey"` // 固定为 "default"
	LastTriggerAt int64  `json:"last_trigger_at"`
	TriggerCount  int64  `json:"trigger_count"`
	NextDueAt     int64  `json:"next_due_at"`
	Enabled       bool   `json:"enabled"`
	UpdatedAt     int64  `json:"updated_at"`
}

func (HeartbeatStateModel) TableName() string {
	return "heartbeat_state"
}

// HeartbeatChannelStateModel 各 channel 的心跳调度状态（历史对话 channel 按 due 发送，支持延长间隔与取消）
type HeartbeatChannelStateModel struct {
	Channel       string `gorm:"primaryKey" json:"channel"`
	UserID        string `gorm:"primaryKey" json:"user_id"`
	LastTriggerAt int64  `json:"last_trigger_at"`
	NextDueAt     int64  `json:"next_due_at"`
	IntervalSec   int    `json:"interval_sec"`
	CancelledAt   int64  `json:"cancelled_at"` // >0 表示已取消心跳，有新消息时清零恢复
	UpdatedAt     int64  `json:"updated_at"`
}

func (HeartbeatChannelStateModel) TableName() string {
	return "heartbeat_channel_state"
}

// SQLiteStore SQLite存储实现
type SQLiteStore struct {
	db          *gorm.DB
	dbPath      string
	ctx         context.Context
	cancel      context.CancelFunc
	mu          sync.RWMutex
	enabled     bool
	autoCleanup bool
	cleanupDays int
}

// NewSQLiteStore 创建新的SQLite存储实例
func NewSQLiteStore(config StateStoreConfig) (*SQLiteStore, error) {
	if !config.Enabled {
		return &SQLiteStore{enabled: false}, nil
	}

	// 确保数据库目录存在
	dbDir := filepath.Dir(config.DBPath)
	if err := os.MkdirAll(dbDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create database directory: %w", err)
	}

	// 连接数据库
	db, err := gorm.Open(sqlite.Open(config.DBPath), &gorm.Config{})
	if err != nil {
		return nil, fmt.Errorf("failed to open database: %w", err)
	}

	store := &SQLiteStore{
		db:          db,
		dbPath:      config.DBPath,
		enabled:     true,
		autoCleanup: config.AutoCleanup,
		cleanupDays: config.CleanupDays,
	}

	return store, nil
}

// Start 启动存储服务
func (s *SQLiteStore) Start() error {
	if !s.enabled {
		slog.Info("StateStore is disabled")
		return nil
	}

	s.ctx, s.cancel = context.WithCancel(context.Background())

	// 自动迁移表结构
	if err := s.db.AutoMigrate(&MessageModel{}, &HeartbeatStateModel{}, &HeartbeatChannelStateModel{}); err != nil {
		return fmt.Errorf("failed to migrate database: %w", err)
	}

	// 启动自动清理任务
	if s.autoCleanup && s.cleanupDays > 0 {
		go s.cleanupLoop()
	}

	slog.Info("SQLite StateStore started", "db_path", s.dbPath)
	return nil
}

// Stop 停止存储服务
func (s *SQLiteStore) Stop() error {
	if !s.enabled {
		return nil
	}

	if s.cancel != nil {
		s.cancel()
	}

	// GORM 会自动管理连接，这里不需要手动关闭

	slog.Info("SQLite StateStore stopped")
	return nil
}

// RecordMessage 记录消息
func (s *SQLiteStore) RecordMessage(msg Message) error {
	if !s.enabled {
		return nil
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	// 序列化复杂字段
	filesJSON, err := json.Marshal(msg.Files)
	if err != nil {
		return fmt.Errorf("failed to marshal files: %w", err)
	}

	extraJSON, err := json.Marshal(msg.Extra)
	if err != nil {
		return fmt.Errorf("failed to marshal extra: %w", err)
	}

	// 创建消息模型
	messageModel := MessageModel{
		ID:            msg.ID,
		ParentID:      msg.ParentID,
		SourceAdapter: msg.SourceAdapter,
		TargetAdapter: msg.TargetAdapter,
		UserID:        msg.UserID,
		Channel:       msg.Channel,
		Content:       msg.Content,
		Type:          string(msg.Type),
		Timestamp:     msg.Timestamp,
		Files:         string(filesJSON),
		Extra:         string(extraJSON),
		ProcessedAt:   time.Now().UnixMilli(),
	}

	// 使用 Create 操作
	if err := s.db.Create(&messageModel).Error; err != nil {
		return fmt.Errorf("failed to insert message: %w", err)
	}

	slog.Debug("Message recorded", "msg_id", msg.ID, "source", msg.SourceAdapter, "target", msg.TargetAdapter)
	return nil
}

// QueryMessages 查询消息历史
func (s *SQLiteStore) QueryMessages(filter MessageFilter) ([]MessageRecord, error) {
	if !s.enabled {
		return []MessageRecord{}, nil
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	var models []MessageModel
	query := s.db.Model(&MessageModel{})

	// 应用过滤条件
	if filter.SourceAdapter != "" {
		query = query.Where("source_adapter = ?", filter.SourceAdapter)
	}
	if filter.TargetAdapter != "" {
		query = query.Where("target_adapter = ?", filter.TargetAdapter)
	}
	if filter.UserID != "" {
		query = query.Where("user_id = ?", filter.UserID)
	}
	if filter.Channel != "" {
		query = query.Where("channel = ?", filter.Channel)
	}
	if filter.Type != "" {
		query = query.Where("type = ?", filter.Type)
	}
	if filter.StartTime > 0 {
		query = query.Where("timestamp >= ?", filter.StartTime)
	}
	if filter.EndTime > 0 {
		query = query.Where("timestamp <= ?", filter.EndTime)
	}

	// 排序和分页
	query = query.Order("timestamp DESC")

	if filter.Limit > 0 {
		query = query.Limit(filter.Limit)
	}
	if filter.Offset > 0 {
		query = query.Offset(filter.Offset)
	}

	// 执行查询
	if err := query.Find(&models).Error; err != nil {
		return nil, fmt.Errorf("failed to query messages: %w", err)
	}

	// 转换为 MessageRecord（内嵌 Message）
	var records []MessageRecord
	for _, model := range models {
		msg := Message{
			ID:            model.ID,
			ParentID:      model.ParentID,
			SourceAdapter: model.SourceAdapter,
			TargetAdapter: model.TargetAdapter,
			UserID:        model.UserID,
			Channel:       model.Channel,
			Content:       model.Content,
			Type:          MessageType(model.Type),
			Timestamp:     model.Timestamp,
		}
		if len(model.Files) > 0 {
			_ = json.Unmarshal([]byte(model.Files), &msg.Files)
		}
		if len(model.Extra) > 0 {
			_ = json.Unmarshal([]byte(model.Extra), &msg.Extra)
		}
		records = append(records, MessageRecord{Message: msg, ProcessedAt: model.ProcessedAt})
	}

	return records, nil
}

// GetStats 获取统计信息
func (s *SQLiteStore) GetStats() (Stats, error) {
	if !s.enabled {
		return Stats{}, nil
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	stats := Stats{
		MessagesByAdapter: make(map[string]int64),
		MessagesByUser:    make(map[string]int64),
		MessagesByChannel: make(map[string]int64),
		MessagesByType:    make(map[string]int64),
	}

	// 获取总消息数
	var total int64
	if err := s.db.Model(&MessageModel{}).Count(&total).Error; err != nil {
		return stats, fmt.Errorf("failed to get total messages count: %w", err)
	}
	stats.TotalMessages = total

	// 按适配器统计
	var adapterStats []struct {
		SourceAdapter string
		Count         int64
	}
	if err := s.db.Model(&MessageModel{}).
		Select("source_adapter, COUNT(*) as count").
		Group("source_adapter").
		Scan(&adapterStats).Error; err != nil {
		return stats, fmt.Errorf("failed to query adapter stats: %w", err)
	}
	for _, stat := range adapterStats {
		stats.MessagesByAdapter[stat.SourceAdapter] = stat.Count
	}

	// 按用户统计
	var userStats []struct {
		UserID string
		Count  int64
	}
	if err := s.db.Model(&MessageModel{}).
		Select("user_id, COUNT(*) as count").
		Group("user_id").
		Scan(&userStats).Error; err != nil {
		return stats, fmt.Errorf("failed to query user stats: %w", err)
	}
	for _, stat := range userStats {
		stats.MessagesByUser[stat.UserID] = stat.Count
	}

	// 按频道统计
	var channelStats []struct {
		Channel string
		Count   int64
	}
	if err := s.db.Model(&MessageModel{}).
		Select("channel, COUNT(*) as count").
		Group("channel").
		Scan(&channelStats).Error; err != nil {
		return stats, fmt.Errorf("failed to query channel stats: %w", err)
	}
	for _, stat := range channelStats {
		stats.MessagesByChannel[stat.Channel] = stat.Count
	}

	// 按类型统计
	var typeStats []struct {
		Type  string
		Count int64
	}
	if err := s.db.Model(&MessageModel{}).
		Select("type, COUNT(*) as count").
		Group("type").
		Scan(&typeStats).Error; err != nil {
		return stats, fmt.Errorf("failed to query type stats: %w", err)
	}
	for _, stat := range typeStats {
		stats.MessagesByType[stat.Type] = stat.Count
	}

	return stats, nil
}

const heartbeatStateID = "default"

// GetHeartbeatState 获取心跳状态（实现 HeartbeatStateStore）
func (s *SQLiteStore) GetHeartbeatState() (*HeartbeatState, error) {
	if !s.enabled {
		return nil, nil
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	var m HeartbeatStateModel
	err := s.db.Where("id = ?", heartbeatStateID).First(&m).Error
	if err != nil {
		if err == gorm.ErrRecordNotFound {
			return &HeartbeatState{Enabled: true}, nil
		}
		return nil, fmt.Errorf("failed to get heartbeat state: %w", err)
	}
	return &HeartbeatState{
		LastTriggerAt: m.LastTriggerAt,
		TriggerCount:  m.TriggerCount,
		NextDueAt:     m.NextDueAt,
		Enabled:       m.Enabled,
	}, nil
}

// UpdateHeartbeatState 更新心跳状态（实现 HeartbeatStateStore）
func (s *SQLiteStore) UpdateHeartbeatState(state HeartbeatState) error {
	if !s.enabled {
		return nil
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	now := time.Now().UnixMilli()
	m := HeartbeatStateModel{
		ID:            heartbeatStateID,
		LastTriggerAt: state.LastTriggerAt,
		TriggerCount:  state.TriggerCount,
		NextDueAt:     state.NextDueAt,
		Enabled:       state.Enabled,
		UpdatedAt:     now,
	}
	return s.db.Save(&m).Error
}

// GetActiveChannels 返回有历史对话的 channel 列表（排除心跳来源，用于按 channel 发送心跳）
func (s *SQLiteStore) GetActiveChannels(sinceTs int64) ([]ChannelInfo, error) {
	if !s.enabled {
		return nil, nil
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	query := s.db.Model(&MessageModel{}).
		Select("DISTINCT channel, user_id").
		Where("source_adapter != ?", "heartbeat").
		Where("channel != ?", "")
	if sinceTs > 0 {
		query = query.Where("timestamp >= ?", sinceTs)
	}
	var rows []struct {
		Channel string
		UserID  string
	}
	if err := query.Find(&rows).Error; err != nil {
		return nil, fmt.Errorf("get active channels: %w", err)
	}
	out := make([]ChannelInfo, 0, len(rows))
	for _, r := range rows {
		out = append(out, ChannelInfo{Channel: r.Channel, UserID: r.UserID})
	}
	return out, nil
}

// GetActiveChannelsForTarget 返回指定 target_adapter 下有历史对话的 channel（消息投递到该 adapter 的 channel），用于按 adapter 区分心跳
func (s *SQLiteStore) GetActiveChannelsForTarget(targetAdapter string, sinceTs int64) ([]ChannelInfo, error) {
	if !s.enabled || targetAdapter == "" {
		return nil, nil
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	query := s.db.Model(&MessageModel{}).
		Select("DISTINCT channel, user_id").
		Where("target_adapter = ?", targetAdapter).
		Where("source_adapter != ?", "heartbeat").
		Where("channel != ?", "")
	if sinceTs > 0 {
		query = query.Where("timestamp >= ?", sinceTs)
	}
	var rows []struct {
		Channel string
		UserID  string
	}
	if err := query.Find(&rows).Error; err != nil {
		return nil, fmt.Errorf("get active channels for target: %w", err)
	}
	out := make([]ChannelInfo, 0, len(rows))
	for _, r := range rows {
		out = append(out, ChannelInfo{Channel: r.Channel, UserID: r.UserID})
	}
	return out, nil
}

// GetChannelHeartbeatState 获取某 channel 的心跳调度状态
func (s *SQLiteStore) GetChannelHeartbeatState(channel, userID string) (*ChannelHeartbeatState, error) {
	if !s.enabled {
		return nil, nil
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	var m HeartbeatChannelStateModel
	err := s.db.Where("channel = ? AND user_id = ?", channel, userID).First(&m).Error
	if err != nil {
		if err == gorm.ErrRecordNotFound {
			return nil, nil
		}
		return nil, err
	}
	return &ChannelHeartbeatState{
		Channel:       m.Channel,
		UserID:        m.UserID,
		LastTriggerAt: m.LastTriggerAt,
		NextDueAt:     m.NextDueAt,
		IntervalSec:   m.IntervalSec,
		CancelledAt:   m.CancelledAt,
	}, nil
}

// UpsertChannelHeartbeatState 插入或更新 channel 心跳状态
func (s *SQLiteStore) UpsertChannelHeartbeatState(state ChannelHeartbeatState) error {
	if !s.enabled {
		return nil
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	now := time.Now().UnixMilli()
	m := HeartbeatChannelStateModel{
		Channel:       state.Channel,
		UserID:        state.UserID,
		LastTriggerAt: state.LastTriggerAt,
		NextDueAt:     state.NextDueAt,
		IntervalSec:   state.IntervalSec,
		CancelledAt:   state.CancelledAt,
		UpdatedAt:     now,
	}
	return s.db.Save(&m).Error
}

// MaxHeartbeatIntervalSec 单 channel 心跳间隔上限，超过后仍无处理则取消该 channel 心跳
const MaxHeartbeatIntervalSec = 86400 * 7 // 7 天

// LengthenChannelInterval 将该 channel 的下次间隔延长（乘 multiplier），有上限；已达上限则取消该 channel 心跳
func (s *SQLiteStore) LengthenChannelInterval(channel, userID string, multiplier float64) error {
	if !s.enabled {
		return nil
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	var m HeartbeatChannelStateModel
	err := s.db.Where("channel = ? AND user_id = ?", channel, userID).First(&m).Error
	if err != nil {
		if err == gorm.ErrRecordNotFound {
			return nil
		}
		return err
	}
	// 已达最长间隔仍无处理，视为 channel 可能已无用，取消心跳
	if m.IntervalSec >= MaxHeartbeatIntervalSec {
		m.CancelledAt = time.Now().UnixMilli()
		m.UpdatedAt = m.CancelledAt
		return s.db.Save(&m).Error
	}
	newInterval := int(float64(m.IntervalSec) * multiplier)
	if newInterval <= 0 {
		newInterval = m.IntervalSec * 2
	}
	if newInterval > MaxHeartbeatIntervalSec {
		newInterval = MaxHeartbeatIntervalSec
	}
	m.IntervalSec = newInterval
	m.NextDueAt = time.Now().UnixMilli() + int64(newInterval)*1000
	m.UpdatedAt = time.Now().UnixMilli()
	return s.db.Save(&m).Error
}

// UncancelChannelHeartbeat 该 channel 有新用户消息时恢复心跳（清除 cancelled_at）
func (s *SQLiteStore) UncancelChannelHeartbeat(channel, userID string) error {
	if !s.enabled {
		return nil
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.db.Model(&HeartbeatChannelStateModel{}).
		Where("channel = ? AND user_id = ? AND cancelled_at > 0", channel, userID).
		Updates(map[string]interface{}{
			"cancelled_at": 0,
			"updated_at":   time.Now().UnixMilli(),
		}).Error
}

// cleanupLoop 自动清理循环
func (s *SQLiteStore) cleanupLoop() {
	if s.cleanupDays <= 0 {
		return
	}

	ticker := time.NewTicker(24 * time.Hour) // 每天清理一次
	defer ticker.Stop()

	for {
		select {
		case <-s.ctx.Done():
			return
		case <-ticker.C:
			s.cleanupOldMessages()
		}
	}
}

// cleanupOldMessages 清理旧消息
func (s *SQLiteStore) cleanupOldMessages() {
	if s.cleanupDays <= 0 {
		return
	}

	cutoffTime := time.Now().AddDate(0, 0, -s.cleanupDays).UnixMilli()
	result := s.db.Where("timestamp < ?", cutoffTime).Delete(&MessageModel{})

	if result.Error != nil {
		slog.Error("failed to cleanup old messages", "err", result.Error)
		return
	}

	if result.RowsAffected > 0 {
		slog.Info("cleaned up old messages", "count", result.RowsAffected, "cutoff_time", cutoffTime)
	}
}
