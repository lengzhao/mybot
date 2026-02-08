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
	if err := s.db.AutoMigrate(&MessageModel{}); err != nil {
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

	// 转换为 MessageRecord
	var records []MessageRecord
	for _, model := range models {
		record := MessageRecord{
			ID:            model.ID,
			SourceAdapter: model.SourceAdapter,
			TargetAdapter: model.TargetAdapter,
			UserID:        model.UserID,
			Channel:       model.Channel,
			Content:       model.Content,
			Type:          MessageType(model.Type),
			Timestamp:     model.Timestamp,
			ProcessedAt:   model.ProcessedAt,
		}

		// 反序列化复杂字段
		if len(model.Files) > 0 {
			if err := json.Unmarshal([]byte(model.Files), &record.Files); err != nil {
				slog.Warn("failed to unmarshal files", "err", err)
			}
		}

		if len(model.Extra) > 0 {
			if err := json.Unmarshal([]byte(model.Extra), &record.Extra); err != nil {
				slog.Warn("failed to unmarshal extra", "err", err)
			}
		}

		records = append(records, record)
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
