package mybot

import (
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

// ConvLogDir 对话日志所在子目录（相对于 adapter 的 workdir）
const ConvLogDir = ".mybot"

// ConvLogFile 对话日志文件名
const ConvLogFile = "conversation.md"

// AppendExchange 将本次 request 与 response 追加到 workdir 下的对话日志文件中，供心跳等场景直接读取历史
func AppendExchange(workdir string, req, resp Message) error {
	if workdir == "" {
		return nil
	}
	dir := filepath.Join(workdir, ConvLogDir)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("conversation log mkdir: %w", err)
	}
	path := filepath.Join(dir, ConvLogFile)
	block := formatExchangeBlock(req, resp)
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return fmt.Errorf("conversation log open: %w", err)
	}
	defer f.Close()
	if _, err := f.WriteString(block); err != nil {
		return fmt.Errorf("conversation log write: %w", err)
	}
	slog.Debug("conversation log appended", "workdir", workdir, "req_id", req.ID)
	return nil
}

func formatExchangeBlock(req, resp Message) string {
	var b strings.Builder
	b.WriteString("\n\n---\n\n")
	b.WriteString("## Request ")
	b.WriteString(req.ID)
	b.WriteString("\n\n")
	b.WriteString("- **时间**: ")
	b.WriteString(strconv.FormatInt(req.Timestamp, 10))
	b.WriteString(" (ms) | **来源**: ")
	b.WriteString(req.SourceAdapter)
	b.WriteString(" | **user_id**: ")
	b.WriteString(req.UserID)
	b.WriteString("\n\n")
	b.WriteString(req.Content)
	b.WriteString("\n\n")
	b.WriteString("## Response ")
	b.WriteString(resp.ID)
	b.WriteString("\n\n")
	b.WriteString("- **时间**: ")
	b.WriteString(strconv.FormatInt(resp.Timestamp, 10))
	b.WriteString(" (ms) | **来源**: ")
	b.WriteString(resp.SourceAdapter)
	b.WriteString("\n\n")
	b.WriteString(resp.Content)
	b.WriteString("\n")
	return b.String()
}
