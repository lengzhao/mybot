package cursor

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/lengzhao/mybot"
)

const uploadsDir = "uploads"

func init() {
	mybot.RegisterAdapterType("cursor", func(id string, config map[string]interface{}) (mybot.Adapter, error) {
		slog.Debug("Creating Cursor Adapter", "id", id, "config", config)

		defaultTarget, _ := config["default_target"].(string)
		bin, _ := config["cursor_bin"].(string)
		if bin == "" {
			bin = "agent"
		}

		// CLI 全局参数：支持两种写法
		// - 列表：args: ["-f", "--approve-mcps"]
		// - 字符串：args: "-f --approve-mcps"
		var cliArgs []string
		if raw, ok := config["args"]; ok && raw != nil {
			switch v := raw.(type) {
			case []interface{}:
				for _, item := range v {
					s := fmt.Sprintf("%v", item)
					if strings.TrimSpace(s) != "" {
						cliArgs = append(cliArgs, s)
					}
				}
			case []string:
				for _, s := range v {
					if strings.TrimSpace(s) != "" {
						cliArgs = append(cliArgs, s)
					}
				}
			case string:
				for _, s := range strings.Fields(v) {
					if strings.TrimSpace(s) != "" {
						cliArgs = append(cliArgs, s)
					}
				}
			}
		}

		// 工作目录：优先显式 workdir，其次主程序注入的 adapter_dir
		workdir, _ := config["workdir"].(string)
		if workdir == "" {
			workdir, _ = config["adapter_dir"].(string)
		}
		if workdir == "" {
			return nil, fmt.Errorf("cursor adapter requires adapter_dir (from main) or workdir")
		}
		absDir, err := filepath.Abs(workdir)
		if err != nil {
			return nil, err
		}
		workdir = absDir
		if err := os.MkdirAll(workdir, 0755); err != nil {
			return nil, err
		}

		return &Adapter{
			id:            id,
			defaultTarget: defaultTarget,
			cursorBin:     bin,
			directory:     workdir,
			sessions:      make(map[string]string),
			args:          cliArgs,
		}, nil
	})
}

// Adapter 通过 Cursor Agent CLI 调用 Cursor，适合作为本地代码助手。
// 设计：每条消息以一次性子进程方式调用 `agent`，按 channel 复用 chatId 以支持会话。
type Adapter struct {
	id            string
	defaultTarget string
	cursorBin     string
	directory     string

	inbound chan<- mybot.Message
	mu      sync.RWMutex
	// sessions 记录 channel -> chatId
	sessions map[string]string
	// args 为传给 Cursor CLI 的全局参数（位于 command 之前），例如：-f / --sandbox / --approve-mcps 等
	args []string
}

func (a *Adapter) GetID() string {
	return a.id
}

func (a *Adapter) GetDefaultTarget() string {
	return a.defaultTarget
}

func (a *Adapter) Start(ctx context.Context, inbound chan<- mybot.Message) error {
	a.inbound = inbound
	return nil
}

func (a *Adapter) ReceiveMessage(ctx context.Context, msg mybot.Message) error {
	// 为每个 channel 准备独立工作目录，并将附件写入其中，方便 Cursor 在本地工作区访问
	workdir := a.workdirForChannel(msg.Channel, msg.UserID)
	if err := os.MkdirAll(workdir, 0755); err != nil {
		slog.Error("Cursor adapter create workdir failed", "channel", msg.Channel, "workdir", workdir, "err", err)
	}
	files, err := a.ensureFilesInDirectory(ctx, workdir, msg.Files)
	if err != nil {
		slog.Error("Cursor adapter ensure files failed", "channel", msg.Channel, "err", err)
	}

	chatID, err := a.getOrCreateChatID(ctx, msg.Channel)
	if err != nil {
		slog.Error("Cursor adapter create chat failed", "channel", msg.Channel, "err", err)
	}

	// 如果消息没有明确的目标适配器，但当前适配器有默认目标，则使用默认目标
	targetAdapter := msg.SourceAdapter
	if dt := a.GetDefaultTarget(); dt != "" {
		targetAdapter = dt
	}

	// 将附件信息追加到提示词末尾，告诉 Cursor 命令行这些文件已保存到本地
	content := msg.Content
	if len(files) > 0 {
		var b strings.Builder
		b.WriteString(content)
		if strings.TrimSpace(content) != "" {
			b.WriteString("\n\n")
		}
		b.WriteString("附件列表（已保存到本地工作目录，可按需查看或编辑）：\n")
		for _, f := range files {
			b.WriteString("- ")
			if f.Name != "" {
				b.WriteString(f.Name)
				b.WriteString(" ")
			}
			b.WriteString("(")
			b.WriteString(f.URL)
			b.WriteString(")\n")
		}
		content = b.String()
	}

	output, err := a.runCursorAgent(ctx, content, chatID, workdir)
	if err != nil {
		slog.Error("Cursor adapter call failed", "channel", msg.Channel, "err", err)
		output = fmt.Sprintf("Cursor adapter error: %v", err)
	}

	reply := mybot.Message{
		ID:            fmt.Sprintf("cursor-%d", time.Now().UnixNano()),
		ParentID:      msg.ID,
		SourceAdapter: a.id,
		TargetAdapter: targetAdapter,
		Content:       output,
		Type:          mybot.TypeText,
		Timestamp:     time.Now().UnixMilli(),
		UserID:        "cursor-agent",
		Channel:       msg.Channel,
	}

	select {
	case a.inbound <- reply:
	case <-ctx.Done():
		return ctx.Err()
	}
	return nil
}

func (a *Adapter) Status() string {
	return "online"
}

// getOrCreateChatID 为指定 channel 获取或创建对应的 chatId。
// 使用 `agent create-chat` 创建新会话。
func (a *Adapter) getOrCreateChatID(ctx context.Context, channel string) (string, error) {
	if channel == "" {
		return "", nil
	}
	a.mu.RLock()
	if id, ok := a.sessions[channel]; ok && id != "" {
		a.mu.RUnlock()
		return id, nil
	}
	a.mu.RUnlock()

	// 创建新 chat
	args := make([]string, 0, len(a.args)+1)
	args = append(args, a.args...)
	args = append(args, "create-chat")
	cmd := exec.CommandContext(ctx, a.cursorBin, args...)
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	if err := cmd.Run(); err != nil {
		errMsg := strings.TrimSpace(stderr.String())
		if errMsg == "" {
			errMsg = err.Error()
		}
		return "", fmt.Errorf("create-chat failed: %s", errMsg)
	}

	chatID := strings.TrimSpace(stdout.String())
	if chatID == "" {
		return "", fmt.Errorf("create-chat returned empty chat id")
	}

	a.mu.Lock()
	a.sessions[channel] = chatID
	a.mu.Unlock()

	return chatID, nil
}

// runCursorAgent 调用 Cursor Agent CLI，对指定 chatId 执行一次性推理。
// 命令形如：
//
//	agent -f --sandbox disabled --print --resume <chatId> agent "<prompt>"
func (a *Adapter) runCursorAgent(ctx context.Context, prompt, chatID, workdir string) (string, error) {
	if strings.TrimSpace(prompt) == "" {
		return "", nil
	}

	// 先应用配置中的全局参数（在 command 前面）
	args := make([]string, 0, len(a.args)+4)
	args = append(args, a.args...)

	// 如果没有显式传入 -p/--print，则默认追加 --print，保证非交互输出可被当前适配器读取
	if !hasPrintFlag(args) {
		args = append(args, "--print")
	}
	if chatID != "" {
		args = append(args, "--resume", chatID)
	}
	args = append(args, "agent")

	// 将整条内容作为一个 prompt 参数传递；CLI 会自动拼接为最终提示词
	args = append(args, prompt)

	cmd := exec.CommandContext(ctx, a.cursorBin, args...)
	if workdir != "" {
		cmd.Dir = workdir
	}

	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	if err := cmd.Run(); err != nil {
		errMsg := strings.TrimSpace(stderr.String())
		if errMsg == "" {
			errMsg = err.Error()
		}
		return strings.TrimSpace(stdout.String()), fmt.Errorf("run cursor agent failed: %s", errMsg)
	}

	out := strings.TrimSpace(stdout.String())
	return out, nil
}

// workdirForChannel 为不同 channel（或 user）划分独立的子目录，方便隔离文件。
func (a *Adapter) workdirForChannel(channel, userID string) string {
	safe := sanitizePathSegment(channel)
	if safe == "" {
		safe = sanitizePathSegment(userID)
	}
	if safe == "" {
		safe = "default"
	}
	return filepath.Join(a.directory, safe)
}

func sanitizePathSegment(s string) string {
	s = strings.ReplaceAll(s, "/", "_")
	s = strings.ReplaceAll(s, "\\", "_")
	s = strings.ReplaceAll(s, "..", "_")
	return strings.TrimSpace(s)
}

// ensureFilesInDirectory 将消息中的附件复制到 workdir/uploads 下，并返回新的 file:// URL 列表
func (a *Adapter) ensureFilesInDirectory(ctx context.Context, workdir string, files []mybot.File) ([]mybot.File, error) {
	if len(files) == 0 {
		return nil, nil
	}
	dir := filepath.Join(workdir, uploadsDir)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, err
	}
	out := make([]mybot.File, 0, len(files))
	for i, f := range files {
		localPath, err := copyToDir(ctx, dir, f, i)
		if err != nil {
			return nil, err
		}
		out = append(out, mybot.File{
			Name:     f.Name,
			URL:      "file://" + filepath.ToSlash(localPath),
			MimeType: f.MimeType,
			Size:     f.Size,
		})
	}
	return out, nil
}

// copyToDir 参考 opencode 适配器，将远程或本地文件复制到指定目录
func copyToDir(ctx context.Context, dir string, f mybot.File, index int) (string, error) {
	name := f.Name
	if name == "" {
		name = fmt.Sprintf("file_%d", index)
	}
	localPath := filepath.Join(dir, filepath.Base(name))
	u, err := url.Parse(f.URL)
	if err != nil {
		return "", err
	}
	switch u.Scheme {
	case "http", "https":
		req, err := http.NewRequestWithContext(ctx, "GET", f.URL, nil)
		if err != nil {
			return "", err
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return "", err
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			return "", fmt.Errorf("download %s: %d", f.URL, resp.StatusCode)
		}
		w, err := os.Create(localPath)
		if err != nil {
			return "", err
		}
		_, err = io.Copy(w, resp.Body)
		w.Close()
		if err != nil {
			_ = os.Remove(localPath)
			return "", err
		}
		return localPath, nil
	case "file":
		src := u.Path
		if u.Host != "" {
			src = u.Host + u.Path
		}
		data, err := os.ReadFile(src)
		if err != nil {
			return "", err
		}
		if err := os.WriteFile(localPath, data, 0644); err != nil {
			return "", err
		}
		return localPath, nil
	default:
		return "", fmt.Errorf("unsupported file URL scheme: %s", u.Scheme)
	}
}

// hasPrintFlag 判断 args 中是否已经包含 -p/--print
func hasPrintFlag(args []string) bool {
	for _, a := range args {
		if a == "-p" || a == "--print" {
			return true
		}
	}
	return false
}
