package main

import (
	"context"
	"flag"
	"log/slog"
	"os"
	"path/filepath"

	"github.com/kardianos/service"
	"github.com/lengzhao/mybot"
	_ "github.com/lengzhao/mybot/adapters"
	"github.com/lengzhao/mybot/admin"
)

type program struct {
	configPath   string
	ctx          context.Context
	cancel       context.CancelFunc
	dispatcher   *mybot.Dispatcher
	adminService *admin.Service
	store        mybot.StateStore
}

func (p *program) Start(s service.Service) error {
	// Start should not block, do the work async.
	go p.run()
	return nil
}

func (p *program) Stop(s service.Service) error {
	// Stop is called in a separate goroutine.
	if p.dispatcher != nil {
		p.dispatcher.Stop()
	}
	if p.adminService != nil {
		if err := p.adminService.Stop(); err != nil {
			slog.Error("Failed to stop admin service", "err", err)
		} else {
			slog.Info("Admin service stopped")
		}
	}
	if p.cancel != nil {
		p.cancel()
	}
	return nil
}

func (p *program) run() {
	cfg, err := mybot.LoadConfig(p.configPath)
	if err != nil {
		slog.Error("Failed to load config", "err", err, "config_path", p.configPath)
		return
	}

	// 1. 计算工作目录：优先使用配置的 work_dir；否则使用「配置文件所在目录/workdir」
	workDir := cfg.System.WorkDir
	if workDir == "" {
		baseDir := filepath.Dir(p.configPath)
		workDir = filepath.Join(baseDir, "workdir")
	}
	if workDir != "" {
		if abs, err := filepath.Abs(workDir); err == nil {
			workDir = abs
		} else {
			slog.Warn("Failed to resolve absolute workdir, use raw value", "workdir", workDir, "err", err)
		}
	}

	// 2. 创建调度器
	dispatcher := mybot.NewDispatcher()
	if cfg.System.DefaultAdapter != "" {
		dispatcher.SetDefaultAdapter(cfg.System.DefaultAdapter)
	}

	var store mybot.StateStore
	if cfg.System.StateStore.Enabled {
		store, err = mybot.NewSQLiteStore(cfg.System.StateStore)
		if err != nil {
			slog.Error("Failed to create state store", "err", err)
			return
		}
	}
	// 3. 创建并启动管理服务（如果启用）
	var adminService *admin.Service
	if cfg.System.Admin.Enabled {
		adminConfig := admin.Config{
			Enabled:     cfg.System.Admin.Enabled,
			Port:        cfg.System.Admin.Port,
			ConfigPath:  cfg.System.Admin.ConfigPath,
			CronAPIURLs: cfg.System.Admin.CronAPIURLs,
			LogPath:     cfg.System.Admin.LogPath,
		}
		adminService = admin.NewService(adminConfig, store)
	}

	// 4. 根据配置实例化并注册适配器
	for _, aCfg := range cfg.Adapters {
		// 从 map 中获取必要字段
		enabled, _ := aCfg["enabled"].(bool)
		if !enabled {
			continue
		}

		id, _ := aCfg["id"].(string)
		adapterType, _ := aCfg["type"].(string)

		// 将 DefaultTarget 添加到适配器配置中
		adapterConfig := mybot.AdapterConfigWithDir(aCfg, workDir, id)

		adapter, err := mybot.CreateAdapter(adapterType, id, adapterConfig)
		if err != nil {
			slog.Error("Failed to create adapter", "id", id, "type", adapterType, "err", err)
			continue
		}

		if err := dispatcher.Register(adapter); err != nil {
			slog.Error("Failed to register adapter", "id", id, "err", err)
			continue
		}
		slog.Debug("Registered adapter", "id", id, "type", adapterType)
	}

	// 5. 启动调度器和管理服务
	ctx, cancel := context.WithCancel(context.Background())
	p.ctx = ctx
	p.cancel = cancel
	p.dispatcher = dispatcher
	p.adminService = adminService
	p.store = store

	// 启动管理服务
	if adminService != nil {
		if err := adminService.Start(ctx); err != nil {
			slog.Error("Failed to start admin service", "err", err)
			// 管理服务失败不终止主程序
		} else {
			slog.Info("Admin service started", "port", cfg.System.Admin.Port)
		}
	}

	if err := dispatcher.Start(ctx); err != nil {
		slog.Error("Failed to start dispatcher", "err", err)
		return
	}
	if store != nil {
		store.Start()
	}
	dispatcher.SetStateStore(store)

	slog.Info("Mybot started as service", "workdir", workDir)
}

func main() {
	// 服务控制参数，参考 github.com/lengzhao/database/main.go
	control := flag.String("c", "", "control of service: install/start/stop/restart/uninstall/stat")
	configFlag := flag.String("config", "", "config file path (default: ~/.mybot/config.yaml)")
	flag.Parse()

	// 默认配置文件放在用户 home 目录的 .mybot/config.yaml
	homeDir, err := os.UserHomeDir()
	if err != nil {
		slog.Error("Failed to get user home dir", "err", err)
		return
	}
	configPath := filepath.Join(homeDir, ".mybot", "config.yaml")
	if *configFlag != "" {
		configPath = *configFlag
	}

	// 根据配置初始化 slog：log_level、log_file（空则 stdout）
	level := slog.LevelInfo
	var logWriter *os.File
	if cfg, loadErr := mybot.LoadConfig(configPath); loadErr == nil {
		switch cfg.System.LogLevel {
		case "debug":
			level = slog.LevelDebug
		case "info", "":
			level = slog.LevelInfo
		case "warn":
			level = slog.LevelWarn
		case "error":
			level = slog.LevelError
		}
		if cfg.System.LogFile != "" {
			logPath := cfg.System.LogFile
			if !filepath.IsAbs(logPath) {
				logPath = filepath.Join(filepath.Dir(configPath), logPath)
			}
			if dir := filepath.Dir(logPath); dir != "" {
				_ = os.MkdirAll(dir, 0o755)
			}
			if f, openErr := os.OpenFile(logPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644); openErr == nil {
				logWriter = f
			}
		}
	}
	if logWriter == nil {
		logWriter = os.Stdout
	}
	handler := slog.NewTextHandler(logWriter, &slog.HandlerOptions{Level: level})
	slog.SetDefault(slog.New(handler))
	if logWriter == os.Stdout {
		slog.Info("Logger initialized", "output", "stdout", "level", level)
	} else {
		slog.Info("Logger initialized", "file", logWriter.Name(), "level", level)
	}

	prg := &program{configPath: configPath}

	svcConfig := &service.Config{
		Name:        "mybot",
		DisplayName: "MyBot Service",
		Description: "MyBot chatbot service.",
		Option: service.KeyValue{
			"UserService": true, // macOS 下以当前用户身份安装到 ~/Library/LaunchAgents
			"RunAtLoad":   true, // 开机 / 登录时自动启动
		},
	}

	s, err := service.New(prg, svcConfig)
	if err != nil {
		slog.Error("Failed to create service", "err", err)
		return
	}

	if *control == "" {
		if err := s.Run(); err != nil {
			slog.Error("Service run error", "err", err)
		}
		return
	}
	if err := service.Control(s, *control); err != nil {
		slog.Error("Service control error", "err", err, "control", *control)
	}
}
