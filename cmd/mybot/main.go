package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	"github.com/lengzhao/mybot"
	_ "github.com/lengzhao/mybot/adapters"
)

func main() {
	// 1. 加载配置
	configPath := "config.yaml"
	if len(os.Args) > 1 {
		configPath = os.Args[1]
	}
	slog.SetLogLoggerLevel(slog.LevelDebug)

	cfg, err := mybot.LoadConfig(configPath)
	if err != nil {
		fmt.Printf("Failed to load config: %v\n", err)
		return
	}

	// 2. 创建调度器
	dispatcher := mybot.NewDispatcher()
	if cfg.System.DefaultAdapter != "" {
		dispatcher.SetDefaultAdapter(cfg.System.DefaultAdapter)
	}

	workDir := cfg.System.WorkDir
	if workDir == "" {
		workDir, _ = os.Getwd()
	}
	if workDir != "" {
		workDir, _ = filepath.Abs(workDir)
	}

	// 3. 根据配置实例化并注册适配器
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
			fmt.Printf("Failed to create adapter [%s] of type [%s]: %v\n", id, adapterType, err)
			continue
		}

		if err := dispatcher.Register(adapter); err != nil {
			fmt.Printf("Failed to register adapter [%s]: %v\n", id, err)
			continue
		}
		slog.Debug("Registered adapter", "id", id, "type", adapterType)
	}

	// 4. 启动调度器
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := dispatcher.Start(ctx); err != nil {
		fmt.Printf("Failed to start dispatcher: %v\n", err)
		return
	}

	fmt.Println("Mybot started. Press Ctrl+C to exit.")

	// 5. 等待信号退出
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	<-sigCh
	fmt.Println("\nShutting down...")
	dispatcher.Stop()
}
