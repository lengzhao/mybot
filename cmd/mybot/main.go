package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
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

	cfg, err := mybot.LoadConfig(configPath)
	if err != nil {
		fmt.Printf("Failed to load config: %v\n", err)
		return
	}

	// 2. 创建调度器
	dispatcher := mybot.NewDispatcher()

	// 3. 根据配置实例化并注册适配器
	for _, aCfg := range cfg.Adapters {
		if !aCfg.Enabled {
			continue
		}

		adapter, err := mybot.CreateAdapter(aCfg.Type, aCfg.ID, aCfg.Config)
		if err != nil {
			fmt.Printf("Failed to create adapter [%s] of type [%s]: %v\n", aCfg.ID, aCfg.Type, err)
			continue
		}

		if err := dispatcher.Register(adapter); err != nil {
			fmt.Printf("Failed to register adapter [%s]: %v\n", aCfg.ID, err)
			continue
		}
		fmt.Printf("Registered adapter: %s (%s)\n", aCfg.ID, aCfg.Type)
	}

	// 4. 启动调度器
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := dispatcher.Start(ctx); err != nil {
		fmt.Printf("Failed to start dispatcher: %v\n", err)
		return
	}

	fmt.Println("MuseBot started. Press Ctrl+C to exit.")

	// 5. 等待信号退出
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	<-sigCh
	fmt.Println("\nShutting down...")
	dispatcher.Stop()
}
