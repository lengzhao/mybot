// Package adapters 通过导入各子包触发适配器自注册；新增适配器时在此添加 import 并在对应子目录实现。
package adapters

import (
	_ "github.com/lengzhao/mybot/adapters/console"
	_ "github.com/lengzhao/mybot/adapters/cursor"
	_ "github.com/lengzhao/mybot/adapters/deepseek"
	_ "github.com/lengzhao/mybot/adapters/echo"
	_ "github.com/lengzhao/mybot/adapters/lark"
	_ "github.com/lengzhao/mybot/adapters/opencode"
	_ "github.com/lengzhao/mybot/adapters/public_opencode"
	_ "github.com/lengzhao/mybot/adapters/webchat"
)
