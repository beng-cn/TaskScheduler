// Package store 提供任务存储的具体实现。
//
// Store 接口定义在 scheduler.Store（为避免循环导入，接口与实现分离）。
// 本包中各文件分别实现不同后端的存储：
//   - memory.go  → 基于内存，适合开发调试和单机演示
//   - (未来) mysql.go  → 基于 MySQL，适合生产环境
//   - (未来) redis.go  → 基于 Redis，利用缓存+持久化
//
// 通过接口抽象，业务代码无需知晓底层存储细节，便于切换和单元测试 mock。
package store
