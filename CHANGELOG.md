# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]
### Added
- 增加binlog运行位置信息记录展示命令
- 优化单个规则返回报告格式
- 忽略错误处理向关闭的管道发送信息时产生的panic
- 优化程序内建服务关闭顺序，使命令在关闭期间可用
- 优化聚合器中的比较逻辑
- 优化rule,consume的代码逻辑
### Changed
- 修改内部flag为空结构体

## [1.1.2] 2020-01-09
### Added
- 添加web服务，为运行中修改同步行为提供支持
- 提供程序运行基本报告
- 添加示例
### Changed
- 配置包使用viper
- 处理mysql链接空闲过长断开问题
- 修改rule report命令逻辑，对具体规则，给出详细报告
- 修改gops命令为http接口形式，为了适配window

## [1.1.1] 2019-12-16
### Added
- 增加命令行功能 
- 优化消费器错误信息
- 解决github.com/siddontang/go-mysql/client线程不安全问题

## [1.1.0] 2019-12-09
### Added
- event增加解析函数
- rule增加新的mysql链接
- 把rule变为可以有多个处理的模式，用worker确认
- 把consume变为可以有多个处理的模式，用worker确认

## [1.0.2] - 2019-12-06
### Changed
- 解决规则多次重载问题

## [1.0.1] - 2019-12-06
### Added 
- 生成规则和消费的基础结构，构建fake对象

## [1.0.0] - 2019-12-03
### Added
- 初始化项目
