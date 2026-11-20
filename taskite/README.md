# Taskite 品牌资源

Taskite 是分布式任务调度引擎。所有图标统一使用三节点环形 T 标识：环形轨道表达任务分发、周期调度与持续执行，中央 T 对应 Taskite。

> **Distributed tasks. Reliable execution.**
>
> **分布式任务，可靠执行。**

矢量母版为 [taskite-icon.svg](taskite-icon.svg)。不同平台的图标保留相同的图形路径和比例，文件名中的尺寸表示默认显示尺寸。

|         用途         |                                               文件                                                |
|--------------------|-------------------------------------------------------------------------------------------------|
| 产品 Logo            | [浅色横版](logo/horizontal-light.svg)、[深色横版](logo/horizontal-dark.svg)、[竖版](logo/stacked-light.svg) |
| 默认横版 Logo          | [logo.svg](logo.svg)                                                                            |
| 浏览器标签页             | `favicon.svg`、`favicon-16x16.svg`、`favicon-32x32.svg`、`favicon-48x48.svg`                       |
| Web 通用图标           | `web/icon-*.svg`                                                                                |
| 核心与文档图标            | `icon/core/taskite-icon.svg`、`icon/docs/icon.svg`                                               |
| 桌面与移动端             | `icon/desktop/app-icon.svg`、`icon/mobile/app-icon.svg`                                          |
| iPhone / iPad      | `apple-touch-icon.svg`、`iphone/apple-touch-icon.svg`、`iphone/app-icons/icon-*.svg`              |
| Android            | `android/mipmap-*/*.svg`、`android/drawable-*/*.svg`、`icon/android/adaptive-*.svg`               |
| Chrome / PWA       | `android-chrome-*x*.svg`、`android-chrome-maskable-*x*.svg`、`site.webmanifest`                   |
| CLI 单色图标           | [cli-monochrome.svg](icon/terminal/cli-monochrome.svg)                                          |
| GitHub 组织头像        | [github-organization.png](icon/web/github-organization.png)                                     |
| Spring Boot Banner | [spring-boot-banner.txt](icon/terminal/spring-boot-banner.txt)                                  |

[icon-sizes.json](icon-sizes.json) 登记尺寸、背景、比例与用途。[品牌资产说明](brand-assets.md) 包含使用规则，[品牌定位](taskite-brand.md) 说明产品与视觉表达，[Logo 与 Slogan](logo/README.md) 包含备选文案。

## 平台适配

- 浏览器、Web、核心和文档图标使用透明背景。
- 桌面图标使用白色圆角底板；iPhone 和移动端设计源文件使用白色方形底板。
- Android 自适应前景以画布中心缩放至 75%，背景使用白色；PWA 可遮罩版本以画布中心缩放至 90%。
- CLI 单色版本使用 `currentColor`，由宿主决定颜色。
- SVG 文件保留矢量路径。GitHub 组织头像另提供 512 × 512 PNG。

Web 接入示例：

```html
<link rel="icon" href="/taskite/favicon.svg" type="image/svg+xml" sizes="any">
<link rel="manifest" href="/taskite/site.webmanifest">
<meta name="theme-color" content="#4657e8">
```

部署 PWA 前应按实际页面入口设置 `start_url` 和 `scope`。原生 iOS / Android 使用设计源文件按平台要求导出 PNG、VectorDrawable 或其他资源格式。
