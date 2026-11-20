# Console 第三方依赖

前端应用源代码使用 Taskite Community License。以下库通过 npm 引入。

|      直接运行依赖       |  锁定版本   | 许可  |
|-------------------|---------|-----|
| React / React DOM | 18.2.0  | MIT |
| Ant Design        | 5.15.4  | MIT |
| Lucide React      | 0.294.0 | ISC |
| dayjs             | 1.11.10 | MIT |
| React Router DOM  | 6.30.3  | MIT |
| lossless-json     | 4.3.3   | MIT |
| CodeMirror JSON   | 6.0.2   | MIT |
| CodeMirror View   | 6.43.15 | MIT |
| React CodeMirror  | 4.25.12 | MIT |

开发与验证使用 Vite、React Vite plugin、TypeScript、React 类型声明、Tailwind CSS、PostCSS、Autoprefixer、Playwright；精确版本及依赖解析记录在 `client/package-lock.json`。Spring MVC 和 Spring Boot 由仓库父 POM 统一管理，许可为 Apache-2.0。

构建时 `build.license` 收集实际生产 bundle 的第三方元数据、许可声明与许可正文，输出 `client/target/dist/licenses.txt`，并打包至 `META-INF/resources/console/licenses.txt`。页面顶部链接该文件；构建与发行检查应确认该文件存在。以上登记不替代第三方包自身的许可条款。
