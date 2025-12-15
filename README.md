# java-tsfile-api-test

------

## 环境

- Java 17+
- maven 3.9+

## 编译

```bash
git clone https://github.com/apache/tsfile.git
mvn clean install -P with-java -DskipTests
```

## 依赖

```xml
<dependency>
    <groupId>org.apache.tsfile</groupId>
    <artifactId>tsfile</artifactId>
    <version>${project.version}</version>
</dependency>
```

## 使用

### 自动化测试

```
mvn clean package -DskipTests
mvn surefire-report:report
```

在根目录下的 target/site 目录下生成默认名为 surefire-report.html 格式的测试报告

### 代码覆盖率测试

1. 收集源码：收集tsfile根目录下的 java\tsfile\src\main\java 中的 org 目录，复制 org 目录到程序根目录下的 code/src 目录中

2. 执行命令：执行下面命令，生成的覆盖率报告位于 target/jacoco-report 下的 index.xml 文件

```bash
mvn clean package -DskipTests
mvn test jacoco:report
```
若需要屏蔽某些目录，可以在 pom.xml 文件中修改 jacoco的 excludes 标签
