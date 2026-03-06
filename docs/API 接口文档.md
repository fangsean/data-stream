# 大数据交换平台迁移服务 - API 接口文档

## 目录

- [概述](#概述)
- [核心接口](#核心接口)
  - [数据去重过滤器接口](#数据去重过滤器接口)
  - [数据源服务接口](#数据源服务接口)
  - [数据处理服务接口](#数据处理服务接口)
  - [迁移任务执行器接口](#迁移任务执行器接口)
  - [错误追溯服务接口](#错误追溯服务接口)
- [数据模型](#数据模型)
- [使用示例](#使用示例)

---

## 概述

本文档描述了大数据交换平台迁移服务的核心 API 接口。该服务用于处理 10 亿级别的数据量从 A 库到 B 库的迁移，支持按日期分批查询、高效去重过滤、异常数据追溯等功能。

### 技术特点

- **高性能**: 基于 BloomFilter 实现高效去重，内存占用小
- **零错误率**: 双重校验机制确保数据准确性
- **可扩展**: 模块化设计，支持水平扩展
- **可追溯**: 完整的异常数据记录和追溯机制
- **持久化**: 支持过滤数据存盘和加载

---

## 核心接口

### 数据去重过滤器接口

**接口名称**: `DuplicateFilter`

**包路径**: `com.datastream.migration.filter`

**描述**: 提供高效的大数据量去重功能，支持 BloomFilter 和磁盘存储

#### 方法列表

##### 1. init()

```java
void init()
```

**功能**: 初始化过滤器，加载历史过滤数据到内存（如果存在）

**返回值**: 无

**异常**: RuntimeException - 初始化失败时抛出

---

##### 2. isDuplicate()

```java
boolean isDuplicate(MigrationData data)
```

**功能**: 检查数据是否重复（双重校验机制）

**参数**:
- `data` - 待检查的迁移数据

**返回值**: 
- `true` - 数据重复
- `false` - 数据不重复

**说明**: 
- 第一重：BloomFilter 快速判断
- 第二重：ConcurrentHashMap 精确校验

---

##### 3. filterDuplicates()

```java
List<MigrationData> filterDuplicates(List<MigrationData> dataList)
```

**功能**: 批量检查数据并过滤重复数据

**参数**:
- `dataList` - 待检查的迁移数据列表

**返回值**: 过滤后的数据列表（不包含重复数据）

**性能**: 支持并行流处理，适合大数据量

---

##### 4. add()

```java
void add(MigrationData data)
```

**功能**: 将数据添加到过滤器中

**参数**:
- `data` - 迁移数据

**线程安全**: 是

---

##### 5. addAll()

```java
void addAll(List<MigrationData> dataList)
```

**功能**: 批量添加数据到过滤器中

**参数**:
- `dataList` - 迁移数据列表

---

##### 6. saveToFile()

```java
void saveToFile()
```

**功能**: 保存过滤器数据到磁盘（只保存精确匹配 Map）

**返回值**: 无

**持久化内容**: ConcurrentHashMap 中的数据

---

##### 7. loadFromFile()

```java
void loadFromFile()
```

**功能**: 从磁盘加载过滤器数据

**返回值**: 无

**说明**: 恢复之前保存的精确匹配 Map，并重建 BloomFilter

---

##### 8. clear()

```java
void clear()
```

**功能**: 清空过滤器，释放内存

**返回值**: 无

---

##### 9. size()

```java
long size()
```

**功能**: 获取过滤器中的元素数量

**返回值**: 元素数量

---

##### 10. destroy()

```java
void destroy()
```

**功能**: 销毁过滤器，释放所有资源

**返回值**: 无

**说明**: 先保存数据到磁盘，再清空内存

---

### 数据源服务接口

**接口名称**: `DataSourceService`

**包路径**: `com.datastream.migration.service`

**描述**: 提供从 A 库查询数据的功能

#### 方法列表

##### 1. init()

```java
void init(MigrationConfig config)
```

**功能**: 初始化数据源服务

**参数**:
- `config` - 迁移配置

---

##### 2. queryByDateRange()

```java
List<MigrationData> queryByDateRange(Date startDate, Date endDate, int offset, int limit)
```

**功能**: 根据日期范围分批查询数据

**参数**:
- `startDate` - 开始日期
- `endDate` - 结束日期
- `offset` - 偏移量
- `limit` - 每批次数量

**返回值**: 迁移数据列表

**分页**: 支持 LIMIT 和 OFFSET 分页查询

---

##### 3. queryByBusinessDate()

```java
List<MigrationData> queryByBusinessDate(Date businessDate, int offset, int limit)
```

**功能**: 根据业务日期查询数据

**参数**:
- `businessDate` - 业务日期
- `offset` - 偏移量
- `limit` - 每批次数量

**返回值**: 迁移数据列表

---

##### 4. countByBusinessDate()

```java
long countByBusinessDate(Date businessDate)
```

**功能**: 获取指定日期的数据总数

**参数**:
- `businessDate` - 业务日期

**返回值**: 数据总数

---

##### 5. totalCount()

```java
long totalCount()
```

**功能**: 获取总数据量

**返回值**: 总数据量

---

##### 6. testConnection()

```java
boolean testConnection()
```

**功能**: 测试数据库连接

**返回值**: 
- `true` - 连接成功
- `false` - 连接失败

---

##### 7. close()

```java
void close()
```

**功能**: 关闭数据源，释放资源

---

### 数据处理服务接口

**接口名称**: `DataProcessService`

**包路径**: `com.datastream.migration.service`

**描述**: 提供数据转换、验证、清洗和入库功能

#### 方法列表

##### 1. init()

```java
void init(MigrationConfig config)
```

**功能**: 初始化数据处理服务

**参数**:
- `config` - 迁移配置

---

##### 2. process()

```java
MigrationData process(MigrationData data)
```

**功能**: 处理单条数据（包括清洗、转换、验证）

**参数**:
- `data` - 待处理的迁移数据

**返回值**: 处理后的迁移数据

**处理流程**:
1. 数据清洗
2. 数据转换
3. 数据验证

---

##### 3. processBatch()

```java
List<MigrationData> processBatch(List<MigrationData> dataList)
```

**功能**: 批量处理数据

**参数**:
- `dataList` - 待处理的迁移数据列表

**返回值**: 处理后的迁移数据列表

---

##### 4. validate()

```java
boolean validate(MigrationData data)
```

**功能**: 验证数据有效性

**参数**:
- `data` - 迁移数据

**返回值**: 
- `true` - 有效
- `false` - 无效

**验证规则**:
- ID 必填
- 去重关键字必填
- 可选：邮箱格式、手机号格式等

---

##### 5. transform()

```java
MigrationData transform(MigrationData data)
```

**功能**: 数据转换

**参数**:
- `data` - 源数据

**返回值**: 转换后的数据

**转换内容**:
- 去重关键字格式化
- 来源标识设置
- 时间字段处理

---

##### 6. clean()

```java
MigrationData clean(MigrationData data)
```

**功能**: 数据清洗

**参数**:
- `data` - 待清洗的数据

**返回值**: 清洗后的数据

**清洗内容**:
- 去除首尾空格
- 处理 NULL 值
- 空字符串转 NULL

---

##### 7. saveToTarget()

```java
boolean saveToTarget(MigrationData data)
```

**功能**: 保存数据到目标库

**参数**:
- `data` - 迁移数据

**返回值**: 
- `true` - 保存成功
- `false` - 保存失败

---

##### 8. saveBatchToTarget()

```java
int saveBatchToTarget(List<MigrationData> dataList)
```

**功能**: 批量保存数据到目标库

**参数**:
- `dataList` - 迁移数据列表

**返回值**: 成功的数量

**性能优化**: 使用批处理方式提高性能

---

##### 9. close()

```java
void close()
```

**功能**: 关闭服务

---

### 迁移任务执行器接口

**接口名称**: `MigrationExecutor`

**包路径**: `com.datastream.migration.scheduler`

**描述**: 负责协调数据源、过滤器、处理器完成数据迁移

#### 方法列表

##### 1. init()

```java
void init(MigrationConfig config, DataSourceService dataSourceService, 
          DataProcessService dataProcessService, DuplicateFilter duplicateFilter)
```

**功能**: 初始化迁移执行器

**参数**:
- `config` - 迁移配置
- `dataSourceService` - 数据源服务
- `dataProcessService` - 数据处理服务
- `duplicateFilter` - 去重过滤器

---

##### 2. execute()

```java
MigrationResult execute()
```

**功能**: 执行迁移任务

**返回值**: 迁移结果

**说明**: 默认按配置的日期范围执行

---

##### 3. executeByDateRange()

```java
MigrationResult executeByDateRange(Date startDate, Date endDate)
```

**功能**: 按日期分批执行迁移

**参数**:
- `startDate` - 开始日期
- `endDate` - 结束日期

**返回值**: 迁移结果

**执行流程**:
1. 生成日期列表
2. 按日期逐个处理
3. 分批查询数据
4. 去重过滤
5. 数据处理
6. 批量入库
7. 记录进度

---

##### 4. pause()

```java
void pause()
```

**功能**: 暂停迁移任务

---

##### 5. resume()

```java
void resume()
```

**功能**: 恢复迁移任务

---

##### 6. stop()

```java
void stop()
```

**功能**: 停止迁移任务

---

##### 7. getProgress()

```java
double getProgress()
```

**功能**: 获取当前进度（百分比）

**返回值**: 进度百分比

---

##### 8. getProcessedCount()

```java
long getProcessedCount()
```

**功能**: 获取已处理的记录数

**返回值**: 已处理记录数

---

### 错误追溯服务接口

**接口名称**: `ErrorTraceService`

**包路径**: `com.datastream.migration.tracing`

**描述**: 提供异常数据的查询、统计、导出等功能

#### 方法列表

##### 1. init()

```java
void init()
```

**功能**: 初始化错误追溯服务

---

##### 2. recordError()

```java
void recordError(ErrorRecord errorRecord)
```

**功能**: 记录错误

**参数**:
- `errorRecord` - 错误记录

**线程安全**: 是

---

##### 3. queryByTaskId()

```java
List<ErrorRecord> queryByTaskId(String taskId)
```

**功能**: 根据任务 ID 查询错误记录

**参数**:
- `taskId` - 任务 ID

**返回值**: 错误记录列表（按发生时间倒序）

---

##### 4. queryByErrorType()

```java
List<ErrorRecord> queryByErrorType(String errorType)
```

**功能**: 根据错误类型查询错误记录

**参数**:
- `errorType` - 错误类型

**返回值**: 错误记录列表

---

##### 5. queryByTimeRange()

```java
List<ErrorRecord> queryByTimeRange(Date startTime, Date endTime)
```

**功能**: 根据时间范围查询错误记录

**参数**:
- `startTime` - 开始时间
- `endTime` - 结束时间

**返回值**: 错误记录列表

---

##### 6. getErrorStatistics()

```java
String getErrorStatistics(String taskId)
```

**功能**: 获取指定任务的错误统计

**参数**:
- `taskId` - 任务 ID

**返回值**: 错误统计信息（JSON 格式）

**统计内容**:
- 总错误数
- 按错误类型统计
- 按处理状态统计

---

##### 7. markAsProcessed()

```java
void markAsProcessed(Long errorId, String remark)
```

**功能**: 标记错误记录为已处理

**参数**:
- `errorId` - 错误记录 ID
- `remark` - 处理备注

---

##### 8. batchMarkAsProcessed()

```java
void batchMarkAsProcessed(List<Long> errorIds, String remark)
```

**功能**: 批量标记错误记录为已处理

**参数**:
- `errorIds` - 错误记录 ID 列表
- `remark` - 处理备注

---

##### 9. exportToFile()

```java
void exportToFile(String taskId, String filePath)
```

**功能**: 导出错误记录到文件

**参数**:
- `taskId` - 任务 ID
- `filePath` - 导出文件路径

---

##### 10. clearErrors()

```java
void clearErrors(String taskId)
```

**功能**: 清空错误记录

**参数**:
- `taskId` - 任务 ID

---

##### 11. close()

```java
void close()
```

**功能**: 关闭服务

---

## 数据模型

### MigrationData

迁移数据实体类

| 字段 | 类型 | 说明 |
|------|------|------|
| id | Long | 主键 ID |
| businessDate | Date | 业务日期 |
| duplicateKey | String | 去重关键字 |
| sourceFlag | String | 数据来源标识 |
| content | String | 数据内容 |
| createTime | Date | 创建时间 |
| updateTime | Date | 更新时间 |
| extField1 | String | 扩展字段 1 |
| extField2 | String | 扩展字段 2 |
| extField3 | String | 扩展字段 3 |

### MigrationConfig

迁移任务配置

| 字段 | 类型 | 说明 | 默认值 |
|------|------|------|--------|
| taskId | String | 任务 ID | - |
| taskName | String | 任务名称 | - |
| sourceDbConnection | String | 源数据库连接 | - |
| targetDbConnection | String | 目标数据库连接 | - |
| sourceTable | String | 源表名 | - |
| targetTable | String | 目标表名 | - |
| duplicateFields | String | 去重字段列表 | - |
| batchSize | int | 每批次查询量 | 10000 |
| startDate | Date | 开始日期 | - |
| endDate | Date | 结束日期 | - |
| threadCount | int | 并发线程数 | 4 |
| enableBloomFilter | boolean | 是否启用 BloomFilter | true |
| expectedInsertions | long | BloomFilter 预期数据量 | 1000000000L |
| fpp | double | BloomFilter 误判率 | 0.0001 |
| filterDataPath | String | 过滤数据存储路径 | - |
| errorDataPath | String | 异常数据存储路径 | - |
| detailedLogging | boolean | 是否记录详细日志 | true |

### MigrationResult

迁移任务执行结果

| 字段 | 类型 | 说明 |
|------|------|------|
| taskId | String | 任务 ID |
| success | boolean | 是否成功 |
| totalRecords | long | 迁移的总记录数 |
| successCount | long | 成功入库的记录数 |
| filteredCount | long | 过滤的重复记录数 |
| failedCount | long | 失败的记录数 |
| startTime | Date | 开始时间 |
| endTime | Date | 结束时间 |
| durationMs | long | 耗时（毫秒） |
| errorMessage | String | 错误信息 |
| detailInfo | String | 详细信息 |

### ErrorRecord

异常数据记录

| 字段 | 类型 | 说明 |
|------|------|------|
| id | Long | 主键 ID |
| taskId | String | 任务 ID |
| errorType | String | 错误类型 |
| originalDataId | Long | 原始数据 ID |
| duplicateKey | String | 去重关键字 |
| errorMessage | String | 错误信息 |
| stackTrace | String | 堆栈跟踪 |
| dataSnapshot | String | 原始数据快照 |
| occurTime | Date | 发生时间 |
| processed | Boolean | 是否已处理 |
| processTime | Date | 处理时间 |
| processRemark | String | 处理备注 |

---

## 使用示例

### 1. 基本使用

```java
// 创建迁移配置
MigrationConfig config = MigrationConfig.builder()
    .taskId("TASK_001")
    .sourceDbConnection("jdbc:mysql://localhost:3306/source_db")
    .targetDbConnection("jdbc:mysql://localhost:3306/target_db")
    .sourceTable("source_table")
    .targetTable("target_table")
    .batchSize(10000)
    .threadCount(4)
    .enableBloomFilter(true)
    .expectedInsertions(1000000000L)
    .fpp(0.0001)
    .build();

// 创建并初始化各服务
DataSourceService dataSourceService = new DataSourceServiceImpl();
dataSourceService.init(config);

DataProcessService dataProcessService = new DataProcessServiceImpl();
dataProcessService.init(config);

DuplicateFilter duplicateFilter = new BloomFilterDuplicateFilter(config);
duplicateFilter.init();

// 创建迁移执行器
MigrationExecutor executor = new MigrationExecutorImpl();
executor.init(config, dataSourceService, dataProcessService, duplicateFilter);

// 执行迁移任务
MigrationResult result = executor.executeByDateRange(startDate, endDate);

// 输出结果
System.out.println("迁移完成：" + result.isSuccess());
System.out.println("总记录数：" + result.getTotalRecords());
System.out.println("成功入库：" + result.getSuccessCount());
System.out.println("过滤重复：" + result.getFilteredCount());
```

### 2. 错误追溯

```java
// 创建错误追溯服务
ErrorTraceService errorTraceService = new ErrorTraceServiceImpl();
errorTraceService.init();

// 查询某任务的错误记录
List<ErrorRecord> errors = errorTraceService.queryByTaskId("TASK_001");

// 获取错误统计
String statistics = errorTraceService.getErrorStatistics("TASK_001");

// 导出错误记录
errorTraceService.exportToFile("TASK_001", "errors/task_001_errors.dat");
```

---

## 版本信息

- **版本**: 1.0
- **创建日期**: 2026-03-06
- **作者**: DataStream Team

---

## 联系方式

如有问题，请联系开发团队。
