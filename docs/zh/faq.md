# 常见问题<a name="ZH-CN_TOPIC_0000002549644861"></a>

## Nexmark连续SQL任务执行异常的解决方法<a name="ZH-CN_TOPIC_0000002549644857"></a>

**问题现象描述<a name="zh-cn_topic_0000002533267891_section758133012554"></a>**

- 现象一：使用开源Nexmark组件连续提交SQL任务时，有概率出现前后两轮任务执行重叠现象。此时上一轮任务占据资源未释放，导致下一轮任务无资源可用。JobManager日志中打印`free slot:0`关键字，同时将任务取消，导致任务最终执行失败。

  ![](figures/zh-cn_image_0000002501354086.png)

- 现象二：Nexmark有概率因任务执行过快而导致未能抓取到吞吐量数据，提示`The metric reporter doesn't collect any metrics`。

  ![](figures/zh-cn_image_0000002533273995.png)

- 现象三：连续提交SQL任务并长时间运行Nexmark Q9等大状态SQL时，有概率引发Java OOM（out of memory，内存不足）问题。

  ![](figures/zh-cn_image_0000002533313933.png)

**关键过程、根本原因分析<a name="zh-cn_topic_0000002533267891_section145813300553"></a>**

- 现象一：Nexmark开源软件存在逻辑缺陷，未正确处理任务释放时序。
- 现象二：Nexmark开源软件存在逻辑缺陷，OmniStream可以正常完成SQL任务的执行。
- 现象三：属Flink原生问题，非OmniStream导致。

**结论、解决方案及效果<a name="zh-cn_topic_0000002533267891_section93441811202317"></a>**

- 现象一：重新提交任务。
- 现象二：忽略提示，无需处理。
- 现象三：需要重启整个Flink集群，并重新提交任务。

## JVM GC 参数丢失的解决办法<a name="ZH-CN_TOPIC_0000002549644857"></a>

**问题现象描述<a name="zh-cn_topic_0000002533267891_section758133012554"></a>**

现象：在使能omniStream后，flink taskManager原本的G1GC参数丢失，导致JVM发生内存溢出（oom）。

**关键过程、根本原因分析<a name="zh-cn_topic_0000002533267891_section145813300553"></a>**

flink taskManager 在启动时会检测是否配置了JVM_OPTS环境变量。如果没有配置，则创建JVM_OPTS并添加G1GC参数启动taskManager，否则使用配置的JVM_OPTS启动taskManager。在使能OmniStream后会自动生成JVM_OPTS，导致G1GC参数丢失。

**结论、解决方案及效果<a name="zh-cn_topic_0000002533267891_section93441811202317"></a>**

参考如下指令，在flink-conf.yaml配置文件中手动添加 taskManager G1GC参数。

```yaml
env.java.opts.taskmanager: -XX:+UseG1GC
```

## SQL作业使用Kafka数据源且包含Window算子时，处于正常运行状态但无结果产生

**问题现象描述**

前提：SQL作业使用Kafka数据源；Source算子并行度大于对应Kafka topic分区数；作业包含事件时间语义下的Window算子

现象：作业正常运行但无结果产生

**关键过程、根本原因分析**

事件时间语义下Window算子的计算结果依赖Watermark推进后触发窗口计算。
当Kafka Source算子并行度大于topic分区数时，会存在部分Source并行实例未分配到Kafka分区。
下游算子的Watermark由各上游输入共同决定，由于这些并行实例始终无法接收到数据，因此也无法正常推进对应的Watermark，从而阻塞下游算子Watermark的推进。
当下游Window算子的Watermark始终无法超过窗口的触发时间时，窗口不会触发计算，因此作业表现为持续运行但没有结果输出。

**结论、解决方案及效果**

- Flink可通过配置数据源空闲超时时间，在Source长时间无数据输入后将其标记为Idle，使该输入暂时不参与Watermark计算，
从而避免阻塞整体Watermark推进，但当前OmniStream暂不支持该能力，见[约束与限制](../../README.md#约束与限制)。

- 使用Kafka数据源且作业包含依赖Watermark触发的Window算子时，应避免Source算子出现无Kafka分区可消费的情况。
建议重新创建Kafka topic，并将topic分区数设置为大于等于Kafka Source算子并行度的值，确保每个Source并行实例均能够分配到至少一个Kafka分区。
调整后，各Source并行实例均可正常消费数据并推进Watermark，下游Window能够按照事件时间正常触发并输出计算结果。

| 文档版本 | 发布日期 | 修改说明 |
| --- | --- | --- |
| 01 | 2026-09-30 | 第一次正式发布。 |
