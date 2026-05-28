# 修订记录<a name="change_history"></a>
<table>
<thead>
  <tr>
    <th style="width: 30%">发布日期</th>
    <th style="width: 70%">修订记录</th>
  </tr>
  </thead>
  <tbody>
    <tr>
    <td>2026-06-30</td>
    <td>第四次正式发布。</br>发布发布OmniStream 1.3.0：</br>在SQL场景中，支持WindowAgg、WindowJoin算子，Calc算子支持UDF函数注册，Calc算子支持JSON_VALUE、JSON_QUERY、COALESCE、PROCTIME_MATERIALIZE、CHAR_LENGTH、TO_TIMESTAMP_LTZ内置函数，Calc算子支持INTEGER、TIMESTAMP_WITH_LOCAL_TIMEZONE(3)数据类型。</td>
  </tr>
  <tr>
    <td>2026-03-30</td>
    <td>第三次正式发布。</br>发布发布OmniStream 1.2.0：</br>增加UDF翻译工具所使用依赖的头文件安装内容，在有状态场景下使能OmniStateStore加速特性。</td>
  </tr>
  <tr>
    <td>2025-12-30</td>
    <td>第二次正式发布。</br>发布OmniStream 1.1.0：</br>在SQL场景中，新增支持task级别算子回退机制；在DataStream场景中，KeyedCoProcess算子支持checkpoint、restore。</td>
  </tr>
  <tr>
    <td>2025-06-30</td>
    <td>第一次正式发布。</br>发布OmniStream 1.0.0：</br><li>SQL：实现了Calc、GroupAgg、Join、Deduplicate、Rank、Window、Kafka Source/Sink算子加速；实现了高效数据组织方式OmniVec；实现了对内存和RocksDB状态后端的支持。</li> <li>DataStream：实现了Kafka Source、Kafka Sink、Map、FlatMap、 Reduce、Filter算子加速；实现了UDF基础框架和UDF翻译基础库，支持UDF自动Native化框架成功运行DataStream Wordcount等有状态和无状态用例；实现了对内存状态后端的支持。</li></td>
  </tr> 
  </tbody>
</table>