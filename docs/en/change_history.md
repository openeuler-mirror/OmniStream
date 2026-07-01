# Change History<a name="change_history"></a>

<table>
<thead>
  <tr>
    <th style="width: 30%">Date</th>
    <th style="width: 70%">Description</th>
  </tr>
  </thead>
  <tbody>
    <tr>
    <td>2026-06-30</td>
    <td>This is the fourth official release.<br>Released OmniStream 1.3.0:<br>Added the following support in SQL scenarios: WindowAgg and WindowJoin operators are supported. The Calc operator supports UDF registration, as well as built-in functions such as JSON_VALUE, JSON_QUERY, COALESCE, PROCTIME_MATERIALIZE, CHAR_LENGTH, and TO_TIMESTAMP_LTZ. The Calc operator also supports data types including INTEGER and TIMESTAMP_WITH_LOCAL_TIMEZONE(3).</td>
  </tr>
  <tr>
    <td>2026-03-30</td>
    <td>This is the third official release.<br>Released OmniStream 1.2.0:<br>Added the content of installing the header file on which the UDF translator depends, and introduced support for enabling the OmniStateStore feature in stateful scenarios.</td>
  </tr>
  <tr>
    <td>2025-12-30</td>
    <td>This is the second official release.<br>OmniStream 1.1.0:<br>Added the task-level operator rollback mechanism in SQL scenarios, and added checkpoint and restore support for KeyedCoProcess in DataStream scenarios.</td>
  </tr>
  <tr>
    <td>2025-06-30</td>
    <td>This is the first official release.<br>Released OmniStream 1.0.0:<br><li>SQL: Implemented acceleration for the Calc, GroupAgg, Join, Deduplicate, Rank, Window, and Kafka Source/Sink operators; implemented the efficient data organization method OmniVec; added support for memory and RocksDB state backends. </li> <li>DataStream: Implemented acceleration for the Kafka Source, Kafka Sink, Map, FlatMap, Reduce, and filter operators; implemented the UDF basic framework and UDF translation basic library; added support for the UDF automatic native framework to run stateful and stateless cases such as DataStream Wordcount; added support for the memory state backend.</li></td>
  </tr> 
  </tbody>
</table>
