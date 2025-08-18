package com.retailersv1.domain;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

/**
 * @BelongsProject: test-git
 * @BelongsPackage: com.retailersv1.domain
 * @Author: liwenjie
 * @CreateTime: 2025-08-18  14:27
 * @Description: TODO
 * @Version: 1.0
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class TableProcessDim implements Serializable {
    // 来源表名
    String sourceTable;

    // 目标表名
    String sinkTable;

    // 输出字段
    String sinkColumns;

    // 数据到 hbase 的列族
    String sinkFamily;

    String sinkRowKey;

    String op;


}
