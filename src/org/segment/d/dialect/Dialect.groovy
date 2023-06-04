package org.segment.d.dialect

import groovy.transform.CompileStatic

@CompileStatic
interface Dialect {

    static String COUNT_COL = 'X_TOTAL_COUNT'

    String generatePaginationSql(String sql, int start, int limit)

    String generateCountSql(String sql)

    boolean isLimitSupport()

}