package com.segment.d

import groovy.transform.CompileStatic

@CompileStatic
class MySQLDialect implements Dialect {
    @Override
    String generatePaginationSql(String sql, int start, int limit) {
        def sb = new StringBuilder()
        sb << 'select ttt.* from ('
        sb << sql.trim()
        sb << ') ttt limit '
        sb << start
        sb << ','
        sb << limit
        sb.toString()
    }

    @Override
    String generateCountSql(String sql) {
        def sb = new StringBuilder()
        sb << 'select count(*) as '
        sb << COUNT_COL
        sb << ' from ('
        sb << sql.trim()
        sb << ') ttt'
        sb.toString()
    }
}
