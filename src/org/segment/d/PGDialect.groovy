package org.segment.d

import groovy.transform.CompileStatic

@CompileStatic
class PGDialect extends MySQLDialect {
    @Override
    String generatePaginationSql(String sql, int start, int limit) {
        def sb = new StringBuilder()
        sb << 'select ttt.* from ('
        sb << sql.trim()
        sb << ') ttt limit '
        sb << limit
        sb << ' offset '
        sb << start
        sb.toString()
    }
}
