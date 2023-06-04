package org.segment.d.dialect

import groovy.transform.CompileStatic

@CompileStatic
class OracleDialect extends MySQLDialect {
    @Override
    String generatePaginationSql(String sql, int start, int limit) {
        def sb = new StringBuilder()
        sb << 'select ttt.* from ('
        sb << sql.trim()
        sb << ') ttt where rownum > '
        sb << start
        sb << ' and rownum <= '
        sb << (start + limit)
        sb.toString()
    }

    @Override
    boolean isLimitSupport() {
        false
    }
}
