package org.segment.d.dialect


import spock.lang.Specification

class DialectTest extends Specification {

    def 'pagination count sql'() {
        given:
        def dialect = new MySQLDialect()
        expect:
        dialect.generateCountSql('select * from a') == 'select count(*) as X_TOTAL_COUNT from (select * from a) ttt'
    }

    def 'pagination limit sql'() {
        given:
        def dialect = new MySQLDialect()
        def dialectOra = new OracleDialect()
        def dialectPG = new PGDialect()
        expect:
        dialect.isLimitSupport()
        !dialectOra.isLimitSupport()
        dialectPG.isLimitSupport()
        dialect.generatePaginationSql('select * from a', 10, 10) ==
                'select ttt.* from (select * from a) ttt limit 10,10'
        dialectOra.generatePaginationSql('select * from a', 10, 10) ==
                'select ttt.* from (select * from a) ttt where rownum > 10 and rownum <= 20'
        dialectPG.generatePaginationSql('select * from a', 10, 10) ==
                'select ttt.* from (select * from a) ttt limit 10 offset 10'
    }
}
