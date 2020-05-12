'''
        def one = new JdbcSqlStatValue()
        one.executeCount
        one.executeErrorCount
        one.executeMillisMax
        one.executeBatchSizeMax
        one.fetchRowCountMax
        one.histogram_1_10
        one.histogram_10_100
        one.histogram_100_1000
        one.histogram_1000_10000
        '''.readLines().collect { it.trim() }.findAll { it.contains('one.') }.each {
    println it
}