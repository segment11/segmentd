package org.segment.d

import com.alibaba.druid.pool.DruidDataSource
import com.alibaba.druid.stat.JdbcSqlStatValue
import groovy.sql.Sql
import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import io.prometheus.client.CollectorRegistry

@CompileStatic
@Slf4j
class Ds {
    static final String ENCODING = 'utf-8'

    @CompileStatic
    private static class DBTypeInfo {
        String driver
        DBJdbcUrlGenerator generator
    }

    @CompileStatic
    static interface DBJdbcUrlGenerator {
        String generate(String ip, int port, String db)
    }

    @CompileStatic
    static enum DBType {
        h2, h2mem, h2Local, mysql, postgresql, oracle
    }

    // use static method dbType to create one instance
    private Ds() {}

    private static Map<String, Ds> cached = [:]

    static Ds one(String name = 'default') {
        cached[name]
    }

    static Ds remove(String name = 'default') {
        cached.remove(name)
    }

    Ds cacheAs(String name = 'default') {
        myNameCached = name
        cached[name] = this
        this
    }

    static void disconnectOne(String name = 'default') {
        one(name)?.closeConnect()
    }

    static synchronized void disconnectAll() {
        cached.each { k, v ->
            v.closeConnect()
        }
    }

    private String jdbcUrl
    private DBType dbType
    private String dbTypeOtherName
    private String user
    private String password
    private DruidDataSource dataSource
    private DruidDataSourceInitHandler dataSourceInitHandler
    private String druidDataSourceFilters = 'mergeStat'
    private Map<String, Object> urlParams = [:]

    private Sql sql

    Sql getSql() {
        sql
    }

    DruidDataSource getDataSource() {
        return dataSource
    }

    static {
        dbTypeOthers[DBType.h2.name()] = new DBTypeInfo(driver: 'org.h2.Driver',
                generator: { String ip, int port, String db ->
                    "jdbc:h2:tcp://${ip}${port == -1 ? '' : (':' + port)}/${db}".toString()
                })
        dbTypeOthers[DBType.h2mem.name()] = new DBTypeInfo(driver: 'org.h2.Driver',
                generator: { String ip, int port, String db ->
                    "jdbc:h2:mem:${db}".toString()
                })
        dbTypeOthers[DBType.h2Local.name()] = new DBTypeInfo(driver: 'org.h2.Driver',
                generator: { String ip, int port, String db ->
                    "jdbc:h2:${db}".toString()
                })
        dbTypeOthers[DBType.mysql.name()] = new DBTypeInfo(driver: 'com.mysql.jdbc.Driver',
                generator: { String ip, int port, String db ->
                    "jdbc:mysql://${ip}:${port}/${db}".toString()
                })
        dbTypeOthers[DBType.postgresql.name()] = new DBTypeInfo(driver: 'org.postgresql.Driver',
                generator: { String ip, int port, String db ->
                    "jdbc:postgresql://${ip}:${port}/${db}".toString()
                })
        dbTypeOthers[DBType.oracle.name()] = new DBTypeInfo(driver: 'oracle.jdbc.driver.OracleDriver',
                // db -> SID
                generator: { String ip, int port, String db ->
                    "jdbc:oracle:thin:@${ip}:${port}:${db}".toString()
                })
    }

    private static Map<String, DBTypeInfo> dbTypeOthers = [:]

    static void register(String name, String driver, DBJdbcUrlGenerator generator) {
        assert name && driver && generator
        dbTypeOthers[name] = new DBTypeInfo(driver: driver, generator: generator)
    }

    static Ds dbType(DBType dbType) {
        def ds = new Ds(dbType: dbType)
        if (dbType == DBType.mysql) {
            ds.urlParam('useSSL', false)
            ds.urlParam('useUnicode', true)
            ds.urlParam('characterEncoding', ENCODING)
            ds.urlParam('serverTimezone', 'UTC')
        }
        ds
    }

    static Ds dbType(String dbTypeOtherName) {
        new Ds(dbTypeOtherName: dbTypeOtherName)
    }

    private String generateJdbcUrl(String ip, int port, String db) {
        assert dbType || (dbTypeOtherName && dbTypeOthers[dbTypeOtherName])
        String name = dbType ? dbType.name() : dbTypeOtherName
        return dbTypeOthers[name].generator.generate(ip, port, db) + generateUrlParamSuffix()
    }

    private String getDriver() {
        assert dbType || (dbTypeOtherName && dbTypeOthers[dbTypeOtherName])
        String name = dbType ? dbType.name() : dbTypeOtherName
        return dbTypeOthers[name].driver
    }

    private String generateUrlParamSuffix() {
        if (!urlParams) {
            return ''
        }
        // h2 tcp mode only need db file path without '?'
        if (dbType && dbType == DBType.h2) {
            return ''
        }
        '?' + urlParams.collect {
            it.key + '=' + it.value
        }.join('&')
    }

    Ds dataSourceInitHandler(DruidDataSourceInitHandler dataSourceInitHandler) {
        this.dataSourceInitHandler = dataSourceInitHandler
        this
    }

    Ds filters(String druidDataSourceFilters) {
        this.druidDataSourceFilters = druidDataSourceFilters
        this
    }

    Ds urlParam(String key, Object value, boolean isOverwrite = false) {
        if (isOverwrite) {
            urlParams[key] = value
        } else {
            if (urlParams[key] == null) {
                urlParams[key] = value
            }
        }
        this
    }

    private String myNameCached

    private String myName() {
        if (myNameCached) {
            return myNameCached
        }

        String urlSimple = jdbcUrl.contains('?') ? jdbcUrl[0..<jdbcUrl.indexOf('?')] : jdbcUrl
        myNameCached = (user ?: 'anon') + '_' + urlSimple.replaceAll(/[:|=|\/|\.]/, '_')
        myNameCached
    }

    volatile boolean isConnected = false

    synchronized Ds connect(String ip, int port, String db, String user, String password) {
        assert !dataSource
        assert dbType || dbTypeOtherName

        this.jdbcUrl = generateJdbcUrl(ip, port, db)
        log.info jdbcUrl
        this.user = user
        this.password = password

        this.sql = Sql.newInstance(jdbcUrl, user, password, getDriver())
        this.isConnected = true
        this
    }

    synchronized Ds connectWithPool(String ip, int port, String db, String user, String password, int minPoolSize = 5, int maxPoolSize = 10) {
        assert !dataSource
        assert dbType || dbTypeOtherName

        this.jdbcUrl = generateJdbcUrl(ip, port, db)
        this.user = user
        this.password = password

        dataSource = new DruidDataSource()
        dataSource.driverClassName = getDriver()
        dataSource.url = jdbcUrl
        dataSource.username = user
        dataSource.password = password
        dataSource.name = myName()

        dataSource.initialSize = minPoolSize
        dataSource.minIdle = minPoolSize
        dataSource.maxActive = maxPoolSize
        dataSource.maxWait = 1000 * 30
        dataSource.testWhileIdle = true
        dataSource.testOnBorrow = true
        dataSource.maxPoolPreparedStatementPerConnectionSize = 5
        dataSource.timeBetweenEvictionRunsMillis = 1000 * 60
        if (druidDataSourceFilters) {
            dataSource.filters = druidDataSourceFilters
        }
        if (dataSourceInitHandler) {
            dataSourceInitHandler.set(dataSource)
        }
        dataSource.init()
        log.info 'done create druid data source {}', myName()

        this.sql = new Sql(dataSource)
        this.isConnected = true

        metricGaugeCollector.maxStatSqlSize = maxStatSqlSize
        metricGaugeCollector.druidDataSourceFilters = druidDataSourceFilters
        metricGaugeCollector.dataSource = dataSource
        this
    }

    static Ds h2mem(String db) {
        dbType(DBType.h2mem).connect(null, 0, db, 'sa', null)
    }

    static Ds h2Local(String db) {
        dbType(DBType.h2Local).connect(null, 0, db, 'sa', null)
    }

    static Ds h2LocalWithPool(String db, String dsName) {
        dbType(DBType.h2Local).cacheAs(dsName).
                connectWithPool(null, 0, db, 'sa', null, 1, 1)
    }

    synchronized void closeConnect() {
        if (sql) {
            sql.close()
            log.info 'close connect - {}', myName()
        }
        if (dataSource) {
            dataSource.close()
            log.info 'close druid data source - {}', myName()
        }
        isConnected = false
    }

    private MetricGaugeCollector metricGaugeCollector = new MetricGaugeCollector()

    Map<Long, String> sqlHashHolder() {
        metricGaugeCollector.sqlHashHolder
    }

    Ds extendLabelValue(String label, String labelValue) {
        metricGaugeCollector.labels.add(label)
        metricGaugeCollector.labelValues.add(labelValue)
        this
    }

    Ds export(CollectorRegistry registry = CollectorRegistry.defaultRegistry) {
        metricGaugeCollector.labels.add('ds_name')
        metricGaugeCollector.labelValues.add(myName())
        registry.register(metricGaugeCollector)
        this
    }

    private int maxStatSqlSize = 100

    Ds maxStatSqlSize(int maxStatSqlSize) {
        assert maxStatSqlSize > 0
        this.maxStatSqlSize = maxStatSqlSize
        this
    }

    JdbcSqlStatValue findSqlStatByKeyword(String keyword) {
        if (!metricGaugeCollector.sqlList) {
            return null
        }
        metricGaugeCollector.sqlList.find { it.sql.toUpperCase().contains(keyword.toUpperCase()) }
    }

    List<JdbcSqlStatValue> findSqlStatListByKeyword(String keyword) {
        if (!metricGaugeCollector.sqlList) {
            return null
        }
        metricGaugeCollector.sqlList.findAll { it.sql.toUpperCase().contains(keyword.toUpperCase()) }
    }

    Ds addSqlStatCollectProperty(String... property) {
        def props = new JdbcSqlStatValue().properties.keySet().collect { it.toString() }
        for (one in property) {
            if (one in props) {
                metricGaugeCollector.sqlStatCollectPropertySet << one
            }
        }
        this
    }
}
