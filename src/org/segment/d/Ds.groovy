package org.segment.d

import com.alibaba.druid.pool.DruidDataSource
import com.alibaba.druid.pool.DruidDataSourceStatValue
import com.alibaba.druid.stat.JdbcSqlStatValue
import groovy.sql.Sql
import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import org.apache.commons.beanutils.PropertyUtils
import org.apache.commons.codec.digest.DigestUtils

import java.util.concurrent.ScheduledThreadPoolExecutor
import java.util.concurrent.TimeUnit

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
        h2, h2mem, h2Local, mysql, postgresql
    }

    // use static method dbType to create one instance
    private Ds() {}

    private static Map<String, Ds> cached = [:]

    static Ds one(String name = 'default') {
        cached[name]
    }

    Ds cacheAs(String name = 'default') {
        cached[name] = this
        this
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
                    "jdbc:posgresql://${ip}:${port}/${db}".toString()
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
        log.info 'done create druid data source ' + myName()

        this.sql = new Sql(dataSource)
        this.isConnected = true

        startCollectDruidDataSourceStatsInterval()
        this
    }

    static Ds h2mem(String db) {
        dbType(DBType.h2mem).connect(null, 0, db, 'sa', null)
    }

    static Ds h2Local(String db) {
        dbType(DBType.h2Local).connect(null, 0, db, 'sa', null)
    }

    synchronized void closeConnect() {
        if (sql) {
            sql.close()
            log.info 'close connect - ' + myName()
        }
        if (dataSource) {
            if (collector) {
                collector.shutdown()
                log.info 'stop collect druid data source stats interval - ' + myName()
            }
            dataSource.close()
            log.info 'close druid data source - ' + myName()
        }
        isConnected = false
    }

    static void oneCloseConnect(String name = 'default') {
        one(name)?.closeConnect()
    }

    @CompileStatic
    static class StatInfo {
        Date time
        DruidDataSourceStatValue statValue
        List<JdbcSqlStatValue> sqlList
    }

    private ScheduledThreadPoolExecutor collector

    private String dataSourceNameForStat

    private MetricGaugeRegister metricGaugeRegister = new MetricGaugeRegister() {

        Map<String, MetricGaugeGetter> gauges = [:]

        @Override
        void register(String name, MetricGaugeGetter getter) {
            gauges[name] = getter
        }
    }

    Ds metricGaugeRegister(MetricGaugeRegister metricGaugeRegister) {
        this.metricGaugeRegister = metricGaugeRegister
        this
    }

    Ds registerDataSourceStats(String dataSourceNameForStat = 'default') {
        assert isConnected && dataSource
        this.dataSourceNameForStat = dataSourceNameForStat

        final String pre = metricKeyForDataSourcePre + dataSourceNameForStat + '-'

        metricGaugeRegister.register(pre + 'collectTime') {
            statInfo ? statInfo.time.format(D.ymdhms) : ''
        }
        metricGaugeRegister.register(pre + 'sqlSet') {
            alreadyRegisterSqlSet
        }

        def props = new DruidDataSourceStatValue().properties.findAll { k, v ->
            v != null && v instanceof Number
        }
        props.each { k, v ->
            metricGaugeRegister.register(pre + k) {
                if (!statInfo) {
                    return 0
                }

                def statValue = statInfo.statValue
                def val = PropertyUtils.getProperty(statValue, k.toString())
                if (val == null) {
                    return 0
                }
                if (val instanceof long[]) {
                    long[] arr = val as long[]
                    return arr.sum() / arr.length
                }
                val
            }
        }
        log.info 'register data source stat for - ' + dataSourceNameForStat
        this
    }

    private void startCollectDruidDataSourceStatsInterval() {
        def now = new Date()
        int sec = now.seconds
        int nextSecDelay = 10 - (sec % 10)
        collector = new ScheduledThreadPoolExecutor(1,
                new NamedThreadFactory(metricKeyForDataSourcePre + myName() + '-Collector'))
        collector.scheduleAtFixedRate({
            def statValue = dataSource.statValueAndReset
            def sqlList = statValue.sqlList ?: new ArrayList<JdbcSqlStatValue>()

            statInfo = new StatInfo(time: new Date(), statValue: statValue, sqlList: sqlList)
            // need not metric registry
            if (!dataSourceNameForStat) {
                return
            }
            if (!(druidDataSourceFilters?.contains('stat') || druidDataSourceFilters?.contains('mergeStat'))) {
                return
            }
            if (alreadyRegisterSqlSet.size() >= maxStatSqlSize) {
                log.warn 'already stat sql size - ' + maxStatSqlSize
                return
            }
            if (!sqlList) {
                return
            }
            def sortedSqlList = sqlList.sort { a, b ->
                a.sql <=> b.sql
            }

            def props = new JdbcSqlStatValue().properties.findAll { k, v ->
                k.toString() in sqlStatCollectPropertySet && (v != null && v instanceof Number)
            }

            sortedSqlList.each {
                def targetSql = it.sql
                if (targetSql in alreadyRegisterSqlSet) {
                    return
                }

                final String namePre = metricKeyForSqlPre + dataSourceNameForStat + '-' + DigestUtils.md5Hex(targetSql) + '__'
                props.each { k, v ->
                    // executeAndResultSetHoldTime no getter method
                    if ('executeAndResultSetHoldTime' == k.toString()) {
                        return
                    }

                    metricGaugeRegister.register(namePre + k, {
                        if (!statInfo) {
                            return 0
                        }
                        def targetOne = statInfo.sqlList.find { x ->
                            x.sql == targetSql
                        }
                        if (!targetOne) {
                            return 0
                        }

                        def val = PropertyUtils.getProperty(targetOne, k.toString())
                        if (val == null) {
                            return 0
                        }
                        if (val instanceof long[]) {
                            long[] arr = val as long[]
                            return arr.sum() / arr.length
                        }
                        val
                    })
                }
                alreadyRegisterSqlSet << targetSql
                log.info 'register sql stat for ' + targetSql
            }
        }, nextSecDelay * 1000, collectDruidDataSourceStatsIntervalMillis, TimeUnit.MILLISECONDS)
        log.info 'start collect druid data source stats interval - ' + dataSource.name
    }

    private volatile StatInfo statInfo
    private Set<String> alreadyRegisterSqlSet = []
    private int maxStatSqlSize = 100
    private long collectDruidDataSourceStatsIntervalMillis = 30 * 1000

    Ds maxStatSqlSize(int maxStatSqlSize) {
        assert maxStatSqlSize > 0
        this.maxStatSqlSize = maxStatSqlSize
        this
    }

    Ds collectDruidDataSourceStatsIntervalMillis(long collectDruidDataSourceStatsIntervalMillis) {
        this.collectDruidDataSourceStatsIntervalMillis = collectDruidDataSourceStatsIntervalMillis
        this
    }

    JdbcSqlStatValue findSqlStatByKeyword(String keyword) {
        if (!statInfo) {
            return null
        }
        statInfo.sqlList.find { it.sql.contains(keyword) }
    }

    List<JdbcSqlStatValue> findSqlStatListByKeyword(String keyword) {
        if (!statInfo) {
            return null
        }
        statInfo.sqlList.findAll { it.sql.contains(keyword) }
    }

    private static final String metricKeyForSqlPre = 'Druid-Sql-'
    private static final String metricKeyForDataSourcePre = 'Druid-DS-'

    private Set<String> sqlStatCollectPropertySet = new HashSet<String>(sqlStatCollectPropertySetDefault)

    private static Set<String> sqlStatCollectPropertySetDefault = []

    static {
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
            sqlStatCollectPropertySetDefault << it.toString().replace('one.', '')
        }
    }

    Ds addSqlStatCollectProperty(String... property) {
        def props = new JdbcSqlStatValue().properties.keySet().collect { it.toString() }
        for (one in property) {
            if (one in props) {
                sqlStatCollectPropertySet << one
            }
        }
        this
    }
}
