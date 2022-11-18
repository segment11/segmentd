package org.segment.d

import com.alibaba.druid.pool.DruidDataSource
import com.alibaba.druid.pool.DruidDataSourceStatValue
import com.alibaba.druid.stat.JdbcSqlStatValue
import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import io.prometheus.client.Collector
import org.apache.commons.beanutils.PropertyUtils

@CompileStatic
@Slf4j
class MetricGaugeCollector extends Collector {
    protected List<String> labels = []
    protected List<String> labelValues = []

    protected Map<Long, String> sqlHashHolder = [:]

    private Map<String, Double> gauges = [:]
    private Map<String, Double> sqlGauges = [:]

    protected int maxStatSqlSize = 100
    protected String druidDataSourceFilters = 'mergeStat'
    protected DruidDataSource dataSource
    protected List<JdbcSqlStatValue> sqlList

    private static final String metricKeyForSqlPre = 'sql_'
    private static final String metricKeyForDataSourcePre = 'ds_'

    Set<String> sqlStatCollectPropertySet = new HashSet<String>(sqlStatCollectPropertySetDefault)

    private static Set<String> sqlStatCollectPropertySetDefault = []

    static {
        '''
        def one = new JdbcSqlStatValue()
        one.executeCount
        one.executeErrorCount
        one.executeMillisMax
        one.executeBatchSizeMax
        one.fetchRowCountMax
        one.histogram_100_1000
        one.histogram_1000_10000
        '''.readLines().collect { it.trim() }.findAll { it.contains('one.') }.each {
            sqlStatCollectPropertySetDefault << it.toString().replace('one.', '')
        }
    }

    private void collectDataSourceStats(DruidDataSourceStatValue statValue) {
        def props = new DruidDataSourceStatValue().properties.findAll { k, v ->
            !k.toString().startsWith('txn_') && v != null && (v instanceof Number || v instanceof long[])
        }
        props.each { k, v ->
            def propertyVal = PropertyUtils.getProperty(statValue, k.toString())
            if (propertyVal == null) {
                return
            }

            double val = 0
            if (propertyVal instanceof long[]) {
                long[] arr = propertyVal as long[]
                val = arr.sum() / arr.length
            } else if (propertyVal instanceof Number) {
                val = (propertyVal as Number).doubleValue()
            }
            gauges[metricKeyForDataSourcePre + D.toUnderline(k.toString())] = val
        }
    }

    protected void collectDruidStats() {
        def statValue = dataSource.statValueAndReset
        collectDataSourceStats(statValue)

        // collect sql stats
        def sqlStatList = statValue.sqlList ?: new ArrayList<JdbcSqlStatValue>()
        if (!sqlStatList) {
            return
        }
        sqlList = sqlStatList

        if (!(druidDataSourceFilters?.contains('stat') || druidDataSourceFilters?.contains('mergeStat'))) {
            return
        }

        def sortedSqlStatList = sqlStatList.sort { a, b ->
            a.sql <=> b.sql
        }

        def props = new JdbcSqlStatValue().properties.findAll { k, v ->
            k.toString() in sqlStatCollectPropertySet && v != null && (v instanceof Number || v instanceof long[])
        }

        sortedSqlStatList.each { sqlStatValue ->
            if (sqlHashHolder.size() >= maxStatSqlSize) {
                return
            }
            def sqlHash = Math.abs(sqlStatValue.sqlHash)
            sqlHashHolder[sqlHash] = sqlStatValue.sql

            final String namePre = sqlHash + '_' + metricKeyForSqlPre
            props.each { k, v ->
                // executeAndResultSetHoldTime no getter method
                if ('executeAndResultSetHoldTime' == k.toString()) {
                    return
                }

                double val = 0
                def propertyVal = PropertyUtils.getProperty(sqlStatValue, k.toString())
                if (propertyVal == null) {
                    return
                }
                if (propertyVal instanceof long[]) {
                    long[] arr = propertyVal as long[]
                    val = arr.sum() / arr.length
                } else if (propertyVal instanceof Number) {
                    val = (propertyVal as Number).doubleValue()
                }
                sqlGauges[namePre + D.toUnderline(k.toString())] = val
            }
        }
    }

    @Override
    List<MetricFamilySamples> collect() {
        collectDruidStats()

        List<MetricFamilySamples> list = []

        List<MetricFamilySamples.Sample> dsSamples = []
        def dsMfs = new MetricFamilySamples('druid_ds_stats', Collector.Type.COUNTER,
                'Druid dataSource stats.', dsSamples)
        list.add(dsMfs)
        gauges.each { k, v ->
            dsSamples.add(new MetricFamilySamples.Sample(k, labels, labelValues, v))
        }

        List<MetricFamilySamples.Sample> sqlSamples = []
        def sqlMfs = new MetricFamilySamples('druid_sql_stats', Collector.Type.COUNTER,
                'Druid dataSource each sql execute stats.', sqlSamples)
        list.add(sqlMfs)
        sqlGauges.each { k, v ->
            int index = k.indexOf('_')
            String sqlHash = k[0..<index]
            String key = k[(index + 1)..-1]
            List<String> sqlSetLabel = new ArrayList<>(labels)
            List<String> sqlSetLabelValues = new ArrayList<>(labelValues)
            sqlSetLabel.add('sql')
            sqlSetLabelValues.add(sqlHash.toString())
            sqlSamples.add(new MetricFamilySamples.Sample(key, sqlSetLabel, sqlSetLabelValues, v))
        }

        list
    }
}