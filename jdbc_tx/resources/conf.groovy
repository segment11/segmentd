import com.alibaba.druid.pool.DruidDataSource
import com.segment.base.OneBean
import org.segment.d.D
import org.segment.d.MySQLDialect
import org.springframework.jdbc.datasource.DataSourceTransactionManager
import org.springframework.jdbc.datasource.TransactionAwareDataSourceProxy

beans {
    dataSourceTarget(DruidDataSource) {
        name = 'test'
        driverClassName = 'org.h2.Driver'
        url = 'jdbc:h2:tcp://localhost:8082/~/test'
        username = 'sa'
        password = ''
        initialSize = 2
        maxActive = 5
        filters = 'mergeStat'
    }

    dataSource(TransactionAwareDataSourceProxy, dataSourceTarget)

    transactionManager(DataSourceTransactionManager) {
        dataSource = dataSourceTarget
    }

    dialect(MySQLDialect)

    d(D, dataSource, dialect)

    oneBean(OneBean) {
        d = d
        tm = transactionManager
    }
}
