package org.segment.d

import com.fasterxml.jackson.annotation.JsonIgnore
import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import org.segment.d.dialect.MySQLDialect
import org.segment.d.dialect.OracleDialect

import java.text.SimpleDateFormat

@CompileStatic
@Slf4j
abstract class Record<V extends Record> implements Serializable {
    protected D d

    void setD(D d) {
        this.d = d
        this
    }

    Record<V> withD(D d) {
        this.d = d
        this
    }

    // override this method to use one specific D instance, in case of transaction
    D useD() {
        d
    }

    abstract String pk()

    private String tableName

    String tbl() {
        if (tableName == null) {
            def name = this.class.simpleName
            if (name.endsWith('DTO')) {
                tableName = D.toUnderline(name[0..-4])
            } else {
                tableName = D.toUnderline(name)
            }
        }
        tableName
    }

    Record<V> tableNameReset(String tableName) {
        this.tableName = tableName
        this
    }

    Record<V> setProperties(Map<String, Object> properties) {
        for (entry in properties) {
            setProperty(entry.key, entry.value)
        }
        this
    }

    private Map<String, Object> extProperties = [:]

    Object prop(String property, Object value = null) {
        if (value == null) {
            return extProperties[property]
        }

        extProperties[property] = value
        value
    }

    Object removeProp(String property) {
        extProperties.remove(property)
    }

    static final Set<String> recordFieldNameList = new HashSet<>()
    static {
        for (field in BeanReflector.getClassFields(Record)) {
            recordFieldNameList << field.name
        }
    }

    Map<String, Object> rawProps(boolean withExt = false) {
        Map<String, Object> r = [:]
        for (field in BeanReflector.getClassFields(this.class, Record)) {
            def name = field.name
            def methodName = 'get' + name[0].toUpperCase() + name[1..-1]
            def reflector = BeanReflector.get(this.class, methodName)
            r[name] = reflector.invoke(this)
        }
        if (withExt) {
            r.putAll(extProperties)
        }
        r
    }

    <T> T asType(Class<T> clz, Map<String, String> fieldMapping = null) {
        if (clz == this.class) {
            return (T) this
        }

        def props = rawProps(true)

        T t = clz.getDeclaredConstructor().newInstance()
        for (field in BeanReflector.getClassFields(clz, Record)) {
            def rawFieldName = field.name
            def name = rawFieldName
            if (fieldMapping) {
                name = fieldMapping[name]
            }
            if (name == null) {
                name = rawFieldName
            }
            def value = props[name]
            if (value != null) {
                def methodName = 'set' + rawFieldName[0].toUpperCase() + rawFieldName[1..-1]
                def reflector = BeanReflector.get(t.class, methodName, field.type)
                reflector.invoke(t, value)
            }
        }
        t
    }

    private String tableFieldsCached

    // join with ','
    String tableFields() {
        if (tableFieldsCached == null) {
            tableFieldsCached = rawProps().keySet().collect { D.toUnderline(it) }.join(',')
        }
        tableFieldsCached
    }

    Map<String, Object> rawPropsWithValue() {
        rawProps().findAll { it.value != null }
    }

    Integer add() {
        useD().add(rawPropsWithValue(), tbl()) as Integer
    }

    int update() {
        useD().update(rawPropsWithValue(), tbl(), D.toUnderline(pk()))
    }

    // only delete one record, use primary key as where condition
    int delete() {
        String limitSuffix
        def dialect = useD().dialect
        if (dialect instanceof MySQLDialect) {
            limitSuffix = 'limit 1'
        } else if (dialect instanceof OracleDialect) {
            limitSuffix = 'and rownum = 1'
        } else {
            limitSuffix = ''
        }

        String sql = "delete from ${tbl()} where ${D.toUnderline(pk())} = ? " + limitSuffix
        useD().exeUpdate(sql, [getProperty(D.toCamel(pk()))])
    }

    int deleteAll() {
        def queryProps = rawPropsWithValue()
        def where = whereClause.length() ? whereClause.toString() : ('and ' +
                queryProps.collect {
                    D.toUnderline(it.key) + ' = ?'
                }.join(' and '))
        List args = whereArgs ?: new ArrayList<>(queryProps.values())
        assert where || args

        String sql = "delete from ${tbl()} where 1 = 1 ${where}"
        useD().exeUpdate(sql, args)
    }

    protected static String valueToSqlString(Object obj) {
        if (obj instanceof Date) {
            def date = obj as Date
            return "to_date('${new SimpleDateFormat(D.DATE_FORMAT_STR).format(date)}', 'yyyy-MM-dd hh24:mi:ss')"
        } else if (obj instanceof String) {
            return "'${obj}'"
        } else {
            return obj.toString()
        }
    }

    // not use prepared statement, just use string concat
    String generateSqlWithoutArgs() {
        def props = rawPropsWithValue()
        D.concatStr('insert into ', tbl(), '(', props.keySet().join(','), ') values (',
                props.values().collect { valueToSqlString(it) }.join(','), ')')
    }

    // you can override this method to limit the number of records to query once
    protected int maxQueryNumOnce() {
        1000
    }

    private StringBuilder whereClause = new StringBuilder()
    private List whereArgs = []
    private String orderByClause

    private String fieldsToQuery

    Record<V> queryFields(String fieldsToQuery) {
        this.fieldsToQuery = D.toUnderline(fieldsToQuery)
        this
    }

    Record<V> queryFieldsExclude(String fieldsToQueryExclude) {
        def exclude = D.toUnderline(fieldsToQueryExclude)
        this.fieldsToQuery = (tableFields().split(',') - exclude.split(',')).join(',')
        this
    }

    Record<V> whereReset() {
        whereClause.delete(0, whereClause.length())
        whereArgs.clear()
        orderByClause = null
        this
    }

    Record<V> orderBy(String orderByClause) {
        this.orderByClause = orderByClause
        this
    }

    Record<V> where(boolean flag, String clause, Object... args = null) {
        if (flag) {
            whereClause << ' and ('
            whereClause << clause
            whereClause << ')'
            if (args != null) {
                for (arg in args) {
                    if (arg != null) {
                        whereArgs << arg
                    }
                }
            }
        }
        this
    }

    Record<V> where(String clause, Object... args = null) {
        where(true, clause, args)
    }

    Record<V> whereIn(String field, List list, boolean withQuote = true) {
        def wrapString = list.collect {
            if (withQuote) {
                return "'" + it + "'"
            } else {
                return it
            }
        }.join(',')
        where("${field} in (${wrapString})")
    }

    Record<V> whereNotIn(String field, List list, boolean withQuote = true) {
        def wrapString = list.collect {
            if (withQuote) {
                return "'" + it + "'"
            } else {
                return it
            }
        }.join(',')
        where("${field} not in (${wrapString})")
    }

    Record<V> noWhere() {
        where('1=1')
    }

    // if you do not give a limit, it will use maxQueryNumOnce() as the limit, default is 1000
    List<V> list(int maxLimitNum = 0) {
        int maxLimit = maxLimitNum ?: maxQueryNumOnce()
        def fields = fieldsToQuery ?: tableFields()

        def queryProps = rawPropsWithValue()
        // use where clause first, if no where clause, use properties as where clause
        def where = whereClause.length() ? whereClause.toString() : ('and ' +
                queryProps.collect {
                    D.toUnderline(it.key) + ' = ?'
                }.join(' and '))
        List args = whereArgs ?: new ArrayList<>(queryProps.values())

        def dialect = useD().dialect

        String sql
        boolean isQueryPagination = pageNum != 0

        def orderBy = orderByClause ? 'order by ' + orderByClause : ''
        if (isQueryPagination) {
            def innerSql = "select ${fields} from ${tbl()} where 1 = 1 ${where} ${orderBy}".toString()
            def pager = new Pager<V>(pageNum, pageSize)
            sql = dialect.generatePaginationSql(innerSql, pager.start, pageSize)
        } else {
            if (dialect.isLimitSupport()) {
                sql = "select ${fields} from ${tbl()} where 1 = 1 ${where} ${orderBy} limit ${maxLimit}"
            } else {
                if (orderBy) {
                    sql = "select * from (select ${fields} from ${tbl()} where 1 = 1 ${where} ${orderBy}) where rownum <= ${maxLimit}"
                } else {
                    sql = "select ${fields} from ${tbl()} where 1 = 1 ${where} and rownum <= ${maxLimit}"
                }
            }
        }
        if (log.isDebugEnabled()) {
            log.debug(sql)
            log.debug(args.toString())
        }

        List<V> list = useD().query(sql, args, this.class as Class<V>)
        if (list && isQueryPagination) {
            def pager = new Pager<V>(pageNum, pageSize)
            def innerSql = "select ${D.toUnderline(pk())} from ${tbl()} where 1 = 1 ${where}".toString()
            def totalCount = useD().one(dialect.generateCountSql(innerSql), args, Integer)
            if (totalCount != null) {
                pager.totalCount = totalCount.intValue()
                V first = list[0]
                first.pager = pager
            }
        }
        list
    }

    @JsonIgnore
    int pageNum = 0
    @JsonIgnore
    int pageSize = 10

    protected Pager<V> pager

    Pager<V> listPager() {
        listPager(pageNum, pageSize)
    }

    Pager<V> listPager(int pageNum, int pageSize = 10) {
        assert pageNum > 0 && pageSize > 0
        assert pageSize <= maxQueryNumOnce()

        this.pageNum = pageNum
        this.pageSize = pageSize
        def list = list()
        if (!list) {
            return new Pager<V>(pageNum, pageSize)
        }

        def pager = list[0].pager
        pager.list = list
        pager
    }

    V one() {
        def list = list(1)
        list ? list[0] : null
    }

    void load() {
        def one = one()
        if (!one) {
            return
        }
        for (entry in one.rawProps()) {
            this.setProperty(entry.key, entry.value)
        }
    }
}
