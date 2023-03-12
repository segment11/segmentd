package org.segment.d

import groovy.sql.Sql
import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import org.apache.commons.beanutils.PropertyUtils
import org.codehaus.groovy.runtime.DefaultGroovyMethods
import org.segment.d.json.JSONFiled
import org.segment.d.json.JsonReader
import org.segment.d.json.JsonWriter

import javax.sql.DataSource
import java.lang.reflect.Array
import java.sql.Clob
import java.sql.ResultSet
import java.sql.Timestamp
import java.sql.Types
import java.text.SimpleDateFormat

@CompileStatic
@Slf4j
class D {

    private Sql db

    Sql getDb() {
        return db
    }

    private Dialect dialect

    Dialect getDialect() {
        return dialect
    }

    private boolean addReturnPk = true

    void addReturnPk(boolean addReturnPk) {
        this.addReturnPk = addReturnPk
    }

    private HashSet<String> skipProperties = new HashSet(skipPropertiesDefault)

    private static HashSet<String> skipPropertiesDefault = []
    static {
        skipPropertiesDefault << 'metaClass'
    }

    void addSkipProperties(String... properties) {
        for (property in properties) {
            skipProperties << property
        }
    }

    D(Sql db, Dialect dialect) {
        this.db = db
        this.dialect = dialect
    }

    D(DataSource ds, Dialect dialect) {
        this.db = new Sql(ds)
        this.dialect = dialect
    }

    D(Ds ds, Dialect dialect) {
        this.db = ds.sql
        this.dialect = dialect
    }

    void close() {
        this.db?.close()
    }

    static String toCamel(String str, boolean isFirstLowerCase = true) {
        if (str == null) {
            return null
        }
        if (str.length() <= 1) {
            return isFirstLowerCase ? str.toLowerCase() : str
        }

        def sb = new StringBuilder()
        def arr = str.toLowerCase().split('_')
        for (int i = 0; i < arr.length; i++) {
            def cc = arr[i]
            int len = cc.length()
            if (len == 0) {
                continue
            }

            if (i == 0 && isFirstLowerCase) {
                sb.append(cc)
            } else {
                sb.append(cc.substring(0, 1).toUpperCase())
                if (len > 1) {
                    sb.append(cc.substring(1))
                }
            }
        }
        sb.toString()
    }

    static String toUnderline(String str) {
        if (str == null) {
            return null
        }
        if (str.length() <= 1) {
            return str.toLowerCase()
        }

        String str2 = str.substring(0, 1).toLowerCase() + str.substring(1)

        def sb = new StringBuilder()
        def len = str2.length()
        for (int i = 0; i < len; i++) {
            def cc = str2.charAt(i)
            if (Character.isUpperCase(cc)) {
                sb.append('_')
                sb.append(Character.toLowerCase(cc))
            } else {
                sb.append(cc)
            }
        }
        sb.toString()
    }

    static Map<String, Object> toUnderline(Map<String, Object> map) {
        def r = new HashMap<String, Object>()
        map.each { k, v ->
            r[D.toUnderline(k)] = v
        }
        r
    }

    static Map<String, Object> bean2map(Object one, Set<String> skipPropertySet = null) {
        Set<String> skipSet = new HashSet<>(skipPropertiesDefault)
        if (skipPropertySet) {
            skipSet.addAll(skipPropertySet)
        }

        def r = new HashMap<String, Object>()
        for (field in one.getClass().getDeclaredFields()) {
            String name = field.name
            if (name in skipSet) {
                continue
            }

            if (PropertyUtils.isReadable(one, name)) {
                r[toUnderline(name)] = PropertyUtils.getProperty(one, name)
            }
        }
        if (one instanceof Expando) {
            Expando ex = one as Expando
            r.putAll(ex.properties)
        }
        r
    }

    static <T> T map2bean(Map<String, Object> row, Class<T> clz) {
        T t = clz.newInstance()
        if (t instanceof Expando) {
            Expando ex = t as Expando
            def props = ex.properties
            row.each { k, v ->
                props[k] = v
                props[D.toUnderline(k)] = v
            }
        }

        for (field in clz.getDeclaredFields()) {
            String name = field.name
            if (PropertyUtils.isWriteable(t, name)) {
                String key = D.toUnderline(name)
                if (!row.containsKey(key)) {
                    continue
                }
                PropertyUtils.setProperty(t, name, row.get(key))
            }
        }
        t
    }

    private static String generateSqlKeys(Set<String> set, String replaced = null) {
        def sb = new StringBuilder()
        int i = 0
        for (key in set) {
            if (replaced != null) {
                if (replaced.contains('$1')) {
                    sb.append(replaced.replace('$1', key))
                } else {
                    sb.append(replaced)
                }
            } else {
                sb.append(key)
            }
            if (i != set.size() - 1) {
                sb.append(',')
            }
            i++
        }
        sb.toString()
    }

    static String concatStr(String... strings) {
        def sb = new StringBuilder()
        for (string in strings) {
            sb.append(string)
        }
        sb.toString()
    }

    final static String ymdhms = 'yyyy-MM-dd HH:mm:ss'

    protected List transferArgs(List args) {
        if (args == null) {
            return null
        }

        def r = new LinkedList()
        for (obj in args) {
            if (obj instanceof Date) {
                Date date = obj as Date
                r.add(dialect instanceof OracleDialect ? new Timestamp(date.time) : new SimpleDateFormat(ymdhms).format(date))
            } else if (obj instanceof JSONFiled) {
                r.add(JsonWriter.instance.json(obj))
            } else {
                r.add(obj)
            }
        }
        r
    }

    static Map<Integer, Class> classTypeBySqlType = [:]
    static {
        classTypeBySqlType[Types.CHAR] = String
        classTypeBySqlType[Types.VARCHAR] = String
        classTypeBySqlType[Types.LONGNVARCHAR] = String
        classTypeBySqlType[Types.NUMERIC] = Double
        classTypeBySqlType[Types.DECIMAL] = Double
        classTypeBySqlType[Types.DOUBLE] = Double
        classTypeBySqlType[Types.BIT] = Boolean
        classTypeBySqlType[Types.BOOLEAN] = Boolean
        classTypeBySqlType[Types.TINYINT] = Byte
        classTypeBySqlType[Types.SMALLINT] = Short
        classTypeBySqlType[Types.INTEGER] = Integer
        classTypeBySqlType[Types.BIGINT] = Long
        classTypeBySqlType[Types.REAL] = Float
        classTypeBySqlType[Types.BINARY] = Array
        classTypeBySqlType[Types.VARBINARY] = Array
        classTypeBySqlType[Types.LONGVARBINARY] = Array
        classTypeBySqlType[Types.DATE] = Date
        classTypeBySqlType[Types.TIME] = Date
        classTypeBySqlType[Types.TIMESTAMP] = Date
    }

    static Class getClassTypeBySqlType(int type) {
        classTypeBySqlType[type] ?: String
    }

    protected Object resultSetValueClassTypeTransfer(Object obj) {
        if (obj == null) {
            return null
        }

        if (obj instanceof java.sql.Date) {
            java.sql.Date date = obj as java.sql.Date
            return new Date(date.time)
        } else if (obj instanceof java.sql.Timestamp) {
            java.sql.Timestamp date = obj as java.sql.Timestamp
            return new Date(date.time)
        } else if (obj instanceof BigDecimal) {
            BigDecimal decimal = obj as BigDecimal
            return decimal.doubleValue()
        } else if (obj instanceof Clob) {
            Clob clob = obj as Clob
            return toCharString(clob.characterStream)
        } else if (obj instanceof Byte) {
            def type = classTypeBySqlType[Types.TINYINT]
            if (type != Byte) {
                return DefaultGroovyMethods.asType(obj, type)
            }
        } else if (obj instanceof Short) {
            def type = classTypeBySqlType[Types.SMALLINT]
            if (type != Short) {
                return DefaultGroovyMethods.asType(obj, type)
            }
        } else {
            return obj
        }
    }

    private String toCharString(Reader reader) {
        char[] arr = new char[8 * 1024]
        def buffer = new StringBuilder()
        int numCharsRead
        while ((numCharsRead = reader.read(arr, 0, arr.length)) != -1) {
            buffer.append(arr, 0, numCharsRead)
        }
        reader.close()
        buffer.toString()
    }

    void exe(String sql, List args = null) {
        args == null ? db.execute(sql) : db.execute(sql, transferArgs(args))
    }

    int exeUpdate(String sql, List args = null) {
        args == null ? db.executeUpdate(sql) : db.executeUpdate(sql, transferArgs(args))
    }

    // for groovy Sql query result set iterate callback
    @CompileStatic
    @Slf4j
    private static class ToBeanCl<T> extends Closure {
        ToBeanCl(Object owner) {
            super(owner)
            this.d = owner as D
        }

        D d
        Class<T> clz
        List<T> r
        Map<String, String> colFieldMapping

        @Override
        Object call(Object obj) {
            ResultSet rs = (ResultSet) obj
            def md = rs.metaData

            Map<Integer, String> cols = [:]
            Map<Integer, Integer> colsType = [:]

            md.columnCount.times { int i ->
                int j = i + 1
                cols.put(j, md.getColumnName(j))
                colsType.put(j, md.getColumnType(j))
            }

            while (rs.next()) {
                r.add(rsToBean(rs, cols, colsType, clz))
            }
            return null
        }

        private T rsToBean(ResultSet rs, Map<Integer, String> cols, Map<Integer, Integer> colsType, Class<T> clz) {
            boolean isBaseType = clz.package.name == 'java.lang' || clz.name == 'java.util.Date'
            if (isBaseType) {
                def obj = d.resultSetValueClassTypeTransfer(rs.getObject(1))
                if (obj == null) {
                    return null
                }
                return DefaultGroovyMethods.asType(obj, clz)
            }

            T t = clz.newInstance()
            boolean isEx = t instanceof Expando
            boolean isRecord = t instanceof Record
            boolean isMap = t instanceof HashMap

            for (int i = 1; i <= cols.size(); i++) {
                String label = cols.get(i)
                def obj = d.resultSetValueClassTypeTransfer(rs.getObject(i))
                if (obj == null) {
                    continue
                }

                if (isEx) {
                    Expando ex = t as Expando
                    ex.setProperty(toCamel(label), obj)
                } else if (isMap) {
                    HashMap map = t as HashMap
                    map.put(toCamel(label), obj)
                } else {
                    String finalLabel
                    if (colFieldMapping != null) {
                        finalLabel = colFieldMapping[label.toLowerCase()] ?: label
                    } else {
                        finalLabel = label
                    }

                    def fieldType = d.getClassTypeBySqlType(colsType.get(i))
                    // check if is a json field
                    if (fieldType == String) {
                        String methodGetName = 'get' + toCamel(finalLabel, false)
                        def methodGet = BeanReflector.get(clz, methodGetName)
                        if (methodGet.returnType.interfaces.any { it == JSONFiled }) {
                            fieldType = methodGet.returnType
                            obj = JsonReader.instance.read(obj.toString(), fieldType)
                        }
                    }

                    String methodSetName = 'set' + toCamel(finalLabel, false)
                    def methodSet = BeanReflector.get(clz, methodSetName, fieldType)
                    if (methodSet == null) {
                        if (!isRecord) {
                            continue
                        } else {
                            Record record = t as Record
                            record.prop(toCamel(finalLabel), obj)
                            continue
                        }
                    }
                    methodSet.invoke(t, obj)
                }
            }

            t
        }
    }

    public <T> List<T> query(String sql, List args, Class<T> clz, Map<String, String> colFieldMapping = null) {
        List<T> r = new ArrayList()
        if (args == null) {
            args = new ArrayList()
        }

        ToBeanCl beanCl = new ToBeanCl(this)
        beanCl.clz = clz
        beanCl.r = r
        beanCl.colFieldMapping = colFieldMapping
        db.query(sql, transferArgs(args), beanCl)
        r
    }

    public <T> Pager<T> pagination(String sql, List args, Class<T> clz, int pageNum, int pageSize,
                                   Map<String, String> colFieldMapping = null) {
        def pager = new Pager(pageNum, pageSize)

        String paginationSql = dialect.generatePaginationSql(sql, pager.start, pageSize)
        List<T> resultList = query(paginationSql, args, clz)
        pager.list = resultList
        if (resultList) {
            Integer totalCount = one(dialect.generateCountSql(sql), args, Integer, colFieldMapping)
            pager.totalCount = totalCount != null ? totalCount.intValue() : 0
        }

        pager
    }

    public <T> List<T> query(String sql, Class<T> clz) {
        query(sql, null, clz)
    }

    public <T> T one(String sql, List args, Class<T> clz, Map<String, String> colFieldMapping = null) {
        def r = query(sql, args, clz, colFieldMapping)
        r ? r[0] : null
    }

    public <T> T one(String sql, Class<T> clz, Map<String, String> colFieldMapping = null) {
        def r = query(sql, null, clz, colFieldMapping)
        r ? r[0] : null
    }

    List<HashMap> query(String sql, List args = null, Map<String, String> colFieldMapping = null) {
        query(sql, args, HashMap, colFieldMapping)
    }

    HashMap one(String sql, List args = null) {
        def r = query(sql, args)
        r ? r[0] : null
    }

    Object add(Map<String, Object> one, String table, boolean isKeyCamel = true) {
        Map<String, Object> row = isKeyCamel ? toUnderline(one) : one
        String insertSql = concatStr('insert into ', table, ' (',
                generateSqlKeys(row.keySet()), ') values (', generateSqlKeys(row.keySet(), '?'), ')')
        List args = new ArrayList(row.values())
        if (addReturnPk) {
            def resultList = db.executeInsert(insertSql, transferArgs(args))
            if (!resultList) {
                return null
            }
            def subList = resultList[0]
            return subList ? subList[0] : null
        } else {
            db.executeUpdate(insertSql, transferArgs(args))
            return null
        }
    }

    int update(Map<String, Object> one, String table, String pkCol, boolean isKeyCamel = true) {
        Map<String, Object> row = isKeyCamel ? toUnderline(one) : one
        def pkVal = row.remove(pkCol)
        String updateSql = concatStr('update ', table, ' set ', generateSqlKeys(row.keySet(), '$1=?'),
                ' where ', pkCol, '=?')
        def args = new ArrayList(row.values())
        args.add(pkVal)
        db.executeUpdate(updateSql, transferArgs(args))
    }

    Object add(Object one, String table, Map<String, String> fieldColMapping = null) {
        def map = bean2map(one, skipProperties)
        if (fieldColMapping != null) {
            Map<String, Object> map2 = [:]
            map.each { k, v ->
                def renamedKey = fieldColMapping[k]
                map2[renamedKey ?: k] = v
            }
            add(map2, table, false)
        } else {
            add(map, table, false)
        }
    }

    int update(Object one, String table, String pkCol, Map<String, String> fieldColMapping = null) {
        def map = bean2map(one, skipProperties)
        if (fieldColMapping != null) {
            Map<String, Object> map2 = [:]
            map.each { k, v ->
                def renamedKey = fieldColMapping[k]
                map2[renamedKey ?: k] = v
            }
            update(map2, table, pkCol, false)
        } else {
            update(map, table, pkCol, false)
        }
    }

    private static String addMergeFieldPrefix(String prefix, String field) {
        if (!prefix) {
            return field
        }
        prefix + field[0].toUpperCase() + field[1..-1]
    }

    static void mergeFields(List<Record> list, List<Record> mergedList, String field, String matchField, String prefix,
                            String... mergedFields) {
        mergedList.each { m ->
            list.findAll {
                it.getProperty(field) == m.getProperty(matchField)
            }.each {
                for (f in mergedFields) {
                    it.prop(addMergeFieldPrefix(prefix, f), m.getProperty(f))
                }
            }
        }
    }

    static void mergeFieldsToMap(List<Map> list, List<Record> mergedList, String field, String matchField, String prefix,
                                 String... mergedFields) {
        mergedList.each { m ->
            list.findAll {
                it[field] == m.getProperty(matchField)
            }.each {
                for (f in mergedFields) {
                    it.put(addMergeFieldPrefix(prefix, f), m.getProperty(f))
                }
            }
        }
    }
}
