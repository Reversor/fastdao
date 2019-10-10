package io.github.pastorgl.fastdao;

import javax.sql.DataSource;
import java.io.InputStream;
import java.io.Reader;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.lang.reflect.ParameterizedType;
import java.sql.*;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.*;

/**
 * Abstract low-level DAO designed for bulk/batch operations.
 * Implementations must specify concrete {@link FastEntity} subclass as type parameter.
 *
 * @param <E> {@link FastEntity} subclass
 */
public abstract class FastDAO<E extends FastEntity> {
    static private int batchSize = 500;
    static private DataSource ds;
    /**
     * Physical name of Primary Key column
     */
    private String pkName;
    /**
     * Physical table name
     */
    private String tableName;
    /**
     * {@link FastEntity} subclass
     */
    private Class<E> persistentClass;
    /**
     * Persistent class field names to physical column names mapping
     */
    private Map<String, String> fwMapping = new HashMap<>();
    /**
     * Physical column names to persistent class field names mapping
     */
    private Map<String, String> revMapping = new HashMap<>();
    /**
     * Persistent class fields cache
     */
    private Map<String, Field> fields = new HashMap<>();
    /**
     * Persistent class field annotations cache
     */
    private Map<Field, Column> columns = new HashMap<>();
    /**
     * Persistent update query
     */
    private String updateQuery;

    {
        persistentClass = (Class<E>) ((ParameterizedType) getClass()
                .getGenericSuperclass()).getActualTypeArguments()[0];

        if (persistentClass.isAnnotationPresent(Table.class)) {
            tableName = persistentClass.getAnnotation(Table.class).value();
        } else {
            tableName = persistentClass.getSimpleName();
        }

        for (Field field : persistentClass.getDeclaredFields()) {
            if ((field.getModifiers() & Modifier.STATIC) == 0) {
                field.setAccessible(true);
                String fieldName = field.getName();

                fields.put(fieldName, field);

                String columnName;
                if (field.isAnnotationPresent(Column.class)) {
                    Column column = field.getAnnotation(Column.class);
                    columnName = column.value();
                    revMapping.put(columnName, fieldName);
                    fwMapping.put(fieldName, columnName);

                    columns.put(field, column);
                } else {
                    columnName = fieldName;
                }

                if (field.isAnnotationPresent(PK.class)) {
                    pkName = columnName;
                }
            }
        }

        if (pkName == null) {
            pkName = tableName + "_id";
        }
    }

    static protected DataSource getDataSource() {
        return ds;
    }

    static public void setDataSource(DataSource ds) {
        FastDAO.ds = ds;
    }

    static public void setBatchSize(int batchSize) {
        FastDAO.batchSize = batchSize;
    }

    /**
     * Call SELECT that returns a lizt of &lt;E&gt; instances
     *
     * @param query any SQL Query whose result is a list of &lt;E&gt;, optionally with ? for replaceable parameters.
     *              Use backslash to escape question marks
     * @param args  objects, whose values will be used as source of replaceable parameters. If object is an array or
     *              {@link List}, it'll be unfolded
     * @return list of &lt;E&gt;
     */
    protected List<E> select(String query, Object... args) {
        try {
            if (args.length != 0) {
                List<Object> expl = new ArrayList<>(args.length);

                StringBuilder sb = new StringBuilder();

                int r = 0;
                for (Object a : args) {
                    int q;
                    boolean found = false;
                    do {
                        q = query.indexOf('?', r);
                        if (q < 0) {
                            throw new IllegalArgumentException("supplied query and replaceable arguments don't match");
                        }
                        if ((q > 0) && (query.charAt(q - 1) == '\\')) {
                            r = q + 1;
                        } else {
                            found = true;
                        }
                    } while (!found);
                    sb.append(query, r, q);
                    r = q + 1;

                    if (a instanceof Object[]) {
                        a = Collections.singletonList(a);
                    }
                    if (a instanceof List) {
                        List<Object> aa = (List<Object>) a;
                        int s = aa.size();
                        sb.append('(');
                        for (int i = 0; i < s; i++) {
                            expl.add(aa.get(i));
                            if (i > 0) {
                                sb.append(',');
                            }
                            sb.append('?');
                        }
                        sb.append(')');
                    } else {
                        expl.add(a);
                        sb.append('?');
                    }
                }

                sb.append(query.substring(r));

                query = sb.toString();
                args = expl.toArray();
            }

            List<E> lst = new ArrayList<>();

            try (Connection con = ds.getConnection()) {
                try (PreparedStatement ps = con.prepareStatement(query)) {

                    int c = 1;
                    for (Object a : args) {
                        setObject(ps, c++, a);
                    }

                    try (ResultSet rs = ps.executeQuery()) {

                        ResultSetMetaData md = rs.getMetaData();
                        int cnt = md.getColumnCount();
                        while (rs.next()) {
                            E e = persistentClass.newInstance();

                            for (int i = 1; i <= cnt; i++) {
                                String colName = md.getColumnLabel(i);
                                Field field = fields.get(getRevMapping(colName));
                                Class<?> type = field.getType();

                                if (type.isEnum()) {
                                    field.set(e, Enum.valueOf((Class<Enum>) type, rs.getString(i)));
                                } else {
                                    convertFromRetrieve(field, e, rs.getObject(i));
                                }
                            }

                            lst.add(e);
                        }
                    }
                }
            }

            return lst;
        } catch (Exception e) {
            throw new FastDAOException("select", e);
        }
    }

    /**
     * Batch insert of a list of &lt;E&gt; instances
     *
     * @param objects &lt;E&gt; instances
     */
    protected void insert(List<E> objects) {
        if (objects.size() == 0) {
            return;
        }

        try {
            StringBuilder sb = new StringBuilder("INSERT INTO " + tableName + " (");

            StringBuilder columns = new StringBuilder();
            int k = 0;
            for (Field f : fields.values()) {
                String colName = getFwMapping(f.getName());
                if (!pkName.equals(colName)) {
                    if (k++ > 0) {
                        sb.append(",");
                    }
                    columns.append(colName);
                }
            }

            sb.append(columns);

            sb.append(") VALUES ");
            int length = fields.size();
            int size = objects.size();

            sb.append("(");
            for (int j = 1; j < length; j++) {
                if (j > 1) {
                    sb.append(",");
                }
                sb.append("?");
            }
            sb.append(")");

            try (Connection con = ds.getConnection()) {
                try (PreparedStatement ps = con.prepareStatement(sb.toString())) {
                    int b = 0;
                    for (int i = 0; i < size; i++, b++) {
                        k = 1;
                        Object o = objects.get(i);
                        for (Field field : fields.values()) {
                            if (!pkName.equals(getFwMapping(field.getName()))) {
                                setObject(ps, k++, convertToStore(field, o));
                            }
                        }
                        ps.addBatch();

                        if (b == batchSize) {
                            ps.executeBatch();

                            ps.clearBatch();
                            b = 0;
                        }
                    }
                    if (b != 0) {
                        ps.executeBatch();
                    }
                }
            }
        } catch (Exception e) {
            throw new FastDAOException("insert - batch", e);
        }
    }

    /**
     * Insert one &lt;E&gt; instance
     *
     * @param object &lt;E&gt; instance
     * @return new object primary key value
     */
    protected Object insert(E object) {
        try {
            StringBuilder sb = new StringBuilder("INSERT INTO " + tableName + " (");

            boolean generateKey = true;

            int k = 0, fc = 0;
            for (Field field : fields.values()) {
                String colName = getFwMapping(field.getName());
                if (!pkName.equals(colName)) {
                    if (k++ > 0) {
                        sb.append(",");
                    }
                    sb.append(colName);
                    fc++;
                } else {
                    if (field.get(object) != null) {
                        if (k++ > 0) {
                            sb.append(",");
                        }
                        sb.append(colName);
                        fc++;
                        generateKey = false;
                    }
                }
            }

            sb.append(") VALUES (");
            for (int j = 0; j < fc; j++) {
                if (j > 0) {
                    sb.append(",");
                }
                sb.append("?");
            }
            sb.append(")");

            Object key;
            Field keyField;
            try (Connection con = ds.getConnection()) {
                try (PreparedStatement ps = con.prepareStatement(sb.toString(), PreparedStatement.RETURN_GENERATED_KEYS)) {
                    k = 1;
                    key = null;
                    keyField = null;
                    for (Field field : fields.values()) {
                        if (!pkName.equals(getFwMapping(field.getName()))) {
                            setObject(ps, k++, convertToStore(field, object));
                        } else {
                            keyField = field;
                            if (!generateKey) {
                                key = convertToStore(keyField, object);
                                setObject(ps, k++, key);
                            }
                        }
                    }

                    ps.executeUpdate();
                    if (generateKey) {
                        try (ResultSet rs = ps.getGeneratedKeys()) {
                            rs.next();
                            switch (rs.getMetaData().getColumnCount()) {
                                case 0: // null key
                                    break;
                                case 1: {
                                    key = rs.getObject(1);
                                    break;
                                }
                                default:
                                    key = rs.getObject(pkName);
                            }
                        }
                    }
                }
            }

            return convertFromRetrieve(keyField, object, key);
        } catch (Exception e) {
            throw new FastDAOException("insert - single", e);
        }
    }

    /**
     * Update a list of &lt;E&gt; instances matched by their primary key values
     *
     * @param objects &lt;E&gt; instances
     */
    protected void update(List<E> objects) {
        if (objects.size() == 0) {
            return;
        }

        try {
            try (Connection con = ds.getConnection()) {
                batchUpdate(con, objects);
            }
        } catch (Exception e) {
            throw new FastDAOException("update - batch", e);
        }
    }

    private void batchUpdate(Connection con, List<E> objects) throws Exception {
        try (PreparedStatement ps = con.prepareStatement(updateQuery)) {
            String updateQuery = getUpdateQuery();

            int b = 0;
            for (int i = 0; i < objects.size(); i++, b++) {
                Object object = objects.get(i);
                int k = 1;
                for (Field field : fields.values()) {
                    if (!getFwMapping(field.getName()).equals(pkName)) {
                        setObject(ps, k++, convertToStore(field, object));
                    }
                }
                setObject(ps, k, convertToStore(fields.get(pkName), object));
                ps.addBatch();

                if (b == batchSize) {
                    ps.executeBatch();
                    ps.clearBatch();
                    b = 0;
                }
            }
            if (b != 0) {
                ps.executeBatch();
            }
        }
    }

    /**
     * Update single &lt;E&gt; instance matching by its primary key value
     *
     * @param object &lt;E&gt; instance
     */
    protected void update(E object) {
        try {
            String updateQuery = getUpdateQuery();

            try (Connection con = ds.getConnection()) {
                singleUpdate(con, object);
            }
        } catch (Exception e) {
            throw new FastDAOException("update - single", e);
        }
    }

    private void singleUpdate(Connection connection, E object) throws Exception {
        try (PreparedStatement ps = connection.prepareStatement(updateQuery)) {

            int k = 1;
            for (Field field : fields.values()) {
                if (!getFwMapping(field.getName()).equals(pkName)) {
                    setObject(ps, k++, convertToStore(field, object));
                }
            }
            setObject(ps, k, convertToStore(fields.get(pkName), object));

            ps.executeUpdate();
        }
    }

    /**
     * Generate update query for &lt;E&gt objects
     *
     * @return update query
     */
    private String getUpdateQuery() {
        if (updateQuery != null) {
            return updateQuery;
        }

        StringBuilder sb = new StringBuilder("UPDATE " + tableName + " SET (");

        int length = fields.size();
        int k = 0;
        for (Field f : fields.values()) {
            String colName = getFwMapping(f.getName());
            if (!colName.equals(pkName)) {
                if (k++ > 0) {
                    sb.append(",");
                }
                sb.append(colName);
            }
        }

        sb.append(") = (");

        for (int j = 1; j < length; j++) {
            if (j > 1) {
                sb.append(",");
            }
            sb.append("?");
        }
        sb.append(") WHERE ").append(pkName).append("=?");

        return updateQuery = sb.toString();
    }

    /**
     * Delete a list of &lt;E&gt; instances matching by their primary key values
     *
     * @param objects &lt;E&gt; instances
     */
    protected void delete(List<E> objects) {
        if (objects.size() == 0) {
            return;
        }

        try {
            StringBuilder sb = new StringBuilder("DELETE FROM " + tableName + " WHERE " + pkName
                    + " IN (");

            int size = objects.size();
            for (int i = 0; i < size; i++) {
                if (i > 0) {
                    sb.append(",");
                }
                sb.append("?");
            }

            sb.append(")");

            Field key = fields.get(getRevMapping(pkName));

            try (Connection con = ds.getConnection()) {
                try (PreparedStatement ps = con.prepareStatement(sb.toString())) {
                    int k = 1;
                    for (E object : objects) {
                        setObject(ps, k++, convertToStore(key, object));
                    }

                    ps.executeUpdate();
                }
            }
        } catch (Exception e) {
            throw new FastDAOException("delete - list", e);
        }
    }

    /**
     * Delete a single &lt;E&gt; instance matching by its primary key value
     *
     * @param object &lt;E&gt; instance
     */
    protected void delete(E object) {
        if (object == null) {
            return;
        }

        try {
            Field key = fields.get(getRevMapping(pkName));

            Connection con = ds.getConnection();
            PreparedStatement ps = con.prepareStatement("DELETE FROM " + tableName + " WHERE " + pkName + "=?");
            setObject(ps, 1, convertToStore(key, object));

            ps.executeUpdate();
        } catch (Exception e) {
            throw new FastDAOException("delete - single", e);
        }
    }

    /**
     * Convenience method to get all &lt;E&gt; instances from the table
     *
     * @return all &lt;E&gt; instances
     */
    protected List<E> getAll() {
        return select("SELECT * FROM " + tableName);
    }

    /**
     * Get a single &lt;E&gt; instance matching by its primary key value
     *
     * @param pk primary key value
     * @return &lt;E&gt; instance
     */
    protected E getByPK(Object pk) {
        List<E> objects = select("SELECT * FROM " + tableName + " WHERE " + pkName + "=?", pk);

        if (objects.size() != 1) {
            return null;
        }

        return objects.get(0);
    }

    /**
     * Delete a single &lt;E&gt; instance matching by its primary key value
     *
     * @param pk primary key value
     */
    protected void deleteByPK(Object pk) {
        if (pk == null) {
            throw new FastDAOException("delete - single", new NullPointerException());
        }

        Class<?> type = fields.get(getRevMapping(pkName)).getType();
        if (!type.isInstance(pk)) {
            throw new FastDAOException("delete - single", new IllegalArgumentException(
                    "Unexpected primary key type. Expected: " + type.getCanonicalName() + " but passed is: " + pk.getClass()
                            .getCanonicalName()));
        }

        try {
            try (Connection con = ds.getConnection()) {
                try (PreparedStatement ps = con.prepareStatement("DELETE FROM " + tableName + " WHERE " + pkName + "=?")) {
                    setObject(ps, 1, pk);

                    ps.executeUpdate();
                }
            }
        } catch (Exception e) {
            throw new FastDAOException("delete - single", e);
        }
    }

    private String getRevMapping(String columnName) {
        if (revMapping.containsKey(columnName)) {
            return revMapping.get(columnName);
        }

        return columnName;
    }

    private String getFwMapping(String fieldName) {
        if (fwMapping.containsKey(fieldName)) {
            return fwMapping.get(fieldName);
        }

        return fieldName;
    }

    private void setObject(PreparedStatement s, int i, Object a) throws SQLException {
        if (a instanceof FastEntity) {
            s.setObject(i, ((FastEntity) a).getId());
            return;
        }

        if (a instanceof java.util.Date) {
            s.setDate(i, new java.sql.Date(((java.util.Date) a).getTime()));
            return;
        }

        if (a instanceof LocalDate) {
            s.setDate(i, java.sql.Date.valueOf((LocalDate) a));
        }

        if (a instanceof LocalDateTime) {
            s.setTimestamp(i, Timestamp.valueOf((LocalDateTime) a));
        }

        if (a instanceof LocalTime) {
            s.setTime(i, Time.valueOf((LocalTime) a));
        }

        if (a instanceof Enum) {
            s.setString(i, ((Enum<?>) a).name());
            return;
        }

        if (a instanceof Reader) {
            s.setCharacterStream(i, (Reader) a);
        }

        if (a instanceof InputStream) {
            s.setBinaryStream(i, (InputStream) a);
        }

        s.setObject(i, a);
    }

    private Object convertToStore(Field field, Object object) throws Exception {
        Object fieldValue = field.get(object);
        if (columns.containsKey(field)) {
            return columns.get(field).store().newInstance().store(ds.getConnection(), fieldValue);
        }


        return fieldValue;
    }

    private Object convertFromRetrieve(Field field, Object object, Object dbValue) throws Exception {
        Object value = dbValue;
        if (columns.containsKey(field)) {
            value = columns.get(field).retrieve().newInstance().retrieve(dbValue);
        }

        field.set(object, value);
        return value;
    }

    protected Transaction getTransaction() {
        return null;
    }

    public abstract class Transaction implements AutoCloseable {
        private Connection connection;
        private boolean rollbackNeeded = false;

        protected Transaction() throws SQLException {
            this.connection = ds.getConnection();
            connection.setAutoCommit(false);
        }

        protected void update(E entity) {
            try {
                FastDAO.this.singleUpdate(connection, entity);
            } catch (Exception e) {
                throw new FastDAOException("transaction update - single", e);
            }
        }

        protected void update(List<E> enitities) {
            try {
                FastDAO.this.batchUpdate(connection, enitities);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        protected List<E> select(String query, Object... args) {
            return null;
        }

        protected void insert(List<E> objects) {
        }

        protected Object insert(E object) {
            return null;
        }

        protected void delete(List<E> objects) {
        }

        protected void delete(E object) {
        }

        protected List<E> getAll() {
            return null;
        }

        protected E getByPK(Object pk) {
            return null;
        }

        protected void deleteByPK(Object pk) {
        }

        protected final Transaction getTransaction() {
            return this;
        }

        public void commit() {
            try {
                connection.commit();
            } catch (SQLException e) {
                rollbackNeeded = true;

                throw new FastDAOException("", e);
            }
        }

        @Override
        public void close() throws FastDAOException {
            try {
                if (connection.isClosed()) {
                    return;
                }

                if (rollbackNeeded) {
                    connection.rollback();
                } else {
                    connection.commit();
                }
                connection.setAutoCommit(true);
            } catch (SQLException e) {
                throw new FastDAOException("transaction - close", e);
            }
        }
    }

}
