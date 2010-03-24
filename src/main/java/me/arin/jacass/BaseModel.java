package me.arin.jacass;

import me.arin.jacass.annotations.Indexed;
import me.arin.jacass.annotations.IndexedProperty;
import me.arin.jacass.annotations.Model;
import me.arin.jacass.annotations.SimpleProperty;
import me.arin.jacass.serializer.PrimitiveSerializer;
import me.prettyprint.cassandra.dao.Command;
import me.prettyprint.cassandra.service.Keyspace;
import org.apache.cassandra.thrift.*;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.reflect.MethodUtils;
import org.safehaus.uuid.UUIDGenerator;

import java.io.IOException;
import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.*;

/**
 * Base model class - extend this yo
 */
abstract public class BaseModel {
    protected String key;
    protected static final int DEFAULT_MAX_COLUMNS = 100;
    protected RowPath rowPath;
    protected Map<String, ColumnInfo> columnInfo;
    protected Serializer serializer;

    public BaseModel() {

    }

    protected int getMaxNumColumns() {
        return DEFAULT_MAX_COLUMNS;
    }

    public String getKey() {
        return key;
    }

    /**
     * Get the row key for this object - and optionally generate one
     * if we don't already have one
     *
     * @param generateIfNull Generate a key if its null
     * @return The row key
     */
    public String getKey(boolean generateIfNull) {
        if (null == key && generateIfNull) {
            key = generateKey();
        }

        return key;
    }

    /**
     * You can use a custom class to (de)serialize the byte[] from the
     * columns in the row associated with this object
     * <p/>
     * By default a PrimitiveSerializer is used which only deals with Java
     * primitive types
     *
     * @return A Serializer
     */
    public Serializer getSerializer() {
        if (serializer == null) {
            serializer = new PrimitiveSerializer();
        }

        return serializer;
    }

    /**
     * Set the row key
     *
     * @param key
     */
    public void setKey(String key) {
        this.key = key;
    }

    /**
     * Same as load() but exceptions are swallowed...
     * You probably shouldn't ever use this
     *
     * @param key
     * @param swallowException
     * @return This object
     */
    public BaseModel load(String key, boolean swallowException) {
        try {
            return load(key);
        } catch (JacassException e) {
            return null;
        }
    }

    /**
     * Load the data from a row in Cassandra into this object
     *
     * @param key The row key
     * @return this
     * @throws JacassException
     */
    public BaseModel load(String key) throws JacassException {
        setKey(key);
        objectSlice();
        return this;
    }

    /**
     * Generate a row key
     *
     * @return A new row key
     */
    public String generateKey() {
        return this.getClass().getSimpleName().toLowerCase()
                + "."
                + UUIDGenerator.getInstance().generateRandomBasedUUID().toString();
    }

    /**
     * Execute a Command
     *
     * @param command
     * @param <T>
     * @return The result of executing the Command aka: result of the Cassandra thrift call
     * @throws Exception
     */
    protected <T> T execute(Command<T> command) throws Exception {
        return Executor.get().execute(command);
    }

    /**
     * See the other get()
     *
     * @param rowKeys
     * @return Map indexed by the rowkey
     */
    public Map<String, BaseModel> get(final String[] rowKeys) {
        return get(Arrays.asList(rowKeys));
    }

    /**
     * Multiget objects by their row keys
     *
     * @param rowKeys
     * @return Map indexed by the rowkey
     */
    public Map<String, BaseModel> get(final List<String> rowKeys) {
        getRowPath();

        final ColumnParent columnParent = new ColumnParent(rowPath.getColumnFamily());
        String superColumn = rowPath.getSuperColumn();

        if (!"".equals(superColumn)) {
            columnParent.setSuper_column(superColumn.getBytes());
        }

        final SlicePredicate sp = new SlicePredicate();
        sp.setSlice_range(new SliceRange(new byte[]{}, new byte[]{}, false, getMaxNumColumns()));

        Command<Map<String, List<Column>>> command = new Command<Map<String, List<Column>>>() {
            @Override
            public Map<String, List<Column>> execute(Keyspace keyspace) throws Exception {
                return keyspace.multigetSlice(rowKeys, columnParent, sp);
            }
        };

        Map<String, BaseModel> rtn = new HashMap<String, BaseModel>();

        try {
            Map<String, List<Column>> stuff = execute(command);
            for (String k : stuff.keySet()) {
                BaseModel bm = this.getClass().newInstance();
                bm.injectColumns(stuff.get(k));
                rtn.put(k, bm);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        return rtn;
    }

    /**
     * Get the RowPath for this object
     * <p/>
     * RowPath is the Keyspace -> CF [-> SC] that leads us to this row
     *
     * @return The row path
     */
    protected RowPath getRowPath() {
        if (rowPath == null) {
            setupRowPath();
        }

        return rowPath;
    }

    /**
     * Examine the @Model annotation to setup the rowPath field
     */
    protected void setupRowPath() {
        Annotation annotation = this.getClass().getAnnotation(Model.class);
        Model a = (Model) annotation;
        rowPath = new RowPath(a.keyspace(), a.columnFamily(), a.superColumn());
    }

    /**
     * If an @Indexed annotation exists then you can override what CF the
     * index data is stored in. If no @Indexed annotation exists then index data is
     * stored in the CF specified in @Model
     *
     * @return The name of the CF to hold index data in
     */
    protected String getIndexColumnFamily() {
        Annotation annotation = this.getClass().getAnnotation(Indexed.class);
        if (annotation == null) {
            return getRowPath().getColumnFamily();
        }

        Indexed indexed = (Indexed) annotation;
        return indexed.columnFamily();
    }

    /**
     * Go thru the members of this class and figure out which ones need to be persisted
     * and indexed
     * <p/>
     * All members with @SimpleProperty & @IndexedProperty will be returned along with some
     * meta data about em (class, index info, etc)
     *
     * @return Info about all the members we need to persist
     */
    protected Map<String, ColumnInfo> getColumnInfo() {
        if (columnInfo == null) {
            columnInfo = new HashMap<String, ColumnInfo>();

            Field[] declaredFields = this.getClass().getDeclaredFields();
            for (Field field : declaredFields) {
                SimpleProperty mp = field.getAnnotation(SimpleProperty.class);
                IndexedProperty idx = field.getAnnotation(IndexedProperty.class);

                if (mp != null || idx != null) {
                    ColumnInfo ci = new ColumnInfo(field.getName(), field.getType());
                    if (idx != null) {
                        ci.setIndexData(new IndexInfo(idx.required(), idx.required()));
                    }

                    columnInfo.put(field.getName(), ci);
                }
            }
        }

        return columnInfo;
    }

    /**
     * Persist this object
     *
     * @return this
     */
    public BaseModel save() {
        Command<Void> command = new Command<Void>() {
            @Override
            public Void execute(Keyspace keyspace) throws Exception {
                keyspace.batchInsert(getKey(true), getCFMap(), null);
                return null;
            }
        };

        try {
            execute(command);
        } catch (Exception e) {
            e.printStackTrace();
        }

        return this;
    }

    /**
     * Remove this object from Cassandra
     *
     * @return true
     */
    public boolean remove() {
        Command<Void> command = new Command<Void>() {
            @Override
            public Void execute(Keyspace keyspace) throws Exception {
                keyspace.remove(getKey(), getColumnPath(getRowPath()));
                return null;
            }
        };

        try {
            execute(command);
        } catch (Exception e) {
            e.printStackTrace();
        }

        return true;
    }

    /**
     * Generate a ColumnPath from a RowPath
     *
     * @param rp
     * @return
     */
    private ColumnPath getColumnPath(RowPath rp) {
        ColumnPath cp = new ColumnPath(rp.getColumnFamily());

        String superColumn = rp.getSuperColumn();
        if (!"".equals(superColumn)) {
            cp.setSuper_column(superColumn.getBytes());
        }

        return cp;
    }

    /**
     * Convenience method for remove()
     *
     * @param modelClass The class of the BaseModel
     * @param key        row key
     * @return The success of this operation true|false
     */
    public static boolean remove(Class modelClass, String key) {
        try {
            BaseModel m = (BaseModel) modelClass.newInstance();
            m.setKey(key);
            return m.remove();
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }

    /**
     * Get a map of Columns to save into Cassandra
     *
     * @return A map of Column objects
     * @throws JacassException
     */
    protected Map<String, List<Column>> getCFMap() throws JacassException {
        getColumnInfo();
        List<Column> columnList = new ArrayList<Column>();

        for (String columnName : columnInfo.keySet()) {
            String getterName = (new StringBuilder("get").append(StringUtils.capitalize(columnName))).toString();
            Method method = MethodUtils.getAccessibleMethod(this.getClass(), getterName, new Class[]{});

            if (method == null) {
                throw new JacassException("No getter for " + columnName);
            }

            try {
                columnList.add(new Column(columnName.getBytes(),
                        getSerializer().toBytes(columnInfo.get(columnName).getCls(),
                                method.invoke(this)),
                        System.currentTimeMillis()));
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        HashMap<String, List<Column>> rtn = new HashMap<String, List<Column>>();
        rtn.put(getRowPath().getColumnFamily(), columnList);
        return rtn;
    }

    /**
     * Fetch Column info for this object from Cassandra
     *
     * @return Slice of Column objects from Cassandra
     * @throws JacassException
     */
    protected boolean objectSlice() throws JacassException {
        final RowPath rp = getRowPath();
        final ColumnParent columnParent = new ColumnParent(rp.getColumnFamily());
        String superColumn = rp.getSuperColumn();

        if (!"".equals(superColumn)) {
            columnParent.setSuper_column(superColumn.getBytes());
        }

        final SlicePredicate sp = new SlicePredicate();
        sp.setSlice_range(new SliceRange(new byte[]{}, new byte[]{}, false, getMaxNumColumns()));

        Command<List<Column>> command = new Command<List<Column>>() {
            @Override
            public List<Column> execute(Keyspace keyspace) throws Exception {
                return keyspace.getSlice(getKey(), columnParent, sp);
            }
        };

        try {
            List<Column> columns = execute(command);
            if (columns == null || columns.isEmpty()) {
                return false;
            }

            injectColumns(columns);
            return true;
        } catch (Exception e) {
            throw new JacassException(e);
        }
    }

    /**
     * Inject the values from Cassandra Columns into member vars of this object
     *
     * @param columns Columns from Cassandra
     * @throws JacassException
     */
    protected void injectColumns(List<Column> columns) throws JacassException {
        getColumnInfo();

        for (Column column : columns) {
            String columnName = new String(column.getName());
            String setterName = (new StringBuilder("set").append(StringUtils.capitalize(columnName))).toString();
            ColumnInfo ci = columnInfo.get(columnName);
            Class columnType = ci.getCls();

            if (null == columnType) {
                continue;
            }

            Object castData = null;
            try {
                castData = getSerializer().fromBytes(columnType, column.getValue());
            } catch (IOException e) {
                throw new JacassException("Could not get value for " + columnName, e);
            }

            if (castData == null) {
                continue;
            }

            Method method = MethodUtils.getAccessibleMethod(this.getClass(), setterName, columnType);
            if (method != null) {
                try {
                    method.invoke(this, castData);
                } catch (Exception e) {
                    throw new JacassException("Could not call " + setterName, e);
                }
            } else {
                throw new JacassException("No setter for " + column);
            }
        }
    }
}

/**
 * Info about a Column
 */
class ColumnInfo {
    private String name;
    private Class cls;
    private IndexInfo indexData;

    ColumnInfo(String name, Class cls) {
        this.name = name;
        this.cls = cls;
    }

    public void setIndexData(IndexInfo indexData) {
        this.indexData = indexData;
    }

    public boolean isIndexed() {
        return (indexData != null);
    }

    public String getName() {
        return name;
    }

    public Class getCls() {
        return cls;
    }

    public IndexInfo getIndexData() {
        return indexData;
    }
}

/**
 * Indexing info
 */
class IndexInfo {
    private boolean isRequired;
    private boolean isUnique;

    IndexInfo(boolean required, boolean unique) {
        isRequired = required;
        isUnique = unique;
    }

    public boolean isRequired() {
        return isRequired;
    }

    public boolean isUnique() {
        return isUnique;
    }
}