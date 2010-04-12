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
    protected Map<String, ColumnInfo> columnInfos;
    protected Serializer serializer;
    protected HashMap<String, byte[]> originalIndexValues = new HashMap<String, byte[]>();
    
    private Executor executor;

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

        List<Column> columns = getColumns();
        if (columns == null || columns.isEmpty()) {
            return null;
        }

        injectColumns(columns);
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
    protected <T> T execute(Command<T> command) throws JacassException {
        try {
            Executor executor = getExectutor();
            return executor.execute(this, command, getConsistencyLevel());
        } catch (Exception e) {
            throw new JacassException(e);
        }
    }

    private <T> Executor getExectutor() {
        if (executor == null) {
            executor = Executor.get(getRowPath().getKeyspace());
        }

        return executor;
    }

    protected ConsistencyLevel getConsistencyLevel() {
        return ConsistencyLevel.ONE;
    }

    /**
     * See the other get()
     *
     * @param rowKeys
     * @return Map indexed by the rowkey
     */
    public Map<String, BaseModel> load(final String[] rowKeys) {
        return load(Arrays.asList(rowKeys));
    }

    /**
     * Multiget objects by their row keys
     *
     * @param rowKeys
     * @return Map indexed by the rowkey
     */
    public Map<String, BaseModel> load(final List<String> rowKeys) {
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
                List<Column> columns = stuff.get(k);
                if (columns == null || columns.isEmpty()) {
                    continue;
                }

                BaseModel bm = this.getClass().newInstance();
                bm.injectColumns(columns);
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
    protected Map<String, ColumnInfo> getColumnInfos() {
        if (columnInfos == null) {
            // have a map of the fieldname to columnInfo object
            // need a map of columnName to field
            columnInfos = new HashMap<String, ColumnInfo>();
            Field[] declaredFields = this.getClass().getDeclaredFields();
            for (Field field : declaredFields) {
                SimpleProperty mp = field.getAnnotation(SimpleProperty.class);
                IndexedProperty idx = field.getAnnotation(IndexedProperty.class);
                
                if (mp != null || idx != null) {
                    ColumnInfo ci = new ColumnInfo(field);
                    if (idx != null) {
                        ci.setIndexData(new IndexInfo(idx.required(), idx.unique()));
                    }

                    columnInfos.put(field.getName(), ci);
                }
            }
        }

        return columnInfos;
    }

    /**
     * Persist this object
     *
     * @return this
     */
    public BaseModel save() throws JacassException {
        final Map<String, List<Column>> cfMap = getCFMap();

        Command<Void> command = new Command<Void>() {
            @Override
            public Void execute(Keyspace keyspace) throws Exception {
                keyspace.batchInsert(getKey(true), cfMap, null);
                return null;
            }
        };

        List<Column> columns = cfMap.get(getRowPath().getColumnFamily());
        for (Column column : columns) {
            final String columnName = new String(column.getName());
            final IndexInfo indexInfo = columnInfos.get(columnName).getIndexData();

            if (indexInfo == null) {
                continue;
            }

            Class columnType;
            Object columnValue;
            
            try {
                ColumnInfo columnInfo = columnInfos.get(columnName);
                                
                columnType = columnInfo.getCls();
                columnValue = columnInfo.getField().get(this);
            } catch (Exception e) {
                throw new JacassIndexException("Could not manage index data for " + columnName, e);
            }

            if (indexInfo.isRequired() && columnValue == null) {
                throw new JacassIndexException(columnName + " is required and cannot be null");
            }

            final byte[] ogValue = originalIndexValues.get(columnName);
            final ColumnCrud columnCrud = getExectutor().getColumnCrud();

            if (ogValue != null) {
                byte[] newValue = column.getValue();
                if (!Arrays.equals(ogValue, newValue)) {
                    columnCrud.remove(getIndexColumnKey(column));

                    if (indexInfo.isUnique()) {

                    } else {

                    }
                    
                    System.out.println("heh");
                    // TODO: delete old index
                    // TODO: create new index
                }
            }
        }

        execute(command);
        return this;
    }

    /**
     * // unique
     * CF {
     *     column_name.unique_index : {
     *         value: object_key
     *     }
     * }
     *
     * // not unique
     * CF {
     *      column_name.value : {
     *          object_key : value
     *    }
     * }
     *
     *
     * @param column
     * @return
     */
    private ColumnKey getIndexColumnKey(Column column) {
        return new ColumnKey(getRowPath().getKeyspace(), getRowPath().getSuperColumn(),
                getRowPath().getColumnFamily(), "idxKey", "idxColumn");
    }

    /**
     * Remove this object from Cassandra
     *
     * @return true
     */
    public boolean remove() throws JacassException {
        Command<Void> command = new Command<Void>() {
            @Override
            public Void execute(Keyspace keyspace) throws Exception {
                keyspace.remove(getKey(), getColumnPath(getRowPath()));
                return null;
            }
        };

        execute(command);
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
    public static boolean remove(Class modelClass, String key) throws JacassException {
        BaseModel m;
        try {
            m = (BaseModel) modelClass.newInstance();
        } catch (Exception e) {
            throw new JacassException("Could not instanciate new " + modelClass.getSimpleName(), e);
        }

        m.setKey(key);
        return m.remove();
    }

    /**
     * Get a map of Columns to save into Cassandra
     *
     * @return A map of Column objects
     * @throws JacassException
     */
    public Map<String, List<Column>> getCFMap() throws JacassException {
        getColumnInfos();
        List<Column> columnList = new ArrayList<Column>();

        for (String columnName : columnInfos.keySet()) {
            String getterName = (new StringBuilder("get").append(StringUtils.capitalize(columnName))).toString();
            Method method = MethodUtils.getAccessibleMethod(this.getClass(), getterName, new Class[]{});

            if (method == null) {
                throw new JacassException("No getter for " + columnName);
            }

            try {
                columnList.add(new Column(columnName.getBytes(),
                        getSerializer().toBytes(columnInfos.get(columnName).getCls(),
                                method.invoke(this)),
                        System.currentTimeMillis()));
            } catch (Exception e) {
                throw new JacassException("Could not serialize columns", e);
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
    protected List<Column> getColumns() throws JacassException {
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
            return execute(command);
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
        getColumnInfos();

        for (Column column : columns) {
            String columnName = new String(column.getName());
            String setterName = (new StringBuilder("set").append(StringUtils.capitalize(columnName))).toString();
            ColumnInfo ci = columnInfos.get(columnName);
            Class columnType = ci.getCls();

            if (null == columnType) {
                continue;
            }

            Object castData = null;
            try {
                castData = getSerializer().fromBytes(columnType, column.getValue());
                if (ci.isIndexed()) {
                    originalIndexValues.put(columnName, column.getValue());
                }
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

    public String getKeyspace() {
        return getRowPath().getKeyspace();
    }

	/**
	 * Load all available objects by their row keys
	 *
	 * @return Map indexed by the rowkey
	 */
	public Map<String, BaseModel> loadAll() {
		return load("","");
	}

	/**
	 * Load range of available objects
	 *
	 * @param startKey
	 * @param finishKey
	 * @return Map indexed by the rowkey
	 */
	public Map<String, BaseModel> load(final String startKey, final String finishKey) {
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
	            return keyspace.getRangeSlice(columnParent, sp, startKey, finishKey, getMaxNumColumns());
	        }
	    };

	    Map<String, BaseModel> rtn = new HashMap<String, BaseModel>();

	    try {
	        Map<String, List<Column>> stuff = execute(command);
	        for (String k : stuff.keySet()) {
	            List<Column> columns = stuff.get(k);
	            if (columns == null || columns.isEmpty()) {
	                continue;
	            }

	            BaseModel bm = this.getClass().newInstance();
	            bm.injectColumns(columns);
	            rtn.put(k, bm);
	        }
	    } catch (Exception e) {
	        e.printStackTrace();
	    }

	    return rtn;
	}
}