package me.arin.jacass;

import me.arin.jacass.serializer.PrimitiveSerializer;
import me.prettyprint.cassandra.dao.Command;
import me.prettyprint.cassandra.service.Keyspace;
import org.apache.cassandra.thrift.*;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.reflect.MethodUtils;

import java.io.IOException;
import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.*;

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

    public String getKey(boolean generateIfNull) {
        if (null == key && generateIfNull) {
            key = generateKey();
        }

        return key;
    }

    public Serializer getSerializer() {
        if (serializer == null) {
            serializer = new PrimitiveSerializer();
        }

        return serializer;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public BaseModel load(String key, boolean swallowException) {
        try {
            return load(key);
        } catch (JacassException e) {
            return null;
        }
    }

    public BaseModel load(String key) throws JacassException {
        setKey(key);
        objectSlice();
        return this;
    }

    public abstract String generateKey();

    protected <T> T execute(Command<T> command) throws Exception {
        return Executor.get().execute(command);
    }

    public Map<String, BaseModel> get(final String[] rowKeys) {
        return get(Arrays.asList(rowKeys));
    }

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

    protected RowPath getRowPath() {
        if (rowPath == null) {
            setupRowPath();
        }

        return rowPath;
    }

    protected void setupRowPath() {
        Annotation annotation = this.getClass().getAnnotation(Model.class);
        Model a = (Model) annotation;
        rowPath = new RowPath(a.keyspace(), a.columnFamily(), a.superColumn());
    }

    protected String getIndexColumnFamily() {
        Annotation annotation = this.getClass().getAnnotation(Indexable.class);
        if (annotation == null) {
            return null;
        }

        Indexable indexable = (Indexable) annotation;
        return indexable.columnFamily();
    }

    protected Map<String, ColumnInfo> getColumnInfo() {
        if (columnInfo == null) {
            columnInfo = new HashMap<String, ColumnInfo>();

            Field[] declaredFields = this.getClass().getDeclaredFields();
            for (Field field : declaredFields) {
                ModelProperty mp = field.getAnnotation(ModelProperty.class);
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

    private ColumnPath getColumnPath(RowPath rp) {
        ColumnPath columnKey = new ColumnPath(rp.getColumnFamily());

        String superColumn = rp.getSuperColumn();
        if (!"".equals(superColumn)) {
            columnKey.setSuper_column(superColumn.getBytes());
        }

        return columnKey;
    }

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
                                          getSerializer().toBytes(columnInfo.get(columnName).getCls(), method.invoke(this)),
                                          System.currentTimeMillis()));
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        HashMap<String, List<Column>> rtn = new HashMap<String, List<Column>>();
        rtn.put(getRowPath().getColumnFamily(), columnList);
        return rtn;
    }

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