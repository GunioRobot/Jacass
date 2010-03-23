package me.arin.jacass;

import me.prettyprint.cassandra.dao.Command;
import me.prettyprint.cassandra.service.Keyspace;
import org.apache.cassandra.thrift.*;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.reflect.MethodUtils;

import java.io.IOException;
import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.*;

abstract public class BaseModel {
    protected String key;
    protected static final int DEFAULT_MAX_COLUMNS = 100;
    protected RowPath rowPath;
    protected Map<String, Class> columnInfo;

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

    public void setKey(String key) {
        this.key = key;
    }

    public BaseModel load(String key) {
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
            Annotation annotation = this.getClass().getAnnotation(Model.class);
            Model a = (Model) annotation;

            String columnFamily = a.columnFamily();
            String superColumn = a.superColumn();
            String keyspace = a.keyspace();

            rowPath = new RowPath(keyspace, columnFamily, superColumn);
        }

        return rowPath;
    }

    protected Map<String, Class> getColumnInfo() {
        if (columnInfo == null) {
            columnInfo = new HashMap<String, Class>();

            Field[] declaredFields = this.getClass().getDeclaredFields();
            for (Field field : declaredFields) {
                ModelProperty mp = field.getAnnotation(ModelProperty.class);
                if (mp != null) {
                    columnInfo.put(field.getName(), field.getType());
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

    protected Map<String, List<Column>> getCFMap() {
        getColumnInfo();
        List<Column> columnList = new ArrayList<Column>();

        for (String columnName : columnInfo.keySet()) {
            String getterName = (new StringBuilder("get").append(StringUtils.capitalize(columnName))).toString();
            Method method = MethodUtils.getAccessibleMethod(this.getClass(), getterName, new Class[]{});

            // TODO: bitch about lack of getter
            if (method == null) {
                continue;
            }

            Object value;
            try {
                value = method.invoke(this);
                byte[] bytes = Caster.toBytes(columnInfo.get(columnName), value);
                Column c = new Column(columnName.getBytes(), bytes, System.currentTimeMillis());
                columnList.add(c);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        HashMap<String, List<Column>> rtn = new HashMap<String, List<Column>>();
        rtn.put(getRowPath().getColumnFamily(), columnList);
        return rtn;
    }

    protected boolean objectSlice() {
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
        } catch (IOException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        } catch (InvocationTargetException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }

        return false;
    }

    protected void injectColumns(List<Column> columns) throws IOException, IllegalAccessException,
            InvocationTargetException {
        getColumnInfo();

        for (Column column : columns) {
            String columnName = new String(column.getName());
            String setterName = (new StringBuilder("set").append(StringUtils.capitalize(columnName))).toString();
            Class columnType = columnInfo.get(columnName);

            if (null == columnType) {
                continue;
            }

            Object castData = Caster.fromBytes(columnType, column.getValue());

            if (castData == null) {
                continue;
            }

            Method method = MethodUtils.getAccessibleMethod(this.getClass(), setterName, columnType);
            if (method != null) {
                method.invoke(this, castData);
            }
        }
    }
}