package me.arin.jacass;

import me.arin.jacass.annotations.Slice;
import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ColumnOrSuperColumn;

import java.lang.annotation.Annotation;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static me.arin.jacass.ForiegnKeyMode.USE_COLUMN_VALUE;

enum ForiegnKeyMode {
    USE_COLUMN_NAME, USE_COLUMN_VALUE, USE_CUSTOM
}

abstract public class BaseView {
    private static final int DEFAULT_COUNT = 100;

    protected ColumnKey columnKey = null;
    protected Class objectClass;
    protected ForiegnKeyMode foreignKeyMode = USE_COLUMN_VALUE;
    protected SliceMode sliceMode = SliceMode.EXCLUSIVE;

    protected String statColumn = "";
    protected String endColumn = "";
    protected int limit = DEFAULT_COUNT;
    private boolean reversed = false;

    abstract protected byte[] generateObjectRowKey(Column column);

    protected ColumnKey getColumnKey() {
        if (columnKey == null) {
            Annotation annotation = this.getClass().getAnnotation(Slice.class);
            Slice a = (Slice) annotation;
            columnKey = new ColumnKey(a.keyspace(), a.columnFamily(), a.key(), a.superColumn());
        }

        return columnKey;
    }

    protected List<ColumnOrSuperColumn> getIndexColumns() {
//        final ClientManager clientManager = getCassandraClientManager();
//        final Cassandra.Client client = (Cassandra.Client) clientManager.getClient();
//        final ColumnKey rk = getColumnKey();
//        final ColumnParent columnParent = new ColumnParent(rk.getColumnFamily());
//
//        String superColumn = rk.getSuperColumn();
//        if (!"".equals(superColumn)) {
//            columnParent.setSuper_column(superColumn.getBytes());
//        }
//
//        int limit = getLimit();
//        if (sliceMode == SliceMode.EXCLUSIVE) {
//            limit++;
//        }
//
//        final SlicePredicate sp = new SlicePredicate();
//        sp.setSlice_range(new SliceRange(statColumn.getBytes(), endColumn.getBytes(), reversed, limit));
//
//        try {
//            ClientOperation<List<ColumnOrSuperColumn>> operation = new ClientOperation<List<ColumnOrSuperColumn>>() {
//                @Override
//                public List<ColumnOrSuperColumn> execute() {
//                    try {
//                        try {
//                            return client
//                                    .get_slice(rk.getKeyspace(), rk.getKey(), columnParent, sp, ConsistencyLevel.ONE);
//                        } catch (InvalidRequestException e) {
//                            e.printStackTrace();
//                        } catch (UnavailableException e) {
//                            e.printStackTrace();
//                        } catch (TException e) {
//                            e.printStackTrace();
//                        }
//                    } catch (TimedOutException e) {
//                        e.printStackTrace();
//                    }
//
//                    return null;
//                }
//            };
//
//            List<ColumnOrSuperColumn> columns = (List<ColumnOrSuperColumn>) clientManager.getResult(operation, client);
//
//            if (sliceMode == SliceMode.EXCLUSIVE && !columns.isEmpty()) {
//                columns.remove(0);
//            }
//
//            return columns;
//        } catch (Exception e) {
//            e.printStackTrace();
//            return null;
//        }

        return null;
    }

    public List<?> get() {
        List<ColumnOrSuperColumn> orSuperColumns = getIndexColumns();
        List<byte[]> rowKeys = getObjectRowKeys(orSuperColumns);

        return getObjects(rowKeys);
    }

    protected List<byte[]> getObjectRowKeys(List<ColumnOrSuperColumn> orSuperColumns) {
        List<byte[]> secondaryIndexcolumnNames = new ArrayList<byte[]>();

        for (ColumnOrSuperColumn orSuperColumn : orSuperColumns) {
            switch (foreignKeyMode) {
                case USE_COLUMN_NAME:
                    secondaryIndexcolumnNames.add(orSuperColumn.getColumn().getName());
                    break;
                case USE_COLUMN_VALUE:
                    secondaryIndexcolumnNames.add(orSuperColumn.getColumn().getValue());
                    break;
                case USE_CUSTOM:
                    secondaryIndexcolumnNames.add(generateObjectRowKey(orSuperColumn.getColumn()));
                    break;
            }
        }

        return secondaryIndexcolumnNames;
    }

    protected List<BaseModel> getObjects(List<byte[]> rowKeys) {
        try {
            List<String> stringKeys = new ArrayList<String>();
            for (byte[] rowKey : rowKeys) {
                stringKeys.add(new String(rowKey));
            }

            BaseModel bm = (BaseModel) getObjectClass().newInstance();
            Map<String, BaseModel> map = bm.load(stringKeys);


            List<BaseModel> rtn = new ArrayList<BaseModel>();
            for (String stringKey : stringKeys) {
                rtn.add(map.get(stringKey));
            }

            return rtn;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    public ForiegnKeyMode getForeignKeyMode() {
        return foreignKeyMode;
    }

    public void setForeignKeyMode(ForiegnKeyMode foreignKeyMode) {
        this.foreignKeyMode = foreignKeyMode;
    }

    public SliceMode getSliceMode() {
        return sliceMode;
    }

    public void setSliceMode(SliceMode sliceMode) {
        this.sliceMode = sliceMode;
    }

    public String getStatColumn() {
        return statColumn;
    }

    public void setStatColumn(String statColumn) {
        this.statColumn = statColumn;
    }

    public String getEndColumn() {
        return endColumn;
    }

    public void setEndColumn(String endColumn) {
        this.endColumn = endColumn;
    }

    public int getLimit() {
        return limit;
    }

    public void setLimit(int limit) {
        this.limit = limit;
    }

    public boolean isReversed() {
        return reversed;
    }

    public void setReversed(boolean reversed) {
        this.reversed = reversed;
    }

    public Class getObjectClass() {
        return objectClass;
    }

    public void setObjectClass(Class objectClass) {
        this.objectClass = objectClass;
    }
}