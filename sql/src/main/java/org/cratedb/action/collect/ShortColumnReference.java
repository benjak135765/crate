package org.cratedb.action.collect;

import org.apache.lucene.index.AtomicReaderContext;
import org.cratedb.DataType;
import org.elasticsearch.index.fielddata.IndexNumericFieldData;
import org.elasticsearch.index.fielddata.LongValues;

public class ShortColumnReference extends FieldCacheExpression<IndexNumericFieldData, Short> {

    LongValues values;

    public ShortColumnReference(String columnName) {
        super(columnName);
    }

    @Override
    public Short evaluate() {
        if (values.setDocument(docId) == 0) {
            return null;
        }

        return ((Long)values.nextValue()).shortValue();
    }

    @Override
    public DataType returnType() {
        return DataType.SHORT;
    }

    @Override
    public void setNextReader(AtomicReaderContext context) {
        super.setNextReader(context);
        values = indexFieldData.load(context).getLongValues();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null)
            return false;
        if (obj == this)
            return true;
        if (!(obj instanceof ShortColumnReference))
            return false;
        return columnName.equals(((ShortColumnReference) obj).columnName);
    }

    @Override
    public int hashCode() {
        return columnName.hashCode();
    }
}
