package org.nlpcn.es4sql.domain;

public class KVValue implements Cloneable {
    public enum ValueType {
        VALUE, EVALUATED, REFERENCE
    }
    public String key;
    public Object value;
    public ValueType valueType = ValueType.VALUE;

    public KVValue(Object value) {
        this.value = value;
    }

    public KVValue(String key, Object value) {
        if (key != null) {
            this.key = key.replace("'", "");
        }
        this.value = value;
    }

    public KVValue(String key, Object value, ValueType valueType ) {
        if (key != null) {
            this.key = key.replace("'", "");
        }
        this.value = value;
        this.valueType = valueType;
    }

    @Override
    public String toString() {
        if (key == null) {
            return value.toString();
        } else {
            return key + "=" + value;
        }
    }
}
