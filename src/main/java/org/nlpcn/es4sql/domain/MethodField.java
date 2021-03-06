package org.nlpcn.es4sql.domain;

import java.util.*;

import org.nlpcn.es4sql.Util;

/**
 * 搜索域
 * 
 * @author ansj
 *
 */
public class MethodField extends Field {
	private List<KVValue> params = null;
	private String option;
	private List<Field> reference = null; // The pipeline aggregation field reference other agg fields.

    public MethodField(String name, List<KVValue> params, String option, String alias) {
        this(name, params, option, alias, null);
    }
	public MethodField(String name, List<KVValue> params, String option, String alias, List<Field> reference) {
		super(name, alias);
		this.params = params;
		this.option = option;
		this.reference = reference;
		if (alias==null||alias.trim().length()==0) {
            Map<String, Object> paramsAsMap = this.getParamsAsMap();
            if(paramsAsMap.containsKey("alias")){
                this.setAlias(paramsAsMap.get("alias").toString());
            }
            else {
                this.setAlias(this.toString());
            }
		}
	}

	public List<KVValue> getParams() {
		return params;
	}

    public Map<String,Object> getParamsAsMap(){
        Map<String,Object> paramsAsMap = new HashMap<>();
        if(this.params == null ) return paramsAsMap;
        for(KVValue kvValue : this.params){
            paramsAsMap.put(kvValue.key,kvValue.value);
        }
        return paramsAsMap;
    }

	@Override
	public String toString() {
		if (option != null) {
			return this.name + "(" + option + " " + Util.joiner(params, ",") + ")";
		}
		return this.name + "(" + Util.joiner(params, ",") + ")";
	}

	public String getOption() {
		return option;
	}

	public void setOption(String option) {
		this.option = option;
	}

    @Override
    public boolean isNested() {
        Map<String, Object> paramsAsMap = this.getParamsAsMap();
        return paramsAsMap.containsKey("nested") || paramsAsMap.containsKey("reverse_nested");
    }

    @Override
    public boolean isReverseNested() {
        return this.getParamsAsMap().containsKey("reverse_nested");

    }

    @Override
    public String getNestedPath() {
        if(!this.isNested()) return null;
        if(this.isReverseNested()){
            String reverseNestedPath = this.getParamsAsMap().get("reverse_nested").toString();
            return reverseNestedPath.isEmpty() ? null : reverseNestedPath;
        }
        return this.getParamsAsMap().get("nested").toString();
    }

    @Override
    public boolean isChildren() {
        Map<String, Object> paramsAsMap = this.getParamsAsMap();
        return paramsAsMap.containsKey("children");
    }

    @Override
    public String getChildType() {
        if(!this.isChildren()) return null;

        return this.getParamsAsMap().get("children").toString();
    }

    public void addReference(Field field) {
        if (reference == null) {
            reference = new ArrayList<Field>();
        }
        reference.add(field);
    }

    public List<Field> getReference() {
        return  reference;
    }

    public Set<Field> flatten() {
        Set<Field> ret = new HashSet<>();
        ret.add(this);

        if (reference != null) {
            for (Field f : reference) {
                if (f instanceof MethodField) {
                    MethodField f2 = (MethodField) f;
                    ret.addAll(f2.flatten());
                }
            }
        }

        return ret;
    }
}
