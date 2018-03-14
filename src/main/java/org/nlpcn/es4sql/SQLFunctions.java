package org.nlpcn.es4sql;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.expr.*;
import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.elasticsearch.common.collect.Tuple;
import org.nlpcn.es4sql.domain.KVValue;

import java.util.*;

/**
 * Created by allwefantasy on 8/19/16.
 */
public class SQLFunctions {

    //Groovy Built In Functions
    public final static Set<String> buildInFunctions = Sets.newHashSet(
            "exp", "ln", "log", "log10", "sqrt", "cbrt", "ceil", "floor", "rint", "pow", "round",
            "random", "abs", //nummber operator
            "split", "concat_ws", "substring", "substr", "trim",//string operator
            "add", "multiply", "divide", "subtract", "modulus",//binary operator
            "field", "to_date", "date_format"
    );


    public static Tuple<String, String> function(String methodName, List<KVValue> paramers, String name,boolean returnValue) {
        Tuple<String, String> functionStr = null;
        switch (methodName) {
            case "split":
                if (paramers.size() == 3) {
                    functionStr = split(Util.expr2Object((SQLExpr) paramers.get(0).value).toString(),
                            Util.expr2Object((SQLExpr) paramers.get(1).value).toString(),
                            Integer.parseInt(Util.expr2Object((SQLExpr) paramers.get(2).value).toString()), name);
                } else {
                    functionStr = split(paramers.get(0).value.toString(),
                            paramers.get(1).value.toString(),
                            name);
                }

                break;

            case "concat_ws":
                List<SQLExpr> result = Lists.newArrayList();
                for (int i = 1; i < paramers.size(); i++) {
                    result.add((SQLExpr) paramers.get(i).value);
                }
                functionStr = concat_ws(paramers.get(0).value.toString(), result, name);

                break;

            case "to_date":
                functionStr = to_date(
                        Util.expr2Object((SQLExpr) paramers.get(0).value).toString(),
                        Util.expr2Object((SQLExpr) paramers.get(1).value).toString(),
                        name);
                break;

            case "date_format":
                functionStr = date_format(
                        Util.expr2Object((SQLExpr) paramers.get(0).value).toString(),
                        Util.expr2Object((SQLExpr) paramers.get(1).value).toString(),
                        name);
                break;

            case "pow":
                functionStr = pow(
                        Util.expr2Object((SQLExpr) paramers.get(0).value).toString(),
                        Util.expr2Object((SQLExpr) paramers.get(1).value).toString(),
                        name);
                break;

            case "ln":
                functionStr = log(paramers.get(0).value.toString(),
                        name);
                break;

            case "log":
                if (paramers.size() == 2) {
                    functionStr = log(paramers.get(0).value.toString(),
                            paramers.get(1).value.toString(),
                            name);
                } else {
                    functionStr = log(paramers.get(0).value.toString(),
                            name);
                }
                break;

            case "log10":
                functionStr = log10(Util.expr2Object((SQLExpr) paramers.get(0).value).toString(), name);
                break;

            case "floor":
            case "round":
            case "ceil":
            case "cbrt":
            case "rint":
            case "exp":
            case "sqrt":
            case "abs":
                functionStr = mathSingleValueTemplate("Math."+methodName,
                        methodName,
                        Util.expr2Object((SQLExpr) paramers.get(0).value).toString(),
                        name);
                break;

            case "substr":
            case "substring":
                if (paramers.size() == 3) {
                    functionStr = substring(Util.expr2Object((SQLExpr) paramers.get(0).value).toString(),
                            Integer.parseInt(Util.expr2Object((SQLExpr) paramers.get(1).value).toString()),
                            Integer.parseInt(Util.expr2Object((SQLExpr) paramers.get(2).value).toString())
                            , name);
                } else {
                    functionStr = substring(Util.expr2Object((SQLExpr) paramers.get(0).value).toString(),
                            Integer.parseInt(Util.expr2Object((SQLExpr) paramers.get(1).value).toString())
                            , name);
                }
                break;
            case "trim":
                functionStr = trim(Util.expr2Object((SQLExpr) paramers.get(0).value).toString(), name);
                break;

            case "add":
                functionStr = add((SQLExpr) paramers.get(0).value, (SQLExpr) paramers.get(1).value);
                break;

            case "subtract":
                functionStr = subtract((SQLExpr) paramers.get(0).value, (SQLExpr) paramers.get(1).value);
                break;
            case "divide":
                functionStr = divide((SQLExpr) paramers.get(0).value, (SQLExpr) paramers.get(1).value);
                break;

            case "multiply":
                functionStr = multiply((SQLExpr) paramers.get(0).value, (SQLExpr) paramers.get(1).value);
                break;
            case "modulus":
                functionStr = modulus((SQLExpr) paramers.get(0).value, (SQLExpr) paramers.get(1).value);
                break;

            case "field":
                functionStr = field(Util.expr2Object((SQLExpr) paramers.get(0).value).toString());
                break;

            default:

        }
        if(returnValue){
            String generatedFieldName = functionStr.v1();
            String returnCommand = ";return " + generatedFieldName +";" ;
            String newScript = functionStr.v2() + returnCommand;
            functionStr = new Tuple<>(generatedFieldName, newScript);
        }
        return functionStr;
    }

    public static String random() {
        return Math.abs(new Random().nextInt()) + "";
    }

    private static Tuple<String, String> concat_ws(String split, List<SQLExpr> columns, String valueName) {
        String name = "concat_ws_" + random();
        List<String> result = Lists.newArrayList();

        for (SQLExpr column : columns) {
            String strColumn = Util.expr2Object(column).toString();
            if (strColumn.startsWith("def ")) {
                result.add(strColumn);
            } else if (isProperty(column)) {
                result.add("doc['" + strColumn + "'].value");
            } else {
                result.add("'" + strColumn + "'");
            }

        }
        return new Tuple<>(name, "def " + name + " =" + Joiner.on("+ " + split + " +").join(result));

    }


    //split(Column str, java.lang.String pattern)
    public static Tuple<String, String> split(String strColumn, String pattern, int index, String valueName) {
        String name = "split_" + random();
        String script = "";
        if (valueName == null) {
            script = "def " + name + " = doc['" + strColumn + "'].value.split('" + pattern + "')[" + index + "]";

        } else {
            script = "; def " + name + " = " + valueName + ".split('" + pattern + "')[" + index + "]";
        }
        return new Tuple<>(name, script);
    }

    private static Tuple<String, String> date_format(String strColumn, String pattern, String valueName) {
        String name = "date_format_" + random();
        if (valueName == null) {
            return new Tuple<>(name, "def " + name + " = new SimpleDateFormat('" + pattern + "').format(new Date(doc['" + strColumn + "'].value))");
        } else {
            return new Tuple<>(name, strColumn + "; def " + name + " = new SimpleDateFormat('" + pattern + "').format(new Date(" + valueName + "))");
        }
    }

    private static Tuple<String, String> to_date(String strColumn, String pattern, String valueName) {
        String name = "to_date" + random();
        String template_column = "" +
                "def v = doc['${COLUMN}'].value; if (v == null || v == '' || v == 0) return null; " +
                "Date d = (v instanceof String) ? new SimpleDateFormat('${PATTERN}').parse(v) : new Date(v); " +
                "SimpleDateFormat sdf = new SimpleDateFormat(\"yyyy-MM-dd'T'HH:mm:ss.SSSXXX\"); " +
                "def ${NAME} = sdf.format(d)";
        String template_value = "" +
                "def v = ${VALUE}; if (v == null || v == '') return null; " +
                "Date d = new Date(v); if (d == null) d = new SimpleDateFormat('${PATTERN}').parse(v); " +
                "SimpleDateFormat sdf = new SimpleDateFormat(\"yyyy-MM-dd'T'HH:mm:ss.SSSXXX\"); " +
                "def ${NAME} = sdf.format(d)";

        Map<String, String> map = new HashMap<>();
        map.put("NAME", name);
        map.put("PATTERN", pattern);

        String script = "";
        if (valueName == null) {
            map.put("COLUMN", strColumn);
            script = Util.renderString(template_column, map);
        } else {
            map.put("VALUE", valueName);
            script = Util.renderString(template_value, map);
        }
        return new Tuple(name, script);
    }

    public static Tuple<String, String> add(SQLExpr a, SQLExpr b) {
        return binaryOpertator("add", "+", a, b);
    }

    private static Tuple<String, String> modulus(SQLExpr a, SQLExpr b) {
        return binaryOpertator("modulus", "%", a, b);
    }

    public static Tuple<String, String> field(String a) {
        String name = "field_" + random();
        return new Tuple<>(name, "def " + name + " = " + "doc['" + a + "'].value");
    }

    private static Tuple<String, String> subtract(SQLExpr a, SQLExpr b) {
        return binaryOpertator("subtract", "-", a, b);
    }

    private static Tuple<String, String> multiply(SQLExpr a, SQLExpr b) {
        return binaryOpertator("multiply", "*", a, b);
    }

    private static Tuple<String, String> divide(SQLExpr a, SQLExpr b) {
        return binaryOpertator("divide", "/", a, b);
    }

    private static Tuple<String, String> binaryOpertator(String methodName, String operator, SQLExpr a, SQLExpr b) {

        String name = methodName + "_" + random();
        return new Tuple<>(name,
                scriptDeclare(a) + scriptDeclare(b) +
                        convertType(a) + convertType(b) +
                        " def " + name + " = " + extractName(a) + " " + operator + " " + extractName(b) ) ;
    }

    private static boolean isProperty(SQLExpr expr) {
        return (expr instanceof SQLIdentifierExpr || expr instanceof SQLPropertyExpr || expr instanceof SQLVariantRefExpr);
    }

    private static String scriptDeclare(SQLExpr a) {

        if (isProperty(a) || a instanceof SQLNumericLiteralExpr)
            return "";
        else return Util.expr2Object(a).toString() + ";";
    }

    private static String extractName(SQLExpr script) {
        if (isProperty(script)) return "doc['" + script + "'].value";
        String scriptStr = Util.expr2Object(script).toString();
        String[] variance = scriptStr.split(";");
        String newScript = variance[variance.length - 1];
        if (newScript.trim().startsWith("def ")) {
            //for now ,if variant is string,then change to double.
            return newScript.trim().substring(4).split("=")[0].trim();
        } else return scriptStr;
    }

    //cast(year as int)

    private static String convertType(SQLExpr script) {
        String[] variance = Util.expr2Object(script).toString().split(";");
        String newScript = variance[variance.length - 1];
        if (newScript.trim().startsWith("def ")) {
            //for now ,if variant is string,then change to double.
            String temp = newScript.trim().substring(4).split("=")[0].trim();

            return " if( " + temp + " instanceof String) " + temp + "= Double.parseDouble(" + temp.trim() + "); ";
        } else return "";


    }

    public static Tuple<String, String> log(String strColumn, String valueName) {
        return log(null, strColumn, valueName);
    }

    public static Tuple<String, String> log(String base, String strColumn, String valueName) {
        String methodName = "Math.log";
        String name = "log_" + random();
        if (base != null && base.trim() != "") {
            name = "log_" + base.trim() + "_" + random();
        }

        String script = "";
        if (valueName == null) {
            script = "def " + name + " = " + methodName + "(doc['" + strColumn + "'].value)";
        } else {
            script = strColumn + ";def " + name + " = " + methodName + "(" + valueName + ")";
        }

        if (base != null && base.trim() != "") {
            script = script + "/" + methodName + "(" + base + ")";
        }

        return new Tuple<>(name, script);
    }

    public static Tuple<String, String> log10(String strColumn, String valueName) {

        return mathSingleValueTemplate("Math.log10", "log10", strColumn, valueName);

    }

    public static Tuple<String, String> sqrt(String strColumn, String valueName) {

        return mathSingleValueTemplate("Math.sqrt", "sqrt",  strColumn, valueName);

    }

    public static Tuple<String, String> round(String strColumn, String valueName) {

        return mathSingleValueTemplate("Math.round","round", strColumn, valueName);

    }

    public static Tuple<String, String> pow(String strColumn, String exponent, String valueName) {

        return mathTwoValueTemplate("Math.pow","pow", strColumn, exponent, valueName);

    }

    public static Tuple<String, String> trim(String strColumn, String valueName) {

        return strSingleValueTemplate("trim", strColumn, valueName);

    }

    private static Tuple<String, String> mathSingleValueTemplate(String methodName, String strColumn, String valueName) {
        return mathSingleValueTemplate(methodName,methodName, strColumn,valueName);
    }
    private static Tuple<String, String> mathSingleValueTemplate(String methodName, String fieldName, String strColumn, String valueName) {
        String name = fieldName + "_" + random();
        if (valueName == null) {
            return new Tuple<>(name, "def " + name + " = " + methodName + "(doc['" + strColumn + "'].value)");
        } else {
            return new Tuple<>(name, strColumn + ";def " + name + " = " + methodName + "(" + valueName + ")");
        }
    }

    public static Tuple<String, String> strSingleValueTemplate(String methodName, String strColumn, String valueName) {
        String name = methodName + "_" + random();
        if (valueName == null) {
            return new Tuple(name, "def " + name + " = doc['" + strColumn + "'].value." + methodName + "()" );
        } else {
            return new Tuple(name, strColumn + "; def " + name + " = " + valueName + "." + methodName + "()");
        }

    }

    private static Tuple<String, String> mathTwoValueTemplate(String methodName, String fieldName, String strColumn, String second, String valueName) {
        String name = fieldName + "_" + random();
        if (valueName == null) {
            return new Tuple<>(name, "def " + name + " = " + methodName + "(doc['" + strColumn + "'].value," + second +")");
        } else {
            return new Tuple<>(name, strColumn + ";def " + name + " = " + methodName + "(" + valueName + "," + second + ")");
        }
    }

    public static Tuple<String, String> floor(String strColumn, String valueName) {

        return mathSingleValueTemplate("Math.floor", "floor",strColumn, valueName);

    }

    public static Tuple<String, String> substring(String strColumn, int pos, String valueName) {
        // SELECT SUBSTRING('Sakila', -3);  => 'ila';
        // SELECT SUBSTRING('Quadratically',5); => 'ratically';
        String template_column = "" +
                "def str = doc['${COLUMN}'].value; if (str == null) return null; if(str == '') return '';" +
                "def begin = ${POS} - 1; " +
                "def ${RET} = str.substring(begin)";
        String template_column_2 = "" +
                "def str = doc['${COLUMN}'].value; if (str == null) return null; if(str == '') return '';" +
                "def begin = ${POS} + str.length(); " +
                "def ${RET} = str.substring(begin)";
        String template_value = "" +
                "def str = ${VALUE}; if (str == null) return null; if(str == '') return ''; " +
                "def begin = ${POS} - 1; " +
                "def ${RET} = str.substring(begin)";
        String template_value_2 = "" +
                "def str = ${VALUE}; if (str == null) return null; if(str == '') return ''; " +
                "def begin = ${POS} + str.length(); " +
                "def ${RET} = str.substring(begin)";

        String script = "";
        String name = "substring_" + random();
        Map<String, String> map = new HashMap<>();
        map.put("RET", name);
        map.put("POS", String.valueOf(pos));

        if (valueName == null) {
            map.put("COLUMN", strColumn);
            if (pos > 0) {
                script = Util.renderString(template_column, map);
            } else if (pos < 0) {
                script = Util.renderString(template_column_2, map);
            } else {
                script = Util.renderString("def ${RET} = null", map);
            }
        } else {
            map.put("VALUE", valueName);
            if (pos > 0) {
                script = Util.renderString(template_value, map);
            } else if (pos < 0) {
                script = Util.renderString(template_value_2, map);
            } else {
                script = Util.renderString("def ${RET} = null", map);
            }
        }
        return new Tuple(name, script);
    }

    public static Tuple<String, String> substring(String strColumn, int pos, int len, String valueName) {
        // SELECT SUBSTRING('Quadratically',5,6);  => 'ratica';
        // SELECT SUBSTRING('Sakila', -5, 3); => 'aki';
        String template_column = "" +
                "def str = doc['${COLUMN}'].value; if (str == null) return null; if(str == '') return '';" +
                "def len = str.length(); def begin = ${POS} - 1; " +
                "def end = begin + ${LEN}; if (end >= len) end = len; " +
                "def ${RET} = str.substring(begin, end)";
        String template_column_2 = "" +
                "def str = doc['${COLUMN}'].value; if (str == null) return null; if(str == '') return '';" +
                "def len = str.length(); def begin = ${POS} + len; " +
                "def end = begin + ${LEN}; if (end >= len) end = len; " +
                "def ${RET} = str.substring(begin, end)";
        String template_value = "" +
                "def str = ${VALUE}; if (str == null) return null; if(str == '') return ''; " +
                "def len = str.length(); def begin =  ${POS} - 1; " +
                "def end = begin + ${LEN}; if (end >= len) end = len; " +
                "def ${RET} = str.substring(begin, end)";
        String template_value_2 = "" +
                "def str = ${VALUE}; if (str == null) return null; if(str == '') return ''; " +
                "def len = str.length(); def begin =  ${POS} + len; " +
                "def end = begin + ${LEN}; if (end >= len) end = len; " +
                "def ${RET} = str.substring(begin, end)";

        String script = "";
        String name = "substring_" + random();
        Map<String, String> map = new HashMap<>();
        map.put("RET", name);
        map.put("POS", String.valueOf(pos));
        map.put("LEN", String.valueOf(len));

        if (valueName == null) {
            map.put("COLUMN", strColumn);
            if (pos > 0) {
                script = Util.renderString(template_column, map);
            } else if (pos < 0) {
                script = Util.renderString(template_column_2, map);
            } else {
                script = Util.renderString("def ${RET} = null", map);
            }
        } else {
            map.put("VALUE", valueName);
            if (pos > 0) {
                script = Util.renderString(template_value, map);
            } else if (pos < 0) {
                script = Util.renderString(template_value_2, map);
            } else {
                script = Util.renderString("def ${RET} = null", map);
            }
        }
        return new Tuple(name, script);
    }

    //split(Column str, java.lang.String pattern)
    public static Tuple<String, String> split(String strColumn, String pattern, String valueName) {
        String name = "split_" + random();
        if (valueName == null) {
            return new Tuple(name, "def " + name + " = doc['" + strColumn + "'].value.split('" + pattern + "')" );
        } else {
            return new Tuple(name, strColumn + "; def " + name + " = " + valueName + ".split('" + pattern + "')");
        }

    }


}
