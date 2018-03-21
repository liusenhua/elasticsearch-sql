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
            "split", "concat", "concat_ws", "substring", "substr", "trim",//string operator
            "add", "multiply", "divide", "subtract", "modulus",//binary operator
            "field", "to_date", "date_format", "to_char", "year", "month", "day", "quarter", "week"
    );

    public final static String ROUND_FUNCTION = "round";
    public final static String ROUND_FUNCTION_BODY = "" +
            "Object round(Object o) { " +
            "  def d = o; if (d == null) return null; " +
            "  return Math.round(Double.valueOf(d));" +
            "}" +
            " " +
            "Double round(Object o, int decimals) { " +
            "  def d = o; if (d == null || decimals < 0) return null; " +
            "  BigDecimal bd = BigDecimal.valueOf(d); " +
            "  double ret = bd.setScale(decimals, RoundingMode.HALF_UP).doubleValue(); " +
            "  return ret; " +
            "}";

    public final static String LOG_FUNCTION = "log";
    public final static String LOG_FUNCTION_BODY = "" +
            "double log(double base, double d) { " +
            "  if (base <=0 || d <=0) return Float.NaN; " +
            "  return Math.log(d)/Math.log(base); " +
            "} " +
            " " +
            "double log(double d) { " +
            "  return Math.log(d); " +
            "}";

    public final static String TO_DATE_FUNCTION = "to_date";
    public final static String TO_DATE_FUNCTION_BODY = "" +
            "Long to_date(Object o, String pattern) {" +
            "   def v = o; if (v == null || v == '' || v == 0) return null; " +
            "   Date d = (v instanceof String) ? new SimpleDateFormat(pattern).parse(v) : new Date(v); " +
//            "   SimpleDateFormat sdf = new SimpleDateFormat(\"yyyy-MM-dd'T'HH:mm:ss.SSSXXX\"); " +
//            "   return sdf.format(d);" +
            "   return d.getTime();" +
            "}";

    public final static String TO_CHAR_FUNCTION = "to_char";
    public final static String TO_CHAR_FUNCTION_BODY = "" +
            "String to_char(Object o, String pattern) {" +
            "    def v = o; if (v == null) return null;" +
            "    if (v instanceof String) return (String)v;" +
            "    return new SimpleDateFormat(pattern).format(new Date(v));" +
            "}" ;

    public final static String SUBSTRING_FUNCTION = "substring";
    public final static String SUBSTRING_FUNCTION_BODY = "" +
            " String substring(String str, int pos, int l) {" +
            "   if (str == null || pos == 0) return null; " +
            "   if (str == '') return ''; " +
            "   def len = str.length(); def begin = pos > 0 ? pos - 1 : pos + len; " +
            "   def end = begin + l; if (end >= len) end = len; " +
            "   return str.substring(begin, end);" +
            " }" +
            " " +
            " String substring(String str, int pos) {" +
            "   if (str == null || pos == 0) return null; " +
            "   if (str == '') return ''; " +
            "   def len = str.length(); def begin = pos > 0 ? pos - 1 : pos + len; " +
            "   return str.substring(begin); " +
            " }";

    public final static String YEAR_FUNCTION = "year";
    public final static String YEAR_FUNCTION_BODY = "" +
            "Long year(Object o) { " +
            "    def v = o; if (v == null ) return null; " +
            "    Date d = new Date(v); if (d == null) return null; " +
            "    Calendar c = Calendar.getInstance(); " +
            "    c.setTime(d); " +
            "    int year = c.get(Calendar.YEAR); " +
            "    return year; " +
            "}";

    public final static String MONTH_FUNCTION = "month";
    public final static String MONTH_FUNCTION_BODY = "" +
            "Long month(Object o) { " +
            "    def v = o; if (v == null) return null; " +
            "    Date d = new Date(v); if (d == null) return null; " +
            "    Calendar c = Calendar.getInstance(); " +
            "    c.setTime(d); " +
            "    int month = c.get(Calendar.MONTH); " +
            "    return month + 1; " +
            "}";

    public final static String WEEK_FUNCTION = "week";
    public final static String WEEK_FUNCTION_BODY = "" +
            "Long week(Object o) { " +
            "    def v = o; if (v == null) return null; " +
            "    Date d = new Date(v); if (d == null) return null; " +
            "    Calendar c = Calendar.getInstance(); " +
            "    c.setTime(d); " +
            "    int week = c.get(Calendar.WEEK_OF_YEAR); " +
            "    return week; " +
            "}";

    public final static String DAY_FUNCTION = "day";
    public final static String DAY_FUNCTION_BODY = "" +
            "Long day(Object o) { " +
            "    def v = o; if (v == null) return null; " +
            "    Date d = new Date(v); if (d == null) return null; " +
            "    Calendar c = Calendar.getInstance(); " +
            "    c.setTime(d); " +
            "    int day = c.get(Calendar.DAY_OF_MONTH); " +
            "    return day; " +
            "}";

    public final static String QUARTER_FUNCTION = "quarter";
    public final static String QUARTER_FUNCTION_BODY = "" +
            "Integer quarter(Object o) {\n" +
            "    def v = o; if (v == null) return null;\n" +
            "    Date d = new Date(v); if (d == null) return null;  \n" +
            "    Calendar c = Calendar.getInstance();  \n" +
            "    c.setTime(d);\n" +
            " " +
            "    int month = c.get(Calendar.MONTH);\n" +
            "    if (month == Calendar.JANUARY || month == Calendar.FEBRUARY || month == Calendar.MARCH) return 1;\n" +
            "    else if (month == Calendar.APRIL || month == Calendar.MAY || month == Calendar.JUNE) return 2;\n" +
            "    else if (month == Calendar.JULY || month == Calendar.AUGUST || month == Calendar.SEPTEMBER) return 3;\n" +
            "    else if (month == Calendar.OCTOBER || month == Calendar.NOVEMBER || month == Calendar.DECEMBER) return 4;\n" +
            "\n" +
            "    return null;  \n" +
            "}";

    public final static Map<String, String> extendFunctions = new TreeMap<>();
    static {
        extendFunctions.put(ROUND_FUNCTION, ROUND_FUNCTION_BODY);
        extendFunctions.put(LOG_FUNCTION, LOG_FUNCTION_BODY);
        extendFunctions.put(TO_DATE_FUNCTION, TO_DATE_FUNCTION_BODY);
        extendFunctions.put(TO_CHAR_FUNCTION, TO_CHAR_FUNCTION_BODY);
        extendFunctions.put(SUBSTRING_FUNCTION, SUBSTRING_FUNCTION_BODY);
        extendFunctions.put(YEAR_FUNCTION, YEAR_FUNCTION_BODY);
        extendFunctions.put(MONTH_FUNCTION, MONTH_FUNCTION_BODY);
        extendFunctions.put(WEEK_FUNCTION, WEEK_FUNCTION_BODY);
        extendFunctions.put(DAY_FUNCTION, DAY_FUNCTION_BODY);
        extendFunctions.put(QUARTER_FUNCTION, QUARTER_FUNCTION_BODY);
    }

    public static Tuple<String, String> function(String methodName, List<KVValue> paramers, String name,boolean returnValue, Set<String> functions) {
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

            case "concat":
                functionStr = concat_ws("", paramers);
                break;

            case "concat_ws":
                functionStr = concat_ws(paramers.get(0).value.toString(), paramers.subList(1, paramers.size()));
                break;

            case "to_date":
                functionStr = to_date(paramers, functions);
                break;

            case "date_format":
            case "to_char":
                functionStr = to_char(paramers, functions);
                break;

            case "year":
                functionStr = year(paramers, functions);
                break;

            case "month":
                functionStr = month(paramers, functions);
                break;

            case "week":
                functionStr = week(paramers, functions);
                break;

            case "day":
                functionStr = day(paramers, functions);
                break;

            case "quarter":
                functionStr = quarter(paramers, functions);
                break;

            case "pow":
                functionStr = pow(
                        Util.expr2Object((SQLExpr) paramers.get(0).value).toString(),
                        Util.expr2Object((SQLExpr) paramers.get(1).value).toString(),
                        name);
                break;

            case "round":
                functionStr = round(paramers, functions);
                break;

            case "ln":
            case "log":
                functionStr = log(paramers, functions);
                break;

            case "log10":
                functionStr = log10(Util.expr2Object((SQLExpr) paramers.get(0).value).toString(), name);
                break;

            case "floor":
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
                functionStr = substring(paramers, functions);
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
            String extendFunctionScript = "";
            List<String> extendFunctionList = new ArrayList<>();
            if (functions != null) {
                for (String fun: functions) {
                    if (extendFunctions.containsKey(fun)) {
                        extendFunctionList.add(extendFunctions.get(fun));
                    }
                }
                extendFunctionScript = Joiner.on(" ").join(extendFunctionList);
                extendFunctionScript = extendFunctionScript.trim();
            }

            String generatedFieldName = functionStr.v1();
            String returnCommand = ";return " + generatedFieldName +";" ;
            String newScript = extendFunctionScript + " " + functionStr.v2() + returnCommand;
            functionStr = new Tuple<>(generatedFieldName, newScript);
        }
        return functionStr;
    }

    public static String random() {
        return Math.abs(new Random().nextInt()) + "";
    }

    private static Tuple<String, String> concat_ws(String split, List<KVValue> columns) {
        String name = "concat_ws_" + random();
        List<String> result = Lists.newArrayList();
        for (KVValue column : columns) {
            String strColumn = getValue(column);
            result.add(strColumn);
        }

        String sep = " + ";
        if (split != "") {
            sep = " + " + split + " + ";
        }
        String script = "def " + name + " = " + Joiner.on(sep).join(result);

        for (KVValue p: columns) {
            if (p.valueType == KVValue.ValueType.EVALUATED) {
                script = Util.expr2Object((SQLExpr) p.value).toString() + ";" + script;
            }
        }

        return new Tuple<>(name, script );
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

    public static Tuple<String, String> round(List<KVValue> parameters, Set<String> functions) {
        functions.add(SQLFunctions.ROUND_FUNCTION);
        if (parameters.size() == 2) {
            return invoke(SQLFunctions.ROUND_FUNCTION, parameters.get(0), parameters.get(1));
        } else {
            return invoke(SQLFunctions.ROUND_FUNCTION, parameters.get(0));
        }
    }

    public static Tuple<String, String> log(List<KVValue> parameters, Set<String> functions) {
        functions.add(SQLFunctions.LOG_FUNCTION);
        if (parameters.size() == 2) {
            return invoke(SQLFunctions.LOG_FUNCTION, parameters.get(0), parameters.get(1));
        } else {
            return invoke(SQLFunctions.LOG_FUNCTION, parameters.get(0));
        }
    }

    public static Tuple<String, String> to_date(List<KVValue> parameters, Set<String> functions) {
        functions.add(SQLFunctions.TO_DATE_FUNCTION);
        return invoke(SQLFunctions.TO_DATE_FUNCTION, parameters.get(0), parameters.get(1));
    }

    public static Tuple<String, String> to_char(List<KVValue> parameters, Set<String> functions) {
        functions.add(SQLFunctions.TO_CHAR_FUNCTION);
        return invoke(SQLFunctions.TO_CHAR_FUNCTION, parameters.get(0), parameters.get(1));
    }

    public static Tuple<String, String> year(List<KVValue> parameters, Set<String> functions) {
        functions.add(SQLFunctions.YEAR_FUNCTION);
        return invoke(SQLFunctions.YEAR_FUNCTION, parameters.get(0));
    }

    public static Tuple<String, String> month(List<KVValue> parameters, Set<String> functions) {
        functions.add(SQLFunctions.MONTH_FUNCTION);
        return invoke(SQLFunctions.MONTH_FUNCTION, parameters.get(0));
    }

    public static Tuple<String, String> week(List<KVValue> parameters, Set<String> functions) {
        functions.add(SQLFunctions.WEEK_FUNCTION);
        return invoke(SQLFunctions.WEEK_FUNCTION, parameters.get(0));
    }

    public static Tuple<String, String> day(List<KVValue> parameters, Set<String> functions) {
        functions.add(SQLFunctions.DAY_FUNCTION);
        return invoke(SQLFunctions.DAY_FUNCTION, parameters.get(0));
    }

    public static Tuple<String, String> quarter(List<KVValue> parameters, Set<String> functions) {
        functions.add(SQLFunctions.QUARTER_FUNCTION);
        return invoke(SQLFunctions.QUARTER_FUNCTION, parameters.get(0));
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

    public static Tuple<String, String> substring(List<KVValue> parameters, Set<String> functions) {
        functions.add(SQLFunctions.SUBSTRING_FUNCTION);
        if (parameters.size() == 3) {
            return invoke(SQLFunctions.SUBSTRING_FUNCTION, parameters.get(0), parameters.get(1), parameters.get(2));
        } else {
            return invoke(SQLFunctions.SUBSTRING_FUNCTION, parameters.get(0), parameters.get(1));
        }
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

    private static String getValue(KVValue param) {
        String value = Util.expr2Object((SQLExpr) param.value, "'").toString();
        if (param.valueType == KVValue.ValueType.EVALUATED) {
            return param.key;
        } else if (isProperty((SQLExpr) param.value)) {
            return "doc['" + value + "'].value";
        }else {
            return value;
        }
    }

    public static Tuple<String, String> invoke(String methodName) {
        String name = methodName + "_" + random();
        String template = "def ${RET} = ${FUNC}()";

        Map<String, String> map = new HashMap<>();
        map.put("FUNC", methodName);
        map.put("RET", name);
        String script = Util.renderString(template, map);;

        return new Tuple<>(name, script);
    }

    public static Tuple<String, String> invoke(String methodName, KVValue arg1 ) {
        String name = methodName + "_" + random();
        String template = "def ${RET} = ${FUNC}(${ARG1})";

        Map<String, String> map = new HashMap<>();
        map.put("FUNC", methodName);
        map.put("RET", name);
        map.put("ARG1", getValue(arg1));
        String script = Util.renderString(template, map);

        List<KVValue> parameters = Arrays.asList(arg1);
        for (KVValue p: parameters) {
            if (p.valueType == KVValue.ValueType.EVALUATED) {
                script = Util.expr2Object((SQLExpr) p.value).toString() + ";" + script;
            }
        }

        return new Tuple<>(name, script);
    }

    public static Tuple<String, String> invoke(String methodName, KVValue arg1, KVValue arg2 ) {
        String name = methodName + "_" + random();
        String template = "def ${RET} = ${FUNC}(${ARG1}, ${ARG2})";

        Map<String, String> map = new HashMap<>();
        map.put("FUNC", methodName);
        map.put("RET", name);
        map.put("ARG1", getValue(arg1));
        map.put("ARG2", getValue(arg2));
        String script = Util.renderString(template, map);

        List<KVValue> parameters = Arrays.asList(arg1, arg2);
        for (KVValue p: parameters) {
            if (p.valueType == KVValue.ValueType.EVALUATED) {
                script = Util.expr2Object((SQLExpr) p.value).toString() + ";" + script;
            }
        }

        return new Tuple<>(name, script);
    }

    public static Tuple<String, String> invoke(String methodName, KVValue arg1, KVValue arg2, KVValue arg3 ) {
        String name = methodName + "_" + random();
        String template = "def ${RET} = ${FUNC}(${ARG1}, ${ARG2}, ${ARG3})";

        Map<String, String> map = new HashMap<>();
        map.put("FUNC", methodName);
        map.put("RET", name);
        map.put("ARG1", getValue(arg1));
        map.put("ARG2", getValue(arg2));
        map.put("ARG3", getValue(arg3));
        String script = Util.renderString(template, map);

        List<KVValue> parameters = Arrays.asList(arg1, arg2, arg3);
        for (KVValue p: parameters) {
            if (p.valueType == KVValue.ValueType.EVALUATED) {
                script = Util.expr2Object((SQLExpr) p.value).toString() + ";" + script;
            }
        }

        return new Tuple<>(name, script);
    }


}
