package org.nlpcn.es4sql;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.expr.*;
import com.alibaba.druid.util.StringUtils;
import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.elasticsearch.common.collect.Tuple;
import org.nlpcn.es4sql.domain.KVValue;

import java.util.*;

/**
 * Created by allwefantasy on 8/19/16.
 *
 * Updated by liusenhua in 2018/03
 */
public class SQLFunctions {

    //Groovy Built In Functions
    public final static Set<String> buildInFunctions = Sets.newHashSet(
            "exp", "ln", "log", "log10", "sqrt", "cbrt", "ceil", "floor", "rint", "pow", "round",
            "random", "abs", //nummber operator
            "split", "concat", "concat_ws", "substring", "substr", "trim", "instr", "replace",  //string operator
            "add", "multiply", "divide", "subtract", "modulus",//binary operator
            "field", "to_date", "date_format", "to_char",
            "year", "month", "day", "quarter", "week", "now", "today", "date_add", "date_diff", "date_trunc", "date_part",
            "eval" // to support evaluate test-case expression
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
            "}" +
            " " +
            "Long to_date(Object o) {" +
            "    return to_date(o, 'yyyy-MM-dd HH:mm:ss.SSS'); " +
            "}";

    public final static String TO_CHAR_FUNCTION = "to_char";
    public final static String TO_CHAR_FUNCTION_BODY = "" +
            "String to_char(Object o, String pattern) {" +
            "    def v = o; if (v == null) return null;" +
            "    if (v instanceof String) return (String)v;" +
            "    return new SimpleDateFormat(pattern).format(new Date(v));" +
            "}" +
            " " +
            "String to_char(Object o) {" +
            "    return to_char(o, 'yyyy-MM-dd HH:mm:ss.SSS'); " +
            "}";

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

    public final static String INSTR_FUNCTION = "instr";
    public final static String INSTR_FUNCTION_BODY = "" +
            "Integer instr(String src, String target, Integer from_index, Integer nth_appearance) { " +
            "    if (src == null || src == '' || target == null || target == '') return null; " +
            "    int fromIndex = 0; if (from_index != null && from_index.intValue() > 0) { fromIndex = from_index.intValue() - 1; } " +
            "    int total = 1; if (nth_appearance != null && nth_appearance.intValue() > 1) { total = nth_appearance.intValue(); } " +
            "    int count = 0; int index = -1; " +
            "    while(count < total) { " +
            "        index = src.indexOf(target, fromIndex); " +
            "        count = count + 1; " +
            "        fromIndex = index > fromIndex ? index + 1 : fromIndex; " +
            "    } " +
            "    return index + 1; " +
            "} " +
            " " +
            "Integer instr(String src, String target, Integer from_index) { " +
            "    return instr(src, target, from_index, null); " +
            "} " +
            " " +
            "Integer instr(String src, String target) { " +
            "    return instr(src, target, null); " +
            "}";

    public final static String REPLACR_FUNCTION = "replace";
    public final static String REPLACR_FUNCTION_BODY = "" +
            "String replace(String src, String search_str, String replace_str) { " +
            "    if (src == null) return null; " +
            "    return src.replace(search_str, replace_str); " +
            "}";

    public final static String YEAR_FUNCTION = "year";
    public final static String YEAR_FUNCTION_BODY = "" +
            "Long year(Object o) { " +
            "    def v = o; if (v == null || v == 0L) return null; " +
            "    Date d = new Date(v); if (d == null) return null; " +
            "    Calendar c = Calendar.getInstance(); " +
            "    c.setTime(d); " +
            "    int year = c.get(Calendar.YEAR); " +
            "    return year; " +
            "}";

    public final static String MONTH_FUNCTION = "month";
    public final static String MONTH_FUNCTION_BODY = "" +
            "Long month(Object o) { " +
            "    def v = o; if (v == null || v == 0L) return null; " +
            "    Date d = new Date(v); if (d == null) return null; " +
            "    Calendar c = Calendar.getInstance(); " +
            "    c.setTime(d); " +
            "    int month = c.get(Calendar.MONTH); " +
            "    return month + 1; " +
            "}";

    public final static String WEEK_FUNCTION = "week";
    public final static String WEEK_FUNCTION_BODY = "" +
            "Long week(Object o) { " +
            "    def v = o; if (v == null || v == 0L) return null; " +
            "    Date d = new Date(v); if (d == null) return null; " +
            "    Calendar c = Calendar.getInstance(); " +
            "    c.setTime(d); " +
            "    int week = c.get(Calendar.WEEK_OF_YEAR); " +
            "    return week; " +
            "}";

    public final static String DAY_FUNCTION = "day";
    public final static String DAY_FUNCTION_BODY = "" +
            "Long day(Object o) { " +
            "    def v = o; if (v == null || v == 0L) return null; " +
            "    Date d = new Date(v); if (d == null) return null; " +
            "    Calendar c = Calendar.getInstance(); " +
            "    c.setTime(d); " +
            "    int day = c.get(Calendar.DAY_OF_MONTH); " +
            "    return day; " +
            "}";

    public final static String QUARTER_FUNCTION = "quarter";
    public final static String QUARTER_FUNCTION_BODY = "" +
            "Integer quarter(Object o) { " +
            "    def v = o; if (v == null || v == 0L) return null; " +
            "    Date d = new Date(v); if (d == null) return null; " +
            "    Calendar c = Calendar.getInstance(); " +
            "    c.setTime(d); " +
            " " +
            "    int month = c.get(Calendar.MONTH); " +
            "    if (month == Calendar.JANUARY || month == Calendar.FEBRUARY || month == Calendar.MARCH) return 1; " +
            "    else if (month == Calendar.APRIL || month == Calendar.MAY || month == Calendar.JUNE) return 2; " +
            "    else if (month == Calendar.JULY || month == Calendar.AUGUST || month == Calendar.SEPTEMBER) return 3; " +
            "    else if (month == Calendar.OCTOBER || month == Calendar.NOVEMBER || month == Calendar.DECEMBER) return 4; " +
            " " +
            "    return null; " +
            "}";

    public final static String NOW_FUNCTION = "now";
    public final static String NOW_FUNCTION_BODY = "" +
            "Long now() { " +
            "    return new Date().getTime();" +
            "}";

    public final static String TODAY_FUNCTION = "today";
    public final static String TODAY_FUNCTION_BODY = "" +
            "Long today() { " +
            "    Calendar todayStart = Calendar.getInstance(); " +
            "    todayStart.set(Calendar.HOUR_OF_DAY, 0); " +
            "    todayStart.set(Calendar.MINUTE, 0); " +
            "    todayStart.set(Calendar.SECOND, 0); " +
            "    todayStart.set(Calendar.MILLISECOND, 0); " +
            "    Date today = todayStart.getTime(); " +
            "    return today.getTime(); " +
            "}";

    public final static String DATE_ADD_FUNCTION = "date_add";
    public final static String DATE_ADD_FUNCTION_BODY = "" +
            "Long date_add(String date_type, int interval, Object o) { " +
            "    def v = o; if (v == null || v == 0L) return null; " +
            "    Date d = new Date(v); if (d == null) return null; " +
            "    Calendar c = Calendar.getInstance(); " +
            "    c.setTime(d); " +
            "    if (date_type.equalsIgnoreCase('year')) { " +
            "        c.add(Calendar.YEAR, interval); " +
            "    } else if (date_type.equalsIgnoreCase('month')) { " +
            "        c.add(Calendar.MONTH, interval); " +
            "    } else if (date_type.equalsIgnoreCase('quarter')) { " +
            "        c.add(Calendar.MONTH, interval * 3); " +
            "    } else if (date_type.equalsIgnoreCase('day')) { " +
            "        c.add(Calendar.DATE, interval); " +
            "    } else if (date_type.equalsIgnoreCase('week')) { " +
            "        c.add(Calendar.DATE, interval * 7); " +
            "    } " +
            "    return c.getTime().getTime(); " +
            "}";

    public final static String DATE_DIFF_FUNCTION = "date_diff";
    public final static String DATE_DIFF_FUNCTION_BODY = "" +
            "LocalDate to_localdate(Object o) { " +
            "    def v = o; if (v == null || v == 0L) return null; " +
            "    Instant instant = Instant.ofEpochMilli(v); if (instant == null) return null; " +
            "    return instant.atZone(ZoneId.systemDefault()).toLocalDate(); " +
            "} " +
            " " +
            "Long date_diff(String date_type, Object o_1, Object o_2) { " +
            "    LocalDate d1 = to_localdate(o_1); if (d1 == null) return null; " +
            "    LocalDate d2 = to_localdate(o_2); if (d2 == null) return null; " +
            "    if (date_type.equalsIgnoreCase('year')) { " +
            "        return ChronoUnit.YEARS.between(d1, d2); " +
            "    } else if (date_type.equalsIgnoreCase('month')) { " +
            "        return ChronoUnit.MONTHS.between(d1, d2); " +
            "    } else if (date_type.equalsIgnoreCase('day')) { " +
            "        return ChronoUnit.DAYS.between(d1, d2); " +
            "    } else if (date_type.equalsIgnoreCase('week')) { " +
            "        return ChronoUnit.WEEKS.between(d1, d2); " +
            "    } else if (date_type.equalsIgnoreCase('quarter')) { " +
            "        return ChronoUnit.MONTHS.between(d1, d2) / 3; " +
            "    } else { " +
            "        return null; " +
            "    } " +
            "} ";

    public final static String DATE_TRUNC_FUNCTION = "date_trunc";
    public final static String DATE_TRUNC_FUNCTION_BODY = "" +
            "Long date_trunc(String date_type, Object o) { " +
            "    def v = o; if (v == null || v == 0L) return null; " +
            "    Date d = new Date(v); if (d == null) return null; " +
            "    Calendar c = Calendar.getInstance(); " +
            "    c.setTime(d); " +
            "    if (date_type.equalsIgnoreCase('year')) { " +
            "        c.set(Calendar.MONTH, 0); " +
            "        c.set(Calendar.DAY_OF_MONTH, 1); " +
            "        c.set(Calendar.HOUR_OF_DAY, 0); " +
            "        c.set(Calendar.MINUTE, 0); " +
            "        c.set(Calendar.SECOND, 0); " +
            "        c.set(Calendar.MILLISECOND, 0); " +
            "    } else if (date_type.equalsIgnoreCase('month')) { " +
            "        c.set(Calendar.DAY_OF_MONTH, 1); " +
            "        c.set(Calendar.HOUR_OF_DAY, 0); " +
            "        c.set(Calendar.MINUTE, 0); " +
            "        c.set(Calendar.SECOND, 0); " +
            "        c.set(Calendar.MILLISECOND, 0); " +
            "    } else if (date_type.equalsIgnoreCase('day')) { " +
            "        c.set(Calendar.HOUR_OF_DAY, 0); " +
            "        c.set(Calendar.MINUTE, 0); " +
            "        c.set(Calendar.SECOND, 0); " +
            "        c.set(Calendar.MILLISECOND, 0); " +
            "    } else if (date_type.equalsIgnoreCase('hour')) { " +
            "        c.set(Calendar.MINUTE, 0); " +
            "        c.set(Calendar.SECOND, 0); " +
            "        c.set(Calendar.MILLISECOND, 0); " +
            "    } else if (date_type.equalsIgnoreCase('minute')) { " +
            "        c.set(Calendar.SECOND, 0); " +
            "        c.set(Calendar.MILLISECOND, 0); " +
            "    } else if (date_type.equalsIgnoreCase('second')) { " +
            "        c.set(Calendar.MILLISECOND, 0); " +
            "    } " +
            "    return c.getTime().getTime(); " +
            "}";

    public final static Map<String, String> extendFunctions = new TreeMap<>();
    static {
        extendFunctions.put(ROUND_FUNCTION, ROUND_FUNCTION_BODY);
        extendFunctions.put(LOG_FUNCTION, LOG_FUNCTION_BODY);
        extendFunctions.put(TO_DATE_FUNCTION, TO_DATE_FUNCTION_BODY);
        extendFunctions.put(TO_CHAR_FUNCTION, TO_CHAR_FUNCTION_BODY);
        extendFunctions.put(SUBSTRING_FUNCTION, SUBSTRING_FUNCTION_BODY);
        extendFunctions.put(INSTR_FUNCTION, INSTR_FUNCTION_BODY);
        extendFunctions.put(REPLACR_FUNCTION, REPLACR_FUNCTION_BODY);
        extendFunctions.put(YEAR_FUNCTION, YEAR_FUNCTION_BODY);
        extendFunctions.put(MONTH_FUNCTION, MONTH_FUNCTION_BODY);
        extendFunctions.put(WEEK_FUNCTION, WEEK_FUNCTION_BODY);
        extendFunctions.put(DAY_FUNCTION, DAY_FUNCTION_BODY);
        extendFunctions.put(QUARTER_FUNCTION, QUARTER_FUNCTION_BODY);
        extendFunctions.put(NOW_FUNCTION, NOW_FUNCTION_BODY);
        extendFunctions.put(TODAY_FUNCTION, TODAY_FUNCTION_BODY);
        extendFunctions.put(DATE_ADD_FUNCTION, DATE_ADD_FUNCTION_BODY);
        extendFunctions.put(DATE_DIFF_FUNCTION, DATE_DIFF_FUNCTION_BODY);
        extendFunctions.put(DATE_TRUNC_FUNCTION, DATE_TRUNC_FUNCTION_BODY);
    }

    public static Tuple<String, String> function(String methodName, List<KVValue> paramers, boolean returnValue, Map<String, String> functions) {
        Tuple<String, String> functionStr = null;
        switch (methodName) {
            case "split":
                if (paramers.size() == 3) {
                    functionStr = split(Util.expr2Object((SQLExpr) paramers.get(0).value).toString(),
                            Util.expr2Object((SQLExpr) paramers.get(1).value).toString(),
                            Integer.parseInt(Util.expr2Object((SQLExpr) paramers.get(2).value).toString()), paramers.get(0).key);
                } else {
                    functionStr = split(paramers.get(0).value.toString(),
                            paramers.get(1).value.toString(),
                            paramers.get(0).key);
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

            case "now":
                functionStr = now(paramers, functions);
                break;

            case "today":
                functionStr = today(paramers, functions);
                break;

            case "date_add":
                functionStr = dateAdd(paramers, functions);
                break;

            case "date_diff":
                functionStr = dateDiff(paramers, functions);
                break;

            case "date_trunc":
                functionStr = dateTrunc(paramers, functions);
                break;

            case "pow":
                functionStr = pow(
                        Util.expr2Object((SQLExpr) paramers.get(0).value).toString(),
                        Util.expr2Object((SQLExpr) paramers.get(1).value).toString(),
                        paramers.get(0).key);
                break;

            case "round":
                functionStr = round(paramers, functions);
                break;

            case "ln":
            case "log":
                functionStr = log(paramers, functions);
                break;

            case "log10":
                functionStr = log10(Util.expr2Object((SQLExpr) paramers.get(0).value).toString(), paramers.get(0).key);
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
                        paramers.get(0).key);
                break;

            case "substr":
            case "substring":
                functionStr = substring(paramers, functions);
                break;

            case "instr":
                functionStr = instr(paramers, functions);
                break;

            case "replace":
                functionStr = replace(paramers, functions);
                break;

            case "trim":
                functionStr = trim(Util.expr2Object((SQLExpr) paramers.get(0).value).toString(), paramers.get(0).key);
                break;

            case "add":
                functionStr = add(paramers.get(0), paramers.get(1));
                break;

            case "subtract":
                functionStr = subtract(paramers.get(0), paramers.get(1));
                break;
            case "divide":
                functionStr = divide(paramers.get(0), paramers.get(1));
                break;

            case "multiply":
                functionStr = multiply(paramers.get(0), paramers.get(1));
                break;
            case "modulus":
                functionStr = modulus(paramers.get(0), paramers.get(1));
                break;

            case "field":
                functionStr = field(Util.expr2Object((SQLExpr) paramers.get(0).value).toString());
                break;

            case "eval":
                functionStr = eval(Util.expr2Object((SQLExpr) paramers.get(0).value).toString(), functions);
                break;

            default:

        }
        if(returnValue){
            String extendFunctionScript = "";
            if (functions != null) {
                extendFunctionScript = Joiner.on(" ").join(functions.values());
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

    public static Tuple<String, String> field(String a) {
        String name = "field_" + random();
        return new Tuple<>(name, "def " + name + " = " + "doc['" + a + "'].value");
    }

    public static Tuple<String, String> eval(String a, Map<String, String> functions) {
        String name = "eval_" + random();
        String func = "func_" + random();
        String func_body = "def " + func + "(def doc) { " + a + " }";
        define(func, func_body, functions);
        return new Tuple<>(name, "def " + name + " = " + func + "(doc)");
    }

    public static Tuple<String, String> add(KVValue a, KVValue b) {
        return binaryOpertator("add", "+", a, b);
    }

    private static Tuple<String, String> modulus(KVValue a, KVValue b) {
        return binaryOpertator("modulus", "%", a, b);
    }

    private static Tuple<String, String> subtract(KVValue a, KVValue b) {
        return binaryOpertator("subtract", "-", a, b);
    }

    private static Tuple<String, String> multiply(KVValue a, KVValue b) {
        return binaryOpertator("multiply", "*", a, b);
    }

    private static Tuple<String, String> divide(KVValue a, KVValue b) {
        return binaryOpertator("divide", "/", a, b);
    }

    private static Tuple<String, String> binaryOpertator(String methodName, String operator, KVValue a, KVValue b) {
        String name = methodName + "_" + random();
        //String template = "def ${RET} = null; if (${ARG1} != null && ${ARG2} != null) { ${RET} = ${ARG1} ${OP} ${ARG2}; }";
        String template = "def ${RET} = ${ARG1} ${OP} ${ARG2}";

        Map<String, String> map = new HashMap<>();
        map.put("RET", name);
        map.put("OP", operator);
        map.put("ARG1", getValue(a));
        map.put("ARG2", getValue(b));
        String script = Util.renderString(template, map);

        String evaluateScript = "";
        List<KVValue> parameters = Arrays.asList(a, b);
        for (KVValue p: parameters) {
            if (p.valueType == KVValue.ValueType.EVALUATED) {
                String str = Util.expr2Object((SQLExpr) p.value).toString();
                if (!StringUtils.isEmpty(str)) {
                    evaluateScript = evaluateScript +  "; " + str;
                }
            }
            String str2 = convertType(p);
            if (!StringUtils.isEmpty(str2)) {
                evaluateScript = evaluateScript +  "; " + str2;
            }
        }

        if (evaluateScript != "") {
            script = evaluateScript.substring(1) + "; " + script; // trim the ";'
        }

        return new Tuple<>(name, script);
    }

    private static String convertType(KVValue param) {
        if (param.valueType == KVValue.ValueType.REFERENCE || param.valueType == KVValue.ValueType.REFERENCE) {
            //for now ,if variant is string,then change to double.
            String temp = param.key;

            return "if( " + temp + " instanceof String ) " + temp + " = Double.parseDouble(" + temp.trim() + ")";
        } else return "";
    }

    private static boolean isProperty(SQLExpr expr) {
        return (expr instanceof SQLIdentifierExpr || expr instanceof SQLPropertyExpr || expr instanceof SQLVariantRefExpr);
    }

    public static Tuple<String, String> round(List<KVValue> parameters, Map<String, String> functions) {
        define(SQLFunctions.ROUND_FUNCTION, functions);
        if (parameters.size() == 2) {
            return invoke(SQLFunctions.ROUND_FUNCTION, parameters.get(0), parameters.get(1));
        } else {
            return invoke(SQLFunctions.ROUND_FUNCTION, parameters.get(0));
        }
    }

    public static Tuple<String, String> log(List<KVValue> parameters, Map<String, String> functions) {
        define(SQLFunctions.LOG_FUNCTION, functions);
        if (parameters.size() == 2) {
            return invoke(SQLFunctions.LOG_FUNCTION, parameters.get(0), parameters.get(1));
        } else {
            return invoke(SQLFunctions.LOG_FUNCTION, parameters.get(0));
        }
    }

    public static Tuple<String, String> to_date(List<KVValue> parameters, Map<String, String> functions) {
        define(SQLFunctions.TO_DATE_FUNCTION, functions);
        if (parameters.size() >= 2) {
            return invoke(SQLFunctions.TO_DATE_FUNCTION, parameters.get(0), parameters.get(1));
        } else {
            return invoke(SQLFunctions.TO_DATE_FUNCTION, parameters.get(0));
        }
    }

    public static Tuple<String, String> to_char(List<KVValue> parameters, Map<String, String> functions) {
        define(SQLFunctions.TO_CHAR_FUNCTION, functions);
        if (parameters.size() >= 2) {
            return invoke(SQLFunctions.TO_CHAR_FUNCTION, parameters.get(0), parameters.get(1));
        } else {
            return invoke(SQLFunctions.TO_CHAR_FUNCTION, parameters.get(0));
        }
    }

    public static Tuple<String, String> year(List<KVValue> parameters, Map<String, String> functions) {
        define(SQLFunctions.YEAR_FUNCTION, functions);
        return invoke(SQLFunctions.YEAR_FUNCTION, parameters.get(0));
    }

    public static Tuple<String, String> month(List<KVValue> parameters, Map<String, String> functions) {
        define(SQLFunctions.MONTH_FUNCTION, functions);
        return invoke(SQLFunctions.MONTH_FUNCTION, parameters.get(0));
    }

    public static Tuple<String, String> week(List<KVValue> parameters, Map<String, String> functions) {
        define(SQLFunctions.WEEK_FUNCTION, functions);
        return invoke(SQLFunctions.WEEK_FUNCTION, parameters.get(0));
    }

    public static Tuple<String, String> day(List<KVValue> parameters, Map<String, String> functions) {
        define(SQLFunctions.DAY_FUNCTION, functions);
        return invoke(SQLFunctions.DAY_FUNCTION, parameters.get(0));
    }

    public static Tuple<String, String> quarter(List<KVValue> parameters, Map<String, String> functions) {
        define(SQLFunctions.QUARTER_FUNCTION, functions);
        return invoke(SQLFunctions.QUARTER_FUNCTION, parameters.get(0));
    }

    public static Tuple<String, String> now(List<KVValue> parameters, Map<String, String> functions) {
        define(SQLFunctions.NOW_FUNCTION, functions);
        return invoke(SQLFunctions.NOW_FUNCTION);
    }

    public static Tuple<String, String> today(List<KVValue> parameters, Map<String, String> functions) {
        define(SQLFunctions.TODAY_FUNCTION, functions);
        return invoke(SQLFunctions.TODAY_FUNCTION);
    }

    public static Tuple<String, String> dateAdd(List<KVValue> parameters, Map<String, String> functions) {
        define(SQLFunctions.DATE_ADD_FUNCTION, functions);
        return invoke(SQLFunctions.DATE_ADD_FUNCTION, parameters.get(0), parameters.get(1), parameters.get(2));
    }

    public static Tuple<String, String> dateDiff(List<KVValue> parameters, Map<String, String> functions) {
        define(SQLFunctions.DATE_DIFF_FUNCTION, functions);
        return invoke(SQLFunctions.DATE_DIFF_FUNCTION, parameters.get(0), parameters.get(1), parameters.get(2));
    }

    public static Tuple<String, String> dateTrunc(List<KVValue> parameters, Map<String, String> functions) {
        define(SQLFunctions.DATE_TRUNC_FUNCTION, functions);
        return invoke(SQLFunctions.DATE_TRUNC_FUNCTION, parameters.get(0), parameters.get(1));
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

    public static Tuple<String, String> substring(List<KVValue> parameters, Map<String, String> functions) {
        define(SQLFunctions.SUBSTRING_FUNCTION, functions);
        if (parameters.size() == 3) {
            return invoke(SQLFunctions.SUBSTRING_FUNCTION, parameters.get(0), parameters.get(1), parameters.get(2));
        } else {
            return invoke(SQLFunctions.SUBSTRING_FUNCTION, parameters.get(0), parameters.get(1));
        }
    }

    public static Tuple<String, String> instr(List<KVValue> parameters, Map<String, String> functions) {
        define(SQLFunctions.INSTR_FUNCTION, functions);
        if (parameters.size() == 4) {
            return invoke(SQLFunctions.INSTR_FUNCTION, parameters.get(0), parameters.get(1), parameters.get(2), parameters.get(3));
        } else if (parameters.size() == 3){
            return invoke(SQLFunctions.INSTR_FUNCTION, parameters.get(0), parameters.get(1), parameters.get(2));
        } else {
            return invoke(SQLFunctions.INSTR_FUNCTION, parameters.get(0), parameters.get(1));
        }
    }

    public static Tuple<String, String> replace(List<KVValue> parameters, Map<String, String> functions) {
        define(SQLFunctions.REPLACR_FUNCTION, functions);
        return invoke(SQLFunctions.REPLACR_FUNCTION, parameters.get(0), parameters.get(1), parameters.get(2));
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
        if (param.valueType == KVValue.ValueType.EVALUATED ||
                param.valueType == KVValue.ValueType.REFERENCE ) {
            return param.key;
        } else if (isProperty((SQLExpr) param.value)) {
            return "doc['" + value + "'].value";
        }else {
            return value;
        }
    }

    private static void define(String func, Map<String, String> functions) {
        if (extendFunctions.containsKey(func) && !functions.containsKey(func)) {
            functions.put(func, extendFunctions.get(func));
        }
    }

    private static void define(String func, String body, Map<String, String> functions) {
        if (!StringUtils.isEmpty(body)) {
            functions.put(func, body);
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

    public static Tuple<String, String> invoke(String methodName, KVValue arg1, KVValue arg2, KVValue arg3, KVValue arg4 ) {
        String name = methodName + "_" + random();
        String template = "def ${RET} = ${FUNC}(${ARG1}, ${ARG2}, ${ARG3}, ${ARG4})";

        Map<String, String> map = new HashMap<>();
        map.put("FUNC", methodName);
        map.put("RET", name);
        map.put("ARG1", getValue(arg1));
        map.put("ARG2", getValue(arg2));
        map.put("ARG3", getValue(arg3));
        map.put("ARG4", getValue(arg4));
        String script = Util.renderString(template, map);

        List<KVValue> parameters = Arrays.asList(arg1, arg2, arg3, arg4);
        for (KVValue p: parameters) {
            if (p.valueType == KVValue.ValueType.EVALUATED) {
                script = Util.expr2Object((SQLExpr) p.value).toString() + ";" + script;
            }
        }

        return new Tuple<>(name, script);
    }


}
