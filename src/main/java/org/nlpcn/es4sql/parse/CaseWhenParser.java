package org.nlpcn.es4sql.parse;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.expr.*;
import com.alibaba.druid.util.StringUtils;
import com.google.common.base.Joiner;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.inject.internal.Join;
import org.nlpcn.es4sql.SQLFunctions;
import org.nlpcn.es4sql.Util;
import org.nlpcn.es4sql.domain.Condition;
import org.nlpcn.es4sql.domain.Condition.OPEAR;
import org.nlpcn.es4sql.domain.MethodField;
import org.nlpcn.es4sql.domain.Where;
import org.nlpcn.es4sql.exception.SqlParseException;

import java.util.*;

/**
 * Created by allwefantasy on 9/3/16.
 */
public class CaseWhenParser {
    private SQLCaseExpr caseExpr;
    private String alias;
    private String tableAlias;
    private Map<String, String> sqlFunctions = new TreeMap<>();

    public CaseWhenParser(SQLCaseExpr caseExpr, String alias, String tableAlias) {
        this.alias = alias;
        this.tableAlias = tableAlias;
        this.caseExpr = caseExpr;
    }

    public Map<String, String> getSqlFunctions() {
        return sqlFunctions;
    }

    public String parse() throws SqlParseException {
        List<String> result = new ArrayList<String>();

        List<String> conditions = new ArrayList<>();
        boolean addIfStatement = false;
        for (SQLCaseExpr.Item item : caseExpr.getItems()) {
            SQLExpr conditionExpr = item.getConditionExpr();

            WhereParser parser = new WhereParser(new SqlParser(), conditionExpr);
            parser.concatSqlFunction(false);

            Tuple<String, String> ret = explain(parser.findWhere());
            String statementCodes = ret.v1();
            String conditionCodes = ret.v2();
            if (conditionCodes.startsWith(" &&")) {
                conditionCodes = conditionCodes.substring(3);
            }

            result.add(statementCodes);

            // collect condition codes
            if (addIfStatement == false) {
                addIfStatement = true;
                conditions.add("if(" + conditionCodes + ")" + "{" + parseValueExpr(item.getValueExpr()) + "}");
            } else {
                conditions.add("else if(" + conditionCodes + ")" + "{" + parseValueExpr(item.getValueExpr()) + "}");
            }

            // collect sql functions defined in condition expression
            this.sqlFunctions.putAll(parser.getSqlFunctions());
        }

        result.addAll(conditions);

        SQLExpr elseExpr = caseExpr.getElseExpr();
        if (elseExpr == null) {
            result.add("else {" +  returnNull() + "}");
        } else {
            result.add("else {" + parseValueExpr(elseExpr) + "}");
        }

        return Joiner.on(" ").join(result);
    }

    private String parseValueExpr(SQLExpr expr) throws SqlParseException {
        if (expr instanceof SQLMethodInvokeExpr) {
            // return ((SQLMethodInvokeExpr) expr).toString();
            SQLMethodInvokeExpr mExpr = (SQLMethodInvokeExpr) expr;
            return parseSQLMethodInvokeExpr(mExpr.getMethodName(), mExpr.getParameters());
        } else if (expr instanceof SQLBinaryOpExpr) {
            SQLMethodInvokeExpr mExpr = FieldMaker.makeBinaryMethodField((SQLBinaryOpExpr) expr, null, false);
            return parseSQLMethodInvokeExpr(mExpr.getMethodName(), mExpr.getParameters());
        } else if (expr instanceof SQLAggregateExpr) {
            SQLAggregateExpr sExpr = (SQLAggregateExpr) expr;
            return parseSQLMethodInvokeExpr(sExpr.getMethodName(), sExpr.getArguments());
        }

        return returnWrapper(Util.getScriptValueWithQuote(expr, "'").toString());
    }

    private String parseSQLMethodInvokeExpr(String name, List<SQLExpr> arguments) throws SqlParseException {
        Map<String, String> functions = this.sqlFunctions;
        MethodField methodField = FieldMaker.makeMethodField(name,
                arguments,
                null,
                null,
                this.tableAlias,
                false,
                functions);

        String ret = methodField.getParams().get(0).value.toString();
        String script = "";
        if (methodField.getParams().size() == 2) {
            script = methodField.getParams().get(1).value.toString();
            if (script != "" && !script.endsWith("}")) {
                script = script + "; ";
            }
        }
        return script + returnWrapper(ret);
    }

    private String returnNull() {
        return "return null;";
    }

    private String returnWrapper(Object statement) {
        return "return " + statement.toString() + ";";
    }

    public Tuple<String, String> explain(Where where) throws SqlParseException {
        List<String> statements = new ArrayList<String>();
        List<String> conditions = new ArrayList<String>();
        while (where.getWheres().size() == 1) {
            where = where.getWheres().getFirst();
        }
        explainWhere(statements, conditions, where);

        // Fix the "Extraneous if statement" IllegalArgumentException from ES
        // by define a variable to hold the condition value.
        List<String> conditionVariables = new ArrayList<>();
        for (String condition: conditions) {
            String conditionVariable = "condition_" + Math.abs(new Random().nextInt());
            conditionVariables.add(conditionVariable);

            String conditionStatement = "def " + conditionVariable + " = " + condition + ";";
            statements.add(conditionStatement);
        }

        String statementCodes = Joiner.on(" ").join(statements);
        String relation = where.getConn().name().equals("AND") ? " && " : " || ";
        String conditionCodes = Joiner.on(relation).join(conditionVariables);

        return new Tuple<>(statementCodes, conditionCodes);
    }


    private void explainWhere(List<String> statements, List<String> codes, Where where) throws SqlParseException {
        if (where instanceof Condition) {
            Condition condition = (Condition) where;

            if (condition.getValue() instanceof ScriptFilter) {
                // Split the script for method invoke, to fix the issue that
                //  if (def date_part_265146825 = date_part('month', doc['createTime'].value);date_part_265146825 < 10)
                //  =>
                //  def date_part_265146825 = date_part('month', doc['createTime'].value); if (date_part_265146825 < 10)

                // Split the origin script with the last ";" or "}"
                String script = ((ScriptFilter) condition.getValue()).getScript();
                String statement = "";
                String conditionStatement = script;
                for (int i = script.length() - 1; i >= 0; i--) {
                    if (script.charAt(i) == ';' || script.charAt(i) == '}') {
                        statement = script.substring(0, i + 1);
                        conditionStatement = script.substring(i + 1, script.length());
                        break;
                    }
                }

                if (!StringUtils.isEmpty(statement)) {
                    statements.add(statement);
                }
                if (!StringUtils.isEmpty(conditionStatement)) {
                    codes.add("(" + conditionStatement + ")");
                }

            } else if (condition.getOpear() == OPEAR.BETWEEN) {
                Object[] objs = (Object[]) condition.getValue();
                codes.add("(" + "doc['" + condition.getName() + "'].value >= " + objs[0] + " && doc['"
                        + condition.getName() + "'].value <=" + objs[1] + ")");
            } else {
                SQLExpr nameExpr = condition.getNameExpr();
                SQLExpr valueExpr = condition.getValueExpr();
                if(valueExpr instanceof SQLNullExpr) {
                    codes.add("(" + "doc['" + nameExpr.toString() + "']" + ".empty)");
                } else {
                    codes.add("(" + Util.getScriptValueWithQuote(nameExpr, "'") + condition.getOpertatorSymbol() + Util.getScriptValueWithQuote(valueExpr, "'") + ")");
                }
            }
        } else {
            for (Where subWhere : where.getWheres()) {
                List<String> subStatements = new ArrayList<String>();
                List<String> subCodes = new ArrayList<String>();
                explainWhere(subStatements, subCodes, subWhere);

                statements.add(Joiner.on(" ").join(subStatements));
                String relation = subWhere.getConn().name().equals("AND") ? "&&" : "||";
                codes.add(Joiner.on(relation).join(subCodes));
            }
        }
    }

}
