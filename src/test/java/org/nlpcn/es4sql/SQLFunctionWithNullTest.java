package org.nlpcn.es4sql;

import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugin.nlpcn.QueryActionElasticExecutor;
import org.elasticsearch.plugin.nlpcn.executors.CSVResult;
import org.elasticsearch.plugin.nlpcn.executors.CSVResultsExtractor;
import org.elasticsearch.plugin.nlpcn.executors.CsvExtractorException;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.nlpcn.es4sql.exception.SqlParseException;
import org.nlpcn.es4sql.parse.SqlParser;
import org.nlpcn.es4sql.query.QueryAction;

import java.net.UnknownHostException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.List;

/**
 * This group of unit test is for test whole sql functions with null value
 *  and new support features:
 *  1) number functions
 *  2) string functions
 *  3) date functions:
 *  4) case when
 *  5) pipeline aggregation
 *  6) inner functions
 */
public class SQLFunctionWithNullTest {

    private static SqlParser parser;

    @BeforeClass
    public static void init() {
        parser = new SqlParser();
    }

    @Test
    public void debug() throws Exception {
        String query = "select field(createTime), field(date_basic), field(date_custom)" +
                " FROM " + TestsConstants.TEST_INDEX + "/account_with_null";
        printQuery(query);
        CSVResult csvResult = getCsvResult(false, query);
        print(csvResult);
    }

    @Test
    public void condition() throws Exception {
        String query = "select *" +
                " FROM " + TestsConstants.TEST_INDEX + "/account_with_null" +
                " WHERE date_part('year', createTime) = date_part('year', now())" +
                " OR ( date_diff('year', createTime, now()) = 2 AND date_part('month', createTime) = 3 )";
        printQuery(query);
        CSVResult csvResult = getCsvResult(false, query);
        print(csvResult);
    }

    @Test
    public void conditionWithIn() throws Exception {
        String query = "select *" +
                " FROM " + TestsConstants.TEST_INDEX + "/account_with_null" +
                " WHERE createTime in (to_date('2017-01-01'), to_date('2016-01-01'))";
        printQuery(query);
        CSVResult csvResult = getCsvResult(false, query);
        print(csvResult);
    }

    @Test
    public void conditionWithBetween() throws Exception {
        String query = "select *" +
                " FROM " + TestsConstants.TEST_INDEX + "/account_with_null" +
                " WHERE createTime BETWEEN to_date('2016_01_01', 'yyyy_MM-dd') AND to_date('2018_01_01', 'yyyy_MM-dd')";
        printQuery(query);
        CSVResult csvResult = getCsvResult(false, query);
        print(csvResult);
    }

    @Test
    public void conditionWithFunction() throws Exception {
        String query = "SELECT *" +
                " FROM " + TestsConstants.TEST_INDEX + "/account_with_null" +
                " WHERE createTime > to_date('2016-01-01', 'yyyy-MM-dd') AND createTime <= to_date('2018/01/01', 'yyyy/MM/dd')";
        printQuery(query);
        CSVResult csvResult = getCsvResult(false, query);
        print(csvResult);
    }

    @Test
    public void date_histogram() throws Exception {
        String query = "SELECT count(age), min(age), max(age), avg(age)" +
                " FROM " + TestsConstants.TEST_INDEX + "/account_with_null" +
                " GROUP BY date_histogram(alias='createTime',field='createTime','interval'='180d')";
        printQuery(query);
        CSVResult csvResult = getCsvResult(false, query);
        print(csvResult);
    }

    @Test
    public void date_range() throws Exception {
        String query = "SELECT count(age), min(age), max(age), avg(age)" +
                " FROM " + TestsConstants.TEST_INDEX + "/account_with_null" +
                " GROUP BY date_range(alias='createTime', field='createTime', '2014-05-1','2016-05-1','now-1y','now', 'now+1y')";
        printQuery(query);
        CSVResult csvResult = getCsvResult(false, query);
        print(csvResult);
    }

    @Test
    public void date_range_with_format() throws Exception {
        String query = "SELECT count(age), min(age), max(age), avg(age)" +
                " FROM " + TestsConstants.TEST_INDEX + "/account_with_null" +
                " GROUP BY date_range(alias='createTime', field='createTime', format='yyyy/MM/dd' ,'2014/05/1','2016/05/1','now-1y','now', 'now+1y')";
        printQuery(query);
        CSVResult csvResult = getCsvResult(false, query);
        print(csvResult);
    }

    @Test
    public void aggWithNestFunc() throws Exception {
        String query = "SELECT sum(pow(age, 2)) as sum_pow " +
                "FROM " + TestsConstants.TEST_INDEX + "/account_with_null group by gender";
        printQuery(query);
        CSVResult csvResult = getCsvResult(false, query);
        print(csvResult);
    }

    @Test
    public void aggWithCaseWhen() throws Exception {
        String query = "SELECT sum(CASE when (gender) = 'M' THEN 1 ELSE 0 END) as sum_1, sum(CASE when (gender) = 'M' THEN 0 ELSE 1 END) as sum_2 " +
                "FROM " + TestsConstants.TEST_INDEX + "/account_with_null";
        printQuery(query);
        CSVResult csvResult = getCsvResult(false, query);
        print(csvResult);
    }

    @Test
    public void aggWithOperationBefore() throws Exception {
        String query = "SELECT min(age * 3 + 1) as min" +
                " FROM " + TestsConstants.TEST_INDEX + "/account_with_null";
        printQuery(query);
        CSVResult csvResult = getCsvResult(false, query);
        print(csvResult);
    }

    @Test
    public void aggWithOperationAfter() throws Exception {
        String query = "SELECT count(age), sum(age), sum(age) / count(age), avg(age) " +
                "FROM " + TestsConstants.TEST_INDEX + "/account_with_null group by gender";
        printQuery(query);
        CSVResult csvResult = getCsvResult(false, query);
        print(csvResult);
    }

    @Test
    public void aggWithOperationAfter2() throws Exception {
        String query = "SELECT min(abs(age)) + max(age) + avg(age) as s " +
                "FROM " + TestsConstants.TEST_INDEX + "/account_with_null group by gender";
        printQuery(query);
        CSVResult csvResult = getCsvResult(false, query);
        print(csvResult);
    }

    @Test
    public void aggWithOperationAfter3() throws Exception {
        String query = "SELECT round(sqrt(min(abs(age)) + max(age)), 3) as s " +
                "FROM " + TestsConstants.TEST_INDEX + "/account_with_null group by gender";
        printQuery(query);
        CSVResult csvResult = getCsvResult(false, query);
        print(csvResult);
    }

    @Test
    public void caseWhen() throws Exception {
        String query = "SELECT gender, CASE when (gender) = 'M' THEN '男' ELSE '女' END as sex " +
                "FROM " + TestsConstants.TEST_INDEX + "/account_with_null";
        printQuery(query);
        CSVResult csvResult = getCsvResult(false, query);
        print(csvResult);
    }

    @Test
    public void caseWhenNest() throws Exception {
        String query = "SELECT gender, concat(CASE WHEN gender = 'M' THEN 'Mr' ELSE 'Mis' END, '.', firstname) as name " +
                "FROM " + TestsConstants.TEST_INDEX + "/account_with_null";
        printQuery(query);
        CSVResult csvResult = getCsvResult(false, query);
        print(csvResult);
    }

    @Test
    public void caseWhenWithFunction() throws Exception {
        String query = "SELECT date_custom as T1,\n" +
                "       concat(date_part('year', (createTime)), '-', case\n" +
                "         when date_part('month', (createTime)) < 10 then\n" +
                "          concat('0', '', date_part('month', (createTime)))\n" +
                "         else\n" +
                "          concat(date_part('month', (createTime)), '')\n" +
                "       end, '-', case\n" +
                "         when date_part('day', (createTime)) < 10 then\n" +
                "          concat('0', '', date_part('day', (createTime)))\n" +
                "         else\n" +
                "          concat(date_part('day', (createTime)), '')\n" +
                "       end) as T2\n" +
                " FROM " + TestsConstants.TEST_INDEX + "/account_with_null\n" +
                " WHERE (date_custom < to_date(('2016-12-30'), 'yyyy-MM-dd')) limit 15 offset 0\n";

        printQuery(query);
        CSVResult csvResult = getCsvResult(false, query);
        print(csvResult);
    }

    @Test
    public void toNumber() throws Exception {
        String query = "SELECT " +
                "to_number(255) num, to_number('255') num_to_str, to_number('255a') bad_str, " +
                "1000 + '255' as add_str, 1000 + to_number('255') as add_num " +
                "FROM " + TestsConstants.TEST_INDEX + "/account_with_null";
        printQuery(query);
        CSVResult csvResult = getCsvResult(false, query);
        print(csvResult);
    }

    @Test
    public void todayFunctions() throws Exception {
        String query = "SELECT " +
                "now() now, today() today, " +
                "to_char(now(), 'yyyy-MM-dd HH:mm:ss.SSS') as now2, " +
                "to_char(today(), 'yyyy-MM-dd HH:mm:ss.SSS') as today2 " +
                "FROM " + TestsConstants.TEST_INDEX + "/account_with_null";
        printQuery(query);
        CSVResult csvResult = getCsvResult(false, query);
        print(csvResult);
    }

    @Test
    public void dateFunctions() throws Exception {
        String query = "SELECT " +
                "year(createTime) as year, " +
                "month(createTime) as month, " +
                "week(createTime) as week, " +
                "day(createTime) as day, " +
                "quarter(createTime) as quarter, " +
                "createTime FROM " + TestsConstants.TEST_INDEX + "/account_with_null";
        printQuery(query);
        CSVResult csvResult = getCsvResult(false, query);
        print(csvResult);
    }

    @Test
    public void toDateToChar() throws Exception {
        String query = "SELECT date_basic, to_date(date_basic, 'yyyyMMdd') date_basic2, to_char(to_date(date_basic, 'yyyyMMdd'), 'yyyy/MM/dd') date_basic3 " +
                " FROM " + TestsConstants.TEST_INDEX + "/account_with_null";
        printQuery(query);
        CSVResult csvResult = getCsvResult(false, query);
        print(csvResult);
    }

    @Test
    public void toChar() throws  Exception {
        String query = "SELECT createTime, date_custom, date_basic, to_char(createTime, 'yyyy_MM_dd') createTime2, to_char(date_custom, 'yyyy_MM_dd') as date_custom2, to_char(date_basic, 'yyyy_MM_dd') as date_basic2" +
                " FROM " + TestsConstants.TEST_INDEX + "/account_with_null";
        printQuery(query);
        CSVResult csvResult = getCsvResult(false, query);
        print(csvResult);
    }

    @Test
    public void toDate() throws Exception {
        String query = "SELECT createTime, date_custom, date_basic, to_date(createTime, 'yyyy-MM-dd HH:mm:ss.SSS') createTime_2, to_date(date_custom, 'yyyy/MM/dd') date_custom2, to_date(date_basic, 'yyyyMMdd') date_basic2" +
                " FROM " + TestsConstants.TEST_INDEX + "/account_with_null";
        printQuery(query);
        CSVResult csvResult = getCsvResult(false, query);
        print(csvResult);
    }

    @Test
    public void dateAdd() throws Exception {
        String query = "SELECT createTime, " +
                "to_char(date_add('day', -1, createTime)) add_day, " +
                "to_char(date_add('week', 1, createTime)) add_week, " +
                "to_char(date_add('month', 1, createTime)) add_month, " +
                "to_char(date_add('quarter', -1, createTime)) add_quarter, " +
                "to_char(date_add('year', 1, createTime)) add_year " +
                " FROM " + TestsConstants.TEST_INDEX + "/account_with_null";
        printQuery(query);
        CSVResult csvResult = getCsvResult(false, query);
        print(csvResult);
    }

    @Test
    public void dateDiff() throws Exception {
        String query = "SELECT createTime, " +
                "date_diff('day', to_date('2015-03-17 13:27:33.953'), createTime) diff_day, " +
                "date_diff('week', to_date('2015-03-17 13:27:33.953'), createTime) diff_week, " +
                "date_diff('month', to_date('2015-03-17 13:27:33.953'), createTime) diff_month, " +
                "date_diff('quarter', to_date('2015-03-17 13:27:33.953'), createTime) quarter, " +
                "date_diff('year', to_date('2015-03-17 13:27:33.953'), createTime) diff_year " +
                " FROM " + TestsConstants.TEST_INDEX + "/account_with_null";
        printQuery(query);
        CSVResult csvResult = getCsvResult(false, query);
        print(csvResult);
    }

    @Test
    public void dateTrunc() throws Exception {
        String query = "SELECT createTime, " +
                "to_char(date_trunc('second',  createTime)) trunc_second, " +
                "to_char(date_trunc('minute',  createTime)) trunc_minute, " +
                "to_char(date_trunc('hour',  createTime)) trunc_hour, " +
                "to_char(date_trunc('day',  createTime)) trunc_day, " +
                "to_char(date_trunc('month',  createTime)) trunc_month, " +
                "to_char(date_trunc('year',  createTime)) trunc_year " +
                " FROM " + TestsConstants.TEST_INDEX + "/account_with_null";
        printQuery(query);
        CSVResult csvResult = getCsvResult(false, query);
        print(csvResult);
    }

    @Test
    public void datePart() throws Exception {
        String query = "SELECT createTime, " +
                "date_part('millisecond',  createTime) millisecond, " +
                "date_part('second',  createTime) second, " +
                "date_part('minute',  createTime) minute, " +
                "date_part('hour',  createTime) hour, " +
                "date_part('day',  createTime) day, " +
                "date_part('month',  createTime) month, " +
                "date_part('year',  createTime) year " +
                " FROM " + TestsConstants.TEST_INDEX + "/account_with_null";
        printQuery(query);
        CSVResult csvResult = getCsvResult(false, query);
        print(csvResult);
    }

    @Test
    public void selectAllFunctions() throws Exception {
        String query = "SELECT *" +
                " FROM " + TestsConstants.TEST_INDEX + "/account_with_null";
        printQuery(query);
        CSVResult csvResult = getCsvResult(false, query);
        print(csvResult);
    }

    @Test
    public void nestFunction() throws  Exception {
        String query = "SELECT balance, abs(ln(log(2, ln(balance)))) as log_balance " +
                " FROM " + TestsConstants.TEST_INDEX + "/account_with_null";
        printQuery(query);
        CSVResult csvResult = getCsvResult(false, query);
        print(csvResult);
    }

    @Test
    public void operationNestInFunction() throws Exception {
        String query = "SELECT balance, sqrt((2-2)*2/2+10)*10+10000 as ret" +
                " FROM " + TestsConstants.TEST_INDEX + "/account_with_null";
        printQuery(query);
        CSVResult csvResult = getCsvResult(false, query);
        print(csvResult);
    }

    @Test
    public void operation() throws Exception {
        String query = "SELECT balance, balance+((2-2)*2/2+10)*10 as ret" +
                " FROM " + TestsConstants.TEST_INDEX + "/account_with_null";
        printQuery(query);
        CSVResult csvResult = getCsvResult(false, query);
        print(csvResult);
    }

    @Test
    public void operationBetweenColumn() throws Exception {
        String query = "SELECT (account_with_null_number + age) * 2" +
                " FROM " + TestsConstants.TEST_INDEX + "/account_with_null";
        printQuery(query);
        CSVResult csvResult = getCsvResult(false, query);
        print(csvResult);
    }

    @Test
    public void concat() throws Exception {
        String query = "SELECT firstname, lastname, " +
                "concat('++', concat_ws('.', city, firstname, lastname), '=', gender, '--')," +
                "gender FROM " + TestsConstants.TEST_INDEX + "/account_with_null";
        printQuery(query);
        CSVResult csvResult = getCsvResult(false, query);
        print(csvResult);
    }

    @Test
    public void substring() throws Exception {
        String query = "SELECT address," +
                "substring(address, 0) as substr_0," +
                "substring(address, 5) as substr_5, " +
                "substring(address, 5, 5) as substr_5_5," +
                "substring(address, -3) as substr_-3, " +
                "substring(address, -3, 1) as substr_-3_1" +
                " FROM " + TestsConstants.TEST_INDEX + "/account_with_null";
        printQuery(query);
        CSVResult csvResult = getCsvResult(false, query);
        print(csvResult);
    }

    @Test
    public void instr() throws Exception {
        String query = "SELECT email," +
                "instr(email, '@') pos, " +
                "instr(email, '.com') pos_com, " +
                "instr(email, '@', 14) from_index, " +
                "instr(email, '@', 1, 2) as second_search" +
                " FROM " + TestsConstants.TEST_INDEX + "/account_with_null";
        printQuery(query);
        CSVResult csvResult = getCsvResult(false, query);
        print(csvResult);
    }

    @Test
    public void replace() throws Exception {
        String query = "SELECT email," +
                "replace(replace(email, '.com', '.net'), '@', '#') " +
                " FROM " + TestsConstants.TEST_INDEX + "/account_with_null";
        printQuery(query);
        CSVResult csvResult = getCsvResult(false, query);
        print(csvResult);
    }

    @Test
    public void countAllFunctions() throws Exception {
        String query = "SELECT count(*) " +
                " FROM " + TestsConstants.TEST_INDEX + "/account_with_null";
        printQuery(query);
        CSVResult csvResult = getCsvResult(false, query);
        print(csvResult);
    }

    @Test
    public void countFunctions() throws Exception {
        String query = "SELECT count(lastname) " +
                "FROM " + TestsConstants.TEST_INDEX + "/account_with_null";
        printQuery(query);
        CSVResult csvResult = getCsvResult(false, query);
        print(csvResult);
    }

    @Test
    public void countDistinctFunctions() throws Exception {
        String query = "SELECT count(DISTINCT gender) " +
                "FROM " + TestsConstants.TEST_INDEX + "/account_with_null";
        printQuery(query);
        CSVResult csvResult = getCsvResult(false, query);
        print(csvResult);
    }

    @Test
    public void extendedStatsFunctions() throws Exception {
        String query = "SELECT extended_stats(age) " +
                "FROM " + TestsConstants.TEST_INDEX + "/account_with_null " +
                "where balance is not null group by gender";
        printQuery(query);
        CSVResult csvResult = getCsvResult(false, query);
        print(csvResult);
    }

    @Test
    public void statsFunctions() throws Exception {
        String query = "SELECT stats(age) " +
                "FROM " + TestsConstants.TEST_INDEX + "/account_with_null " +
                "where balance is not null group by gender";
        printQuery(query);
        CSVResult csvResult = getCsvResult(false, query);
        print(csvResult);
    }

    @Test
    public void aggFunctions() throws Exception {
        String query = "SELECT count(*), " +
                "sum(balance) as sum_balance, " +
                "min(balance) as min_balance, " +
                "max(balance) as max_balance, " +
                "avg(balance) as avg_balance " +
                "FROM " + TestsConstants.TEST_INDEX + "/account_with_null";
        printQuery(query);
        CSVResult csvResult = getCsvResult(false, query);
        print(csvResult);
    }

    @Test
    public void roundFunction() throws Exception {
        String query = "SELECT " +
                "round(balance) as round, " +
                "round(balance,1) as round_1, " +
                "round(balance,2) as round_2, " +
                "round(balance,3) as round_3, " +
                "round(balance,4) as round_4, " +
                "round(balance,5) as round_5, " +
                "balance " +
                "FROM " + TestsConstants.TEST_INDEX + "/account_with_null order by account_number limit 1000 offset 0 ";
        printQuery(query);
        CSVResult csvResult = getCsvResult(false, query);
        print(csvResult);
    }

    @Test
    public void numberFunctions() throws Exception {
        String query = "SELECT " +
                "floor(balance) as floor_balance, " +
                "ceil(balance) as ceil_balance, " +
                "round(balance) as round_balance, " +
                "abs(balance) as abs_balance, " +
                "sqrt(balance) as sqrt_balance, " +
                "ln(balance) as ln_balance, " +
                "log(balance) as log_balance, " +
                "log(3,balance) as log_3_balance, " +
                "log(10,balance) as log_10_balance, " +
                "log10(balance) as log10_balance, " +
                "pow(balance,2) as pow_balance, " +
                "balance " +
                "FROM " + TestsConstants.TEST_INDEX + "/account_with_null order by account_number limit 1000 offset 0 ";
        printQuery(query);
        CSVResult csvResult = getCsvResult(false, query);
        print(csvResult);
    }

    private CSVResult getCsvResult(boolean flat, String query) throws SqlParseException, SQLFeatureNotSupportedException, Exception, CsvExtractorException {
        return getCsvResult(flat, query, false, false,false);
    }

    private CSVResult getCsvResult(boolean flat, String query, boolean includeScore, boolean includeType,boolean includeId) throws SqlParseException, SQLFeatureNotSupportedException, Exception, CsvExtractorException {
        SearchDao searchDao = getSearchDao();
        QueryAction queryAction = searchDao.explain(query);
        Object execution = QueryActionElasticExecutor.executeAnyAction(searchDao.getClient(), queryAction);
        return new CSVResultsExtractor(includeScore, includeType, includeId).extractResults(execution, flat, ",");
    }

    private SearchDao getSearchDao() throws UnknownHostException {
        if (MainTestSuite.getSearchDao() != null) {
            return MainTestSuite.getSearchDao();
        }

        Settings settings = Settings.builder().put("client.transport.ignore_cluster_name", true).build();
        Client client = new PreBuiltTransportClient(settings).
                addTransportAddress(MainTestSuite.getTransportAddress());
        return new SearchDao(client);
    }

    private void print(CSVResult csvResult) {
        List<String> headers = csvResult.getHeaders();
        List<String> contents = csvResult.getLines();
        for (String h: headers) {
            System.out.print(h);
            System.out.print("\t");
        }
        System.out.println();
        for (String c: contents) {
            System.out.print(c);
            System.out.println();
        }
    }

    private void printQuery(String query) throws Exception {
        System.out.println(query);
        System.out.println(getSearchDao().explain(query).explain().explain());
    }
}
