package org.nlpcn.es4sql;

import com.alibaba.druid.util.StringUtils;
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
 * This group of unit test is for PINGAN
 */
public class PaesSQLFunctionsTest {

    private static SqlParser parser;

    @BeforeClass
    public static void init() {
        parser = new SqlParser();
    }

    @Test
    public void debug() throws Exception {
//        String query = "SELECT log(2, CASE  when ((sex1)) <= (0) THEN null ELSE ((sex1)) END) as C2117434807 from custom " +
//                "WHERE cardhold_city_name IS NOT NULL AND cust_no is not null   limit 0 , 51";
        String query = "SELECT insert_time FROM paes/account WHERE date_custom BETWEEN '2014/08/18' AND '2017/08/21' LIMIT 3";
        printQuery(query);
        CSVResult csvResult = getCsvResult(false, query);
        print(csvResult);
    }

    @Test
    public void aggWithNestFunc() throws Exception {
        String query = "SELECT sum(pow(age, 2)) as sum_pow " +
                "FROM " + TestsConstants.PAES_TEST_INDEX + "/account";
        printQuery(query);
        CSVResult csvResult = getCsvResult(false, query);
        print(csvResult);
    }

    @Test
    public void aggWithCaseWhen() throws Exception {
        String query = "SELECT sum(CASE when (gender) = 'M' THEN 1 ELSE 0 END) as sum_1, sum(CASE when (gender) = 'M' THEN 0 ELSE 1 END) as sum_2 " +
                "FROM " + TestsConstants.PAES_TEST_INDEX + "/account";
        printQuery(query);
        CSVResult csvResult = getCsvResult(false, query);
        print(csvResult);
    }

    @Test
    public void aggWithOperationBefore() throws Exception {
        String query = "SELECT min(age * 3 + 1) as min" +
                " FROM " + TestsConstants.PAES_TEST_INDEX + "/account";
        printQuery(query);
        CSVResult csvResult = getCsvResult(false, query);
        print(csvResult);
    }

    @Test
    public void aggWithOperationAfter() throws Exception {
        String query = "SELECT count(age), sum(age), sum(age) / count(age), avg(age) " +
                "FROM " + TestsConstants.PAES_TEST_INDEX + "/account group by gender";
        printQuery(query);
        CSVResult csvResult = getCsvResult(false, query);
        print(csvResult);
    }

    @Test
    public void aggWithOperationAfter2() throws Exception {
        String query = "SELECT min(abs(age)) + max(age) + avg(age) as s " +
                "FROM " + TestsConstants.PAES_TEST_INDEX + "/account group by gender";
        printQuery(query);
        CSVResult csvResult = getCsvResult(false, query);
        print(csvResult);
    }

    @Test
    public void aggWithOperationAfter3() throws Exception {
        String query = "SELECT round(sqrt(min(abs(age)) + max(age)), 3) as s " +
                "FROM " + TestsConstants.PAES_TEST_INDEX + "/account group by gender";
        printQuery(query);
        CSVResult csvResult = getCsvResult(false, query);
        print(csvResult);
    }

    @Test
    public void caseWhen() throws Exception {
        String query = "SELECT gender, CASE when (gender) = 'M' THEN '男' ELSE '女' END as sex " +
                "FROM " + TestsConstants.PAES_TEST_INDEX + "/account";
        printQuery(query);
        CSVResult csvResult = getCsvResult(false, query);
        print(csvResult);
    }

    @Test
    public void caseWhenNest() throws Exception {
        String query = "SELECT gender, concat(CASE WHEN gender = 'M' THEN 'Mr' ELSE 'Mis' END, '.', firstname) as name " +
                "FROM " + TestsConstants.PAES_TEST_INDEX + "/account";
        printQuery(query);
        CSVResult csvResult = getCsvResult(false, query);
        print(csvResult);
    }

    @Test
    public void nest() throws Exception {
        String query = "SELECT * from (SELECT * from paes/account WHERE gender = 'F') T";
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
                "FROM " + TestsConstants.PAES_TEST_INDEX + "/account";
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
                "createTime FROM " + TestsConstants.PAES_TEST_INDEX + "/account";
        printQuery(query);
        CSVResult csvResult = getCsvResult(false, query);
        print(csvResult);
    }

    @Test
    public void toDateToChar() throws Exception {
        String query = "SELECT date_basic, to_date(date_basic, 'yyyyMMdd') date_basic2, to_char(to_date(date_basic, 'yyyyMMdd'), 'yyyy/MM/dd') date_basic3 " +
                " FROM " + TestsConstants.PAES_TEST_INDEX + "/account";
        printQuery(query);
        CSVResult csvResult = getCsvResult(false, query);
        print(csvResult);
    }

    @Test
    public void toChar() throws  Exception {
        String query = "SELECT createTime, date_custom, date_basic, to_char(createTime, 'yyyy_MM_dd') createTime2, to_char(date_custom, 'yyyy_MM_dd') as date_custom2, to_char(date_basic, 'yyyy_MM_dd') as date_basic2" +
                " FROM " + TestsConstants.PAES_TEST_INDEX + "/account";
        printQuery(query);
        CSVResult csvResult = getCsvResult(false, query);
        print(csvResult);
    }

    @Test
    public void toDate() throws Exception {
        String query = "SELECT createTime, date_custom, date_basic, to_date(createTime, 'yyyy-MM-dd HH:mm:ss.SSS') createTime_2, to_date(date_custom, 'yyyy/MM/dd') date_custom2, to_date(date_basic, 'yyyyMMdd') date_basic2" +
                " FROM " + TestsConstants.PAES_TEST_INDEX + "/account";
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
                " FROM " + TestsConstants.PAES_TEST_INDEX + "/account";
        printQuery(query);
        CSVResult csvResult = getCsvResult(false, query);
        print(csvResult);
    }

    @Test
    public void dateDiff() throws Exception {
        String query = "SELECT createTime, " +
                "date_diff('day', to_date('2015-03-16 21:27:33.953'), createTime) diff_day, " +
                "date_diff('week', to_date('2015-03-16 21:27:33.953'), createTime) diff_week, " +
                "date_diff('month', to_date('2015-03-16 21:27:33.953'), createTime) diff_month, " +
                "date_diff('quarter', to_date('2015-03-16 21:27:33.953'), createTime) diff_quterr, " +
                "date_diff('year', to_date('2015-03-16 21:27:33.953'), createTime) diff_year " +
                " FROM " + TestsConstants.PAES_TEST_INDEX + "/account";
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
                " FROM " + TestsConstants.PAES_TEST_INDEX + "/account";
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
                " FROM " + TestsConstants.PAES_TEST_INDEX + "/account";
        printQuery(query);
        CSVResult csvResult = getCsvResult(false, query);
        print(csvResult);
    }

    @Test
    public void selectAllFunctions() throws Exception {
        String query = "SELECT *" +
                " FROM " + TestsConstants.PAES_TEST_INDEX + "/account";
        printQuery(query);
        CSVResult csvResult = getCsvResult(false, query);
        print(csvResult);
    }

    @Test
    public void nestFunction() throws  Exception {
        String query = "SELECT balance, abs(ln(log(2, ln(balance)))) as log_balance " +
                " FROM " + TestsConstants.PAES_TEST_INDEX + "/account";
        printQuery(query);
        CSVResult csvResult = getCsvResult(false, query);
        print(csvResult);
    }

    @Test
    public void operationNestInFunction() throws Exception {
        String query = "SELECT balance, sqrt((2-2)*2/2+10)*10+10000 as ret" +
                " FROM " + TestsConstants.PAES_TEST_INDEX + "/account";
        printQuery(query);
        CSVResult csvResult = getCsvResult(false, query);
        print(csvResult);
    }

    @Test
    public void operation() throws Exception {
        String query = "SELECT balance, balance+((2-2)*2/2+10)*10 as ret" +
                " FROM " + TestsConstants.PAES_TEST_INDEX + "/account";
        printQuery(query);
        CSVResult csvResult = getCsvResult(false, query);
        print(csvResult);
    }

    @Test
    public void operationBetweenColumn() throws Exception {
        String query = "SELECT (account_number + age) * 2" +
                " FROM " + TestsConstants.PAES_TEST_INDEX + "/account";
        printQuery(query);
        CSVResult csvResult = getCsvResult(false, query);
        print(csvResult);
    }

    @Test
    public void concat() throws Exception {
        String query = "SELECT firstname, lastname, " +
                "concat('++', concat_ws('.', city, firstname, lastname), '=', gender, '--')," +
                "gender FROM " + TestsConstants.PAES_TEST_INDEX + "/account";
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
                " FROM " + TestsConstants.PAES_TEST_INDEX + "/account";
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
                " FROM " + TestsConstants.PAES_TEST_INDEX + "/account";
        printQuery(query);
        CSVResult csvResult = getCsvResult(false, query);
        print(csvResult);
    }

    @Test
    public void replace() throws Exception {
        String query = "SELECT email," +
                "replace(replace(email, '.com', '.net'), '@', '#') " +
                " FROM " + TestsConstants.PAES_TEST_INDEX + "/account";
        printQuery(query);
        CSVResult csvResult = getCsvResult(false, query);
        print(csvResult);
    }

    @Test
    public void countAllFunctions() throws Exception {
        String query = "SELECT count(*) " +
                " FROM " + TestsConstants.PAES_TEST_INDEX + "/account";
        printQuery(query);
        CSVResult csvResult = getCsvResult(false, query);
        print(csvResult);
    }

    @Test
    public void countFunctions() throws Exception {
        String query = "SELECT count(lastname) " +
                "FROM " + TestsConstants.PAES_TEST_INDEX + "/account";
        printQuery(query);
        CSVResult csvResult = getCsvResult(false, query);
        print(csvResult);
    }

    @Test
    public void countDistinctFunctions() throws Exception {
        String query = "SELECT count(DISTINCT gender) " +
                "FROM " + TestsConstants.PAES_TEST_INDEX + "/account";
        printQuery(query);
        CSVResult csvResult = getCsvResult(false, query);
        print(csvResult);
    }

    @Test
    public void extendedStatsFunctions() throws Exception {
        String query = "SELECT extended_stats(age) " +
                "FROM " + TestsConstants.PAES_TEST_INDEX + "/account " +
                "where balance is not null group by gender";
        printQuery(query);
        CSVResult csvResult = getCsvResult(false, query);
        print(csvResult);
    }

    @Test
    public void statsFunctions() throws Exception {
        String query = "SELECT stats(age) " +
                "FROM " + TestsConstants.PAES_TEST_INDEX + "/account " +
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
                "FROM " + TestsConstants.PAES_TEST_INDEX + "/account group by gender";
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
                "FROM " + TestsConstants.PAES_TEST_INDEX + "/account order by account_number limit 1000 offset 0 ";
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
                "FROM " + TestsConstants.PAES_TEST_INDEX + "/account order by account_number limit 1000 offset 0 ";
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
        if (PaesMainTestSuite.getSearchDao() != null) {
            return PaesMainTestSuite.getSearchDao();
        }

        Settings settings = Settings.builder().put("client.transport.ignore_cluster_name", true).build();
        Client client = new PreBuiltTransportClient(settings).
                addTransportAddress(PaesMainTestSuite.getTransportAddress());
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
