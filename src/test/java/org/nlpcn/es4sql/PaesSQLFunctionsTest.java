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
 * This group of unit test is for PINGAN
 */
public class PaesSQLFunctionsTest {

    private static SqlParser parser;

    @BeforeClass
    public static void init() {
        parser = new SqlParser();
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
    public void operationNestInAggFunction() throws Exception {
        String query = "SELECT balance, age, min(age*3+1) as ret" +
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
        String query = "SELECT balance+((2-2)*2/2+10)*10 as ret" +
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
        String query = "SELECT count(gender) " +
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
    public void numberFunctions() throws Exception {
        String query = "SELECT balance, " +
                "floor(balance) as floor_balance, " +
                "ceil(balance) as ceil_balance, " +
                "round(balance) as round_balance, " +
                "abs(balance) as abs_balance, " +
                "sqrt(balance) as sqrt_balance, " +
                "log(balance) as log_balance, " +
                "log10(balance) as log10_balance, " +
                "pow(balance,2) as pow_balance " +
                "FROM " + TestsConstants.PAES_TEST_INDEX + "/account order by account_number limit 1000 offset 0 ";
        printQuery(query);
        CSVResult csvResult = getCsvResult(false, query);
        print(csvResult);
    }

    private CSVResult getCsvResult(boolean flat, String query) throws SqlParseException, SQLFeatureNotSupportedException, Exception, CsvExtractorException {
        return getCsvResult(flat, query, false, false,false);
    }

    private CSVResult getCsvResult(boolean flat, String query, boolean includeScore, boolean includeType,boolean includeId) throws SqlParseException, SQLFeatureNotSupportedException, Exception, CsvExtractorException {
        SearchDao searchDao = MainTestSuite.getSearchDao() != null ? MainTestSuite.getSearchDao() : getSearchDao();
        QueryAction queryAction = searchDao.explain(query);
        Object execution = QueryActionElasticExecutor.executeAnyAction(searchDao.getClient(), queryAction);
        return new CSVResultsExtractor(includeScore, includeType, includeId).extractResults(execution, flat, ",");
    }

    private SearchDao getSearchDao() throws UnknownHostException {
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
        SearchDao searchDao = MainTestSuite.getSearchDao() != null ? MainTestSuite.getSearchDao() : getSearchDao();
        System.out.println(searchDao.explain(query).explain().explain());
    }
}
