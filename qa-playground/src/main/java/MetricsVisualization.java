import javafx.application.Application;
import javafx.application.Platform;
import javafx.scene.Scene;
import javafx.scene.chart.CategoryAxis;
import javafx.scene.chart.LineChart;
import javafx.scene.chart.NumberAxis;
import javafx.scene.chart.XYChart;
import javafx.stage.Stage;

import java.util.*;

import java.util.Map;

/**
 * Created by donlam on 9/1/18.
 * Use JFX to help visualize some of our test metrics data
 *
 * @todo add ability to launch/update this app while main QAHelper is running through different iterations of menu selection
 * stack-overflow appears to be in abundant with suggestions on solving that problem -- need to figure out how to use that here
 * https://stackoverflow.com/questions/15653118/javafx-launch-another-application
 * https://stackoverflow.com/questions/24320014/how-to-call-launch-more-than-once-in-java
 */
public class MetricsVisualization extends Application implements Observer {

    public static Map<String, Object> beforeResponseTime_SmallIncUpdate;
    public static Map<String, Object> afterResponseTime_SmallIncUpdate;
    public static Map<String, Object> beforeResponseTime_SingleUpdate;
    public static Map<String, Object> afterResponseTime_SingleUpdate;

    public static String chartTitle = "Voldemort Update Response Time";
    public static int parentRunCount = 0;
    public static int chartUpdateCount = 0;


    @Override
    public void start(Stage primaryStage) throws Exception {

        Platform.setImplicitExit(false);

        primaryStage.setTitle("Response Time - Before & After Updates");
        final CategoryAxis xAxis = new CategoryAxis();
        final NumberAxis yAxis = new NumberAxis();
        xAxis.setLabel("X-Iteration");
        final LineChart<String, Number> lineChart = new LineChart<String, Number>(xAxis, yAxis);

        lineChart.setTitle(chartTitle);


        Scene scene = new Scene(lineChart, 1200, 1000);
          /*
        wrap update inside a thread
         */
        Thread thread = new Thread(new Runnable() {
            @Override
            public void run() {
                Runnable updater = new Runnable() {
                    int countOfTitleChange = 0;

                    @Override
                    public void run() {
                        lineChart.setTitle(chartTitle);
                        countOfTitleChange++;
                        if (chartUpdateCount < parentRunCount) {
                            updateSeries();
                            chartUpdateCount++;
                            LineChart<String, Number> chart = new LineChart<String, Number>(xAxis, yAxis);
                            chart.setTitle(chartTitle);
                            chart.getData().addAll(updatableSeries1, updatableSeries2, updatableSeries3, updatableSeries4);
                            Scene updatedScene = new Scene(chart, 1200, 1000);
                            primaryStage.setScene(updatedScene);
                        }
                        if (countOfTitleChange == 1000) Platform.exit();
                    }
                };

                while (true) {
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException ex) {
                    }

                    // UI update is run on the Application thread
                    Platform.runLater(updater);
                }
            }

        });
        // don't let thread prevent JVM shutdown
        thread.setDaemon(true);
        thread.start();
        /*
        end of update experiment
         */
        primaryStage.setScene(scene);
        primaryStage.show();
    }

    private XYChart.Series makeNewSeries(Map<String, Object> responseTimeMap, String name) {
        XYChart.Series series = new XYChart.Series();
        series.setName(name);

        Map<Integer, Integer> sortedMap = cheapSorter(responseTimeMap);

        for (Integer skeys : sortedMap.keySet()) {
            series.getData().add(new XYChart.Data(String.valueOf(skeys), sortedMap.get(skeys)));
        }
        return series;
    }

    public MetricsVisualization() {
    }

    public void launchChart() {
        launch();
    }

    //our response time data is given in <String,Double> map and it is not sorted -- here is a cheapstake way of sorting
    public Map<Integer, Integer> cheapSorter(Map<String, Object> responseTime) {
        Map<Integer, Integer> sortedMap = new HashMap<>();

        for (String key : responseTime.keySet()) {
            sortedMap.put(Integer.parseInt(key), (Integer) responseTime.get(key));
        }
        List<Integer> sortedKeys = new ArrayList(sortedMap.keySet());
        Collections.sort(sortedKeys);

        sortedMap = new HashMap<>(); //clean it up

        for (Integer i : sortedKeys) {
            sortedMap.put(i, (Integer) responseTime.get(String.valueOf(i)));
        }
        return sortedMap;
    }

    @Override
    public void update(Observable observable, Object o) {
        String message = (String) o;
        updateChartData(observable, message);
    }

    public void updateChartData(Observable observable, String data) {
        chartTitle = data;
        System.out.printf("the parent app now runs %d and child app now updated %d %n", parentRunCount, chartUpdateCount);
    }

    public void updateChartRunCount() {
        chartUpdateCount++;
    }

    public void updateSeries() {
        updatableSeries1 = makeNewSeries(this.beforeResponseTime_SmallIncUpdate, "Small Inc. Unique ItemList Updates (Before)");
        updatableSeries2 = makeNewSeries(this.afterResponseTime_SmallIncUpdate, "Small Inc. Unique ItemList Updates (After)");
        updatableSeries3 = makeNewSeries(this.beforeResponseTime_SingleUpdate, "Single Unique ItemList Updates (Before)");
        updatableSeries4 = makeNewSeries(this.afterResponseTime_SingleUpdate, "Single Unique ItemList Updates (After)");
    }

    public static XYChart.Series updatableSeries1;
    public static XYChart.Series updatableSeries2;
    public static XYChart.Series updatableSeries3;
    public static XYChart.Series updatableSeries4;
}
