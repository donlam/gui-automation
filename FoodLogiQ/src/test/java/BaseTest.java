import org.openqa.selenium.WebDriver;
import org.openqa.selenium.chrome.ChromeDriver;
import org.openqa.selenium.chrome.ChromeOptions;
import org.testng.annotations.AfterSuite;
import org.testng.annotations.BeforeSuite;

import java.util.concurrent.TimeUnit;

public class BaseTest {

    private WebDriver driver;

    @BeforeSuite
    public void beforeSuite() {
        System.setProperty("webdriver.chrome.driver","/Users/donlam/selenium/chromedriver");
        driver = new ChromeDriver();
        driver.manage().timeouts().implicitlyWait(10, TimeUnit.SECONDS);
    }

    @AfterSuite
    public void afterSuite() {
        if(null != driver) {
            driver.close();
            driver.quit();
        }
    }

    public WebDriver getDriver() {
        return driver;
    }
}