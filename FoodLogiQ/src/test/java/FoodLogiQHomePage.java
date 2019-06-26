import org.openqa.selenium.By;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.WebElement;

import java.util.List;

/**
 * Created by donlam on 6/24/19.
 */
public class FoodLogiQHomePage extends BasePage {


    private String loginButtonLocator = ".mega-menu-item-11";
    private String europeanCookiesAlert = "#hs-eu-confirmation-button";


    /*open up the foodlogiq home page and click on the cookie confirmation */
    public FoodLogiQHomePage(WebDriver driver) {
        super(driver);
        driver.get("http://foodlogiq.com");
        WebElement euroCookies = driver.findElement(By.cssSelector(europeanCookiesAlert));
        if(euroCookies.isDisplayed()) euroCookies.click();
    }


    /* click the login button and switch to the next tab after the login button has been clicked */
    public LoginPage clickLoginButton(){
        driver.findElement(By.cssSelector(loginButtonLocator)).click();
        for(String winHandle: driver.getWindowHandles()){
            driver.switchTo().window(winHandle);
        }
        return new LoginPage(this.driver);
    }


}
