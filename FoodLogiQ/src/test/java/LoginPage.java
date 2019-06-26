import org.openqa.selenium.By;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.WebElement;

/**
 * Created by donlam on 6/25/19.
 * Everything that could be done on the LoginPage
 */
public class LoginPage extends BasePage {

    private String passwordInputLocator = "#passwordFormGroup input[name~=password]";
    private String emailInputLocator = "input[type~=email]";
    private String loginButtonLocator = "#loginButton";
    private String alertBoxLocator = ".alert-danger";


    public LoginPage(WebDriver driver) {
        super(driver);
        waitForElementToAppear(By.cssSelector(emailInputLocator));
    }

    public void setEmailAddress(String emailAddress){
        driver.findElement(By.cssSelector(emailInputLocator)).sendKeys(emailAddress);
    }

    public void setEmailPassword(String password){
        driver.findElement(By.cssSelector(passwordInputLocator)).sendKeys(password);
    }

    public void clickLoginButton(){
        driver.findElement(By.cssSelector(loginButtonLocator)).click();
    }

    public String loginAlert(){
        String alertMsg = null;
        WebElement alert = driver.findElement(By.cssSelector(alertBoxLocator));
        if(alert.isDisplayed()) {
            alertMsg = alert.getText();
        }
        return alertMsg;
    }



}
