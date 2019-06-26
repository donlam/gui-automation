
/* modeled after page object framework tutorial on https://www.testingexcellence.com/page-object-framework-java-webdriver/ */
import org.testng.Assert;
import org.testng.annotations.Test;

public class FoodLogiQLoginTest extends BaseTest{

    @Test
    public void homepageInvalidLoginTest(){

        String expectedInvalidLoginMessage = "The email address and password entered do not match our records. Please confirm your credentials and try again.";

        FoodLogiQHomePage homePage = new FoodLogiQHomePage(getDriver());
        LoginPage loginPage = homePage.clickLoginButton();
        loginPage.setEmailAddress("tester@foodlogiq.com");
        loginPage.setEmailPassword("invalidPassword");
        loginPage.clickLoginButton();

        Assert.assertEquals(loginPage.loginAlert(),expectedInvalidLoginMessage);
    }



}