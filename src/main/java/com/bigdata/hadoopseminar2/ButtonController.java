package com.bigdata.hadoopseminar2;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.ResponseBody;

@Controller
public class ButtonController {

    private final SendLogs sendLogs;

    public ButtonController(SendLogs sendLogs) {
        this.sendLogs = sendLogs;
    }

    @GetMapping("/")
    public String indexPage() {
        return "index.html";
    }

    @GetMapping("/send-accesslogs")
    @ResponseBody
    public String sendAccessLogs() {
        boolean success = sendLogs.sendAccessLogs();

        if (success) {
            return "Access logs sent successfully.";
        } else {
            String errorMessage = sendLogs.getErrorMessage();
            return "Sending access logs failed. Error message: " + "   " + errorMessage ;
        }
    }

    @GetMapping("/send-activitylogs")
    @ResponseBody
    public String sendActivityLogs() {

        boolean success = sendLogs.sendActivityLogs();
        return success ? "Activity logs sent successfully." : "Sending activity logs failed.";
    }

    @GetMapping("/create-activitylog-db")
    @ResponseBody
    public String createActivityLogDB() {

        boolean success = sendLogs.createActivityLogDB();
        return success ? "Creating Activity Log DB successfully." : "Creating Activity Log DB failed.";
    }
}
