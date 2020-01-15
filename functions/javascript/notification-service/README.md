# Notification Service
This service sends a notification to users. 
It is built to support multiple channels of notification but only supports email for now.

It accepts only `POST` http requests.
The body of the http request is expected to contain the following parameters:

```{
    "notificationType": <string>,
    "subject": <string>,
    "message": <string>,
    "contacts": <array>
}
```

For example: 

```
{
    "notificationType": "EMAIL",
    "subject": "Fraud Alert Important",
    "message": "user has been flagged",
    "contacts": ["bolu@plutosave.com"]
}
```

On receiving the above payload, `notificaion-service` sends an email to 
`bolu@plutosave.com` with the subject and message body.
 