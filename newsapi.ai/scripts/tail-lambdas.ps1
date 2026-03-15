$lambdas = @(
    @{ Title = "Article Collector"; LogGroup = "/aws/lambda/newsapi-ai_collector" },
    @{ Title = "Article Loader";    LogGroup = "/aws/lambda/newsapi-ai_loader" },
    @{ Title = "Event Collector";   LogGroup = "/aws/lambda/event_collector" },
    @{ Title = "Event Loader";      LogGroup = "/aws/lambda/event-loader" }
)

foreach ($lambda in $lambdas) {
    Start-Process powershell -ArgumentList @(
        "-NoExit",
        "-Command",
        "& { `$host.UI.RawUI.WindowTitle = '$($lambda.Title)'; aws logs tail $($lambda.LogGroup) --follow --region us-east-2 }"
    )
}