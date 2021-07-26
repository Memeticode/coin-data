# coin-data
Code to populate and analyze crypto database
#homecooking

## Next Steps:
1. Sync database w/ latest coinbase data
    a. C# to poll latest updates from get historic product rates
2. Automate genereration of model input metrics
    a. Sql data flow
3. Deploy trained ML.NET model
    a. Generate predictions and write to DB
    b. Re-train as new data is loaded / available
4. Portfolio trading implementation
    a. Hook up coinbase to model predictons
    b. Logic to buy / sell / hold based on:
        1. Model predictions
        2. Portfolio positions


