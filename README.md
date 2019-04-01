# nescala-etl
Code from my presentation on ETL at Northeast Scala 4/2/2019

Framework package is full representation of what is used internally
Utils package are stubs of utilities that were already existing
Model package is a stub of our internal domain model
Demo package is an example of what a functioning app extending the framework would look like

In the demo data directory there is a sample input and output file for the MovePositionsJob

# Extensions if you want to play around with the code
1. Change the MovePositionsJob so that each portfolio gets its own separate csv file output
2. Update to take the path to the input file and the output path as additional command line args
3. Add a new mode that would take trades in format (portfolioId, securityId, trade) where the trade can be positive or negative
    and output all non-zero trades in format (portfolioName, securityName, Direction, trade) where direction = Long if trade was positive and direction = Short if trade was negative

