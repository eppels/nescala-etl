package demo

/*
In reality, the constructor args here are often connection-related (e.g. database adapters)
and there are many repositories all implemented as traits that depend on the constructor args to implement the interfaces
these repositories often reference each other using the cake pattern

In many cases, at the overall domain library level an abstract interface for data wil two implementations:
1. the tow truck implementation, which shows how to get the data from the true source
2. The domain implementation, which pulls from the tow truck destination - all downstream applications mix in these implementations
 */
class DemoTowTruckDomain()
  extends DemoEntityRepository
