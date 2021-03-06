A servlet filter implementation which provides quality-of-service by dynamically partitioning the worker threadpool across multiple (configurable) request types such that a certain capacity of thread-pool is reserved for requests for a given type. If the number of requests exceed this configured maximum, they are suspended and placed on a queue for future execution. Whenever a thread is available (and number of concurrent requests for a type are less than the configured maximum), the request is again resumed automatically.

Note this implementation is inspired by [Jetty QoSFilter](http://www.eclipse.org/jetty/documentation/current/qos-filter.html). The major difference between these two is that the QoSFilter does not attempt to guarantee resource isolation e.g. a large burst of requests for a given type can potentially take up the entire worker threadpool.

This project includes a sample servlet and a jmeter configuration file for testing. Following steps are required to execute this example,

1. Clone this project
2. Run following command on one terminal
   mvn jetty:run
3. Run following command on another terminal
   mvn verify -P test
