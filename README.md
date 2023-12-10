# Background

`pulpo-messaging` is a lightweight framework for handling distributed delayed job processing. This can be useful for any background jobs that you need within your system.

Consider the following scenarios:
* After a customer purchases an item from your site, your system needs to process the order as a background job
* When a patient has a pending medication order, your system needs to perform medication/allergy checks
* As new metric data becomes available, your system needs to periodically create reports derived from raw data

Here are a list of general use cases:
* Offloading Heavy Tasks from Web Requests: In web applications, long-running tasks like sending emails, generating reports, or image processing can be offloaded to Beanstalkd. This helps in responding to user requests quickly, improving user experience by handling time-consuming tasks asynchronously.
* Task Scheduling and Delayed Execution: Beanstalkd can be used for tasks that need to be executed after a certain delay. For example, sending reminder emails, scheduled notifications, or delayed updates can be queued and executed at the specified time.
* Workload Distribution: Distributing tasks among multiple worker processes or servers is a common use case. Beanstalkd can help in load balancing by evenly distributing jobs across various workers, enhancing the efficiency and scalability of the system.
* Handling Spikes in Traffic: During periods of high traffic, Beanstalkd can be used to manage and queue incoming jobs, ensuring that the system remains responsive and stable by controlling the rate at which jobs are processed.
* Job Prioritization: In systems where certain tasks have higher priority, Beanstalkd's priority queuing feature allows for critical jobs to be processed first, ensuring important tasks are completed in a timely manner.
* Decoupling System Components: Beanstalkd can act as a buffer and a communication medium between different parts of a system, helping in decoupling components. This separation of concerns makes systems easier to manage and scale.
* Batch Processing: Accumulating data and processing it in batches (like aggregating logs, analytics data, or processing bulk email sends) can be effectively managed using Beanstalkd.
* Microservices Communication: In a microservices architecture, Beanstalkd can be used for inter-service communication, where services can asynchronously push and pull messages or tasks to and from the queue.
* Reliability and Fault Tolerance: For applications requiring high reliability, Beanstalkd's job reservation and time-to-run features ensure that jobs are not lost and are retried in case of worker failures.
* Background Data Processing: Performing data-intensive operations like data mining, file parsing, or ETL (Extract, Transform, Load) tasks in the background.


# Features
Each of these uses follows a workflow of: 
* request job (publish)
* delegate job to a software component that can handle the request based upon job characteristics. 

`pulpo-messaging` manages this workflow:
* Defines a flexible `message` model which includes: `headers` for routing / control, `body` for processing
* Provides a `publish` API to request a job, which in turn stores within an internal queue.  Publishing a message support a variety of featurees including specifying the request type, delaying a message, or specifying expiration.
* Defines a `queue_adapter` interface, to allow for a variety of queue implementation
* Provides a lightweight, file based `queue_adapter` implementation for low volume use cases
* Provider a robust `queue_adapter` implementation based on `beanstalkd`
* Defines a `handler` interface, to be implemented by integrator (you!) specific to your system logic
* Handles result logic, including deleting the request on success and resubmitted the request on a transient exception

## Roles
* Architect can map job type to handlers
* Developers can implement new handlers
* Producers can request jobs to be processed
* Producers can specify expiration of job requests
* Producers can specify delay (earliest date/time to execute job) of job requests
* `pulpo-messaging` processes requests based on priority order, and routes job requests to handler
* `pulpo-messaging` automatically retries on transient exceptions, and buries jobs on fatal exceptions

