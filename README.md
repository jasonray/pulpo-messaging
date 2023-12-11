# pulpo-messaging

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

# Concepts
* `job`: single delayed processing task
* `job request`: request to `pulpo-messaging` to perform a single job
* `producer`: system requesting job
* `handler`: the set of Python code to fulfil the job request. This is where the extensibility of the system comes into play: you create your own handlers with your logic and `pulpo-messaging` will delegate to your registered handlers.
* `queue`: `pulpo-messaging` uses a queue for holding the job requests which need to be processed.
* `queue_adapter`: the queue adapter integrates `pulpo-messaging` with a specific queue implementation.  Currently, there are `queue_adapters` for a light-weight, file based implementation (`file_queue_adapter`) and a robust implementation utilizing `beanstalkd` (`beanstalk_queue_adapter`)

# API

## Message
A message is the implementation of a `job request`.  Message is implemented a dictionary, with root elements of `id`, `header`, and `body`.
| Field        | Parent | Specified by               | Description |
| ------------ | ------ | -------------------------- | ----------- |
| id           | root   | `queue_adapter` on enqueue | unique identifier |
| header       | root   | various                    | stores content used for routing / flow control |
| request_type | header | producer                   | defines the job that is being requests (i.e. send_email, print_shipping_label, etc) |
| expiration   | header | producer                   | Specifies the latest that a job may be processed. This is provided as an absolute date/time. |
| delay        | header | producer                   | Specifies the earlier that a job may be processed.  Prior to this date/time, the message will not be dequeue. This is provided as an absolute date/time. |
| priority     | header | producer                   | Specifies the order by which jobs will be processed. 0 is the highest priority, the lowest priority being approx 4M. Negative numbers are treated as 0
| attempts     | header | `queue_adapter`            | Tracks the number of (failed) attempts on a given message.  This is likely only used by the file_queue_adapter.
| body         | root   | producer                   | defines the content the the handler will need to execute the job.  This is stored as key-value pairs.  For example, for a job that sends an email, the message could have a body with key-value pairs of "to", "subject", "body". 
| payload      | body   | producer                   | for simplistic jobs, payload acts as a single value for job request.  |

## Pulpo
### Config
* `shutdown_after_number_of_empty_iterations` (int): pulpo looks for new jobs to process by iterating, checking the queue_adapter for new jobs.  If there are multiple iterations with no messages (as specified by this setting), pulpo will shutdown (with the expectation that it would be automatically restarted).
* `sleep_duration` (int): specifies the number of seconds to pause for each iteration when there are no messages available.
* `queue_adapter_type(self)` (str): specifies the implementation of the queue_adapter.  Specify `FileQueueAdapter` to use the file based queue, or `BeanstalkdQueueAdapter` to use the beanstalkd based queue.
* `enable_output_buffering(self)`
* `enable_banner`
* `banner_name`
* `banner_font`

### Publish
* `publish(message: Message) -> Message` => enqueues the job request on to the queue
  * `message.request_type`: specifies the job that is being requested.  This value is looked up against the configuration of the registry to determine the handler
  * `priority` (optional): specifies the order by which jobs will be processed.  
  * `expiration` (optional): specifies the latest that a job may be processed.
  * `delay` (optional): specifies the earlier that a job may be processed.
  * publish returns a `Message`, which is the requested message with a populated message id

### initialize

