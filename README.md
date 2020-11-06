# IoT Delivery Vehicles Experiment (IoTDV)

Consider a scenario set in the not too distant future where self-driving vehicles are commonplace among normal traffic. In such a scenario, vehicles would need to be monitored for anomalous behaviors which could endanger the general public. 

## Streaming Data
We created a simulation which mapped the streets and intersections of an area of central Berlin, Germany. In this area a number of delivery vehicles were generated travelling along various routes and providing an update message every 1 second. Each update message contained the vehicle\_ID, vehicle\_type, geolocation, speed, direction, and event\_time. The number of delivery vehicles generated at any one point in time is kept fairly stable with allowances made to include some measure of positive and negative variance. Messages are submitted to the Kafka cluster at a rate of 500,000 messages per second to await processing.

## Streaming Job
We created a DSP job based on the simulated delivery vehicle data. The job consisted of the following streaming operations: read an event from Kafka; deserialize the JSON string; filter update messages not within a certain radius of a designated observation geo-point where delivery vehicles are of a particular type, i.e. self-driving; take a 10 second window where all update messages are of the same vehicle ID and calculate the vehicles average speed; generate a notification for vehicles which have exceeded the speed limit; enrich notification with vehicle type information from data stored in system memory and write it back out to Kafka.
