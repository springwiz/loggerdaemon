# loggerdaemon
Logger sidecar

The Package implements a daemon that is intended to be run as a logging sidecar in a microservice. It listens for TCP Connections on localhost at a specific port. It then extracts the data from the incoming data stream and pushes in into the pre-configured middleware.It decouples the log collection fuctionality from the platform used to build the microservice. Its main features are:
  * Decouples log collection from the microservice.
  * Supports multiple transports for log collection i.e. RabbitMQ, Kafka.
  * Levarages the multiprocessing/concurrency capabilities of the Go language.
  * Employes bigcache https://github.com/allegro/bigcache project to build the reliability and scalibility into the software.
  * Could be extended to support further more type of Log Repository.
  * Restarts itself automatically once it crashes.
  
# install
With a correctly configured Go toolchain:

go get -u github.com/springwiz/loggerdaemon
  
