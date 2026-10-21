producer_consumer: producer_consumer.cpp
	g++ -Wall -pthread producer_consumer.cpp -o producer_consumer

clean:
	rm -f producer_consumer
