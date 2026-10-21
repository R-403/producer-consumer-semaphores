#include <iostream>
#include <pthread.h>
#include <semaphore.h>
#include <unistd.h>
#include <sys/time.h>
#include <cstdlib>

using namespace std;

int *buffer;
int Size = 1;
int numC = 1;

int in = 0;
int out = 0;

sem_t emptyS;
sem_t fullS;
sem_t buffMutex;
sem_t coutMutex;


sem_t barrierS;
sem_t barrierMutex;
int barrierC = 0;

long prodwaitsec = 0;
long prodwaitUsec = 0;

timeval startT;


void addTime(timeval start, timeval end, long &sec, long &usec){
    long s = end.tv_sec - start.tv_sec;
    long us = end.tv_usec - start.tv_usec;

    if(us < 0){
        s--;
        us += 1000000;
    }

    sec += s;
    usec += us;

    if(usec >= 1000000){
        sec += usec / 1000000;
        usec %= 1000000;
    }
}



void barrier(int n){
    sem_wait(&barrierMutex);

    barrierC++;

    if(barrierC == n){
        for(int i = 0; i < n; i++){
            sem_post(&barrierS);
        }
    }
    sem_post(&barrierMutex);
    sem_wait(&barrierS);
}

void *producer(void *arg){
    int v;

    while(cin >> v){
        timeval before, after;

        gettimeofday(&before, NULL);
        sem_wait(&emptyS);
        gettimeofday(&after, NULL);

        addTime(before, after, prodwaitsec, prodwaitUsec);

        sem_wait(&buffMutex);

        buffer[in] = v;
        in = (in + 1) % Size;

        sem_post(&buffMutex);
        sem_post(&fullS);

        if(v < 0){
            break;
        }
    }

    // Send termination signals to the remaining consumers
    for(int i = 1; i < numC; i++){
        timeval before, after;

        gettimeofday(&before, NULL);
        sem_wait(&emptyS);
        gettimeofday(&after, NULL);

        addTime(before, after, prodwaitsec, prodwaitUsec);

        sem_wait(&buffMutex);

        buffer[in] = -1;
        in = (in + 1) % Size;

        sem_post(&buffMutex);
        sem_post(&fullS);
    }

    return NULL;
}

void *consumer(void *arg){
    int id = *(int *)arg;
    int total = 0;

    while(true){

        sem_wait(&fullS);
        sem_wait(&buffMutex);
        int v = buffer[out];
        out = (out + 1) %Size;
        sem_post(&buffMutex);
        sem_post(&emptyS);
        if(v < 0){
            break;
        }
        total += v;
        sleep(1);
    }

    barrier(numC);
    sem_wait(&coutMutex);
    cout << "Consumer " << id << " total is " << total << endl;
    timeval finishT;
    gettimeofday(&finishT, NULL);
    long finishSec = finishT.tv_sec - startT.tv_sec;
    long finishUsec = finishT.tv_usec - startT.tv_usec;
    if (finishUsec < 0) {
        finishSec--;
        finishUsec += 1000000;
    }
    cout << "Consumer " << id << " completion time is " << finishSec << " seconds and " << finishUsec << " useconds" << endl;
    sem_post(&coutMutex);

    return NULL;
}

int main(int argc, char *argv[]){
    if(argc >= 2){
        Size = atoi(argv[1]);
    }
    if(argc >= 3){
        numC = atoi(argv[2]);
    }
    if(Size < 1 || Size > 32){
        cerr << "Buffer size must be between 1 and 32" << endl;
        return 1;
    }
    if(numC < 1 || numC > 10){
        cerr << "Number of consumers must be between 1 and 10" << endl;
        return 1; 
    }
    buffer = new int[Size];

    sem_init(&emptyS, 0, Size);
    sem_init(&fullS, 0, 0);
    sem_init(&buffMutex, 0, 1);
    sem_init(&barrierS, 0, 0);
    sem_init(&barrierMutex, 0, 1);
    sem_init(&coutMutex, 0, 1);

    pthread_t producerThread;
    pthread_t consumerThreads[numC];

    int consumerIDs[numC];
    gettimeofday(&startT, NULL);
    pthread_create(&producerThread, NULL, producer, NULL);

    for(int i = 0; i< numC; i++){
        consumerIDs[i] = i;

        pthread_create(
            &consumerThreads[i],
            NULL,
            consumer, 
            &consumerIDs[i]
        );
    }

    pthread_join(producerThread, NULL);

    for(int i = 0; i < numC; i++){
        pthread_join(consumerThreads[i], NULL);
    }

    timeval endTime;
    gettimeofday(&endTime, NULL);
    long programSec = endTime.tv_sec - startT.tv_sec;
    long programUsec = endTime.tv_usec - startT.tv_usec;

    if(programUsec < 0){
        programSec--;
        programUsec += 1000000;
    }

    cout << "Total producer wait time is " << prodwaitsec << " seconds and " << prodwaitUsec << " useconds" << endl;

    cout << "Program time is " << programSec << " seconds and " << programUsec << " useconds" << endl;

    sem_destroy(&emptyS);
    sem_destroy(&fullS);
    sem_destroy(&buffMutex);
    sem_destroy(&barrierS);
    sem_destroy(&barrierMutex);
    sem_destroy(&coutMutex);
    
    delete[] buffer;
    return 0;         
}
