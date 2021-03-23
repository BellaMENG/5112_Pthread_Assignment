/*
 * Name: MENG Zihan
 * Student id: 20412027
 * ITSC email: zmengaa@connect.ust.hk
 *
 * Please only change this file and do not change any other files.
 * Feel free to change/add any helper functions.
 *
 * COMPILE: g++ -lstdc++ -std=c++11 -lpthread clustering_pthread_skeleton.cpp -main.cpp -o pthread
 * RUN:     ./pthread <path> <epsilon> <mu> <num_threads>
 */

#include <pthread.h>
#include "clustering.h"

struct AllThings{
    int num_threads;
    int my_rank;

    AllThings(int inum_threads, int imy_rank){
        num_threads = inum_threads;
        my_rank = imy_rank;
    };
};

void *parallel(void* allthings){
    AllThings *all = (AllThings *) allthings;

//    printf("Hello from %d of %d\n", all->my_rank, all->num_threads);

    return 0;
}

int *scan(float epsilon, int mu, int num_threads, int num_vs, int num_es, int *nbr_offs, int *nbrs){
    long thread;
    pthread_t* thread_handles = (pthread_t*) malloc(num_threads*sizeof(pthread_t));
    int *cluster_result = new int[num_vs];

    for (thread=0; thread < num_threads; thread++)
        pthread_create(&thread_handles[thread], NULL, parallel, (void *) new AllThings(
                num_threads, thread));
    for (thread=0; thread < num_threads; thread++)
        pthread_join(thread_handles[thread], NULL);

    return cluster_result;
}



