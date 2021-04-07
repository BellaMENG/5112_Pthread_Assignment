//
//  minimum_dup.cpp
//  
//
//  Created by MENG Zihan on 4/7/21.
//

#include <stdio.h>
#include <cassert>
#include <pthread.h>
#include <iostream>
#include "clustering.h"

using namespace std;

int num_p;
bool* visited;

struct AllThings{
    int num_threads;
    int my_rank;
    
    AllThings(int inum_threads, int imy_rank){
        num_threads = inum_threads;
        my_rank = imy_rank;
    };
};

void *threadWork(void* allthings) {
    AllThings *all = (AllThings*) allthings;
    int local_n = num_p/all->num_threads + 1;
    int start = all->my_rank*local_n;
    int end = (all->my_rank + 1)*loccal_n;
    
    if (end > num_p)
        end = num_p;
    for (int i = start; i < end; ++i) {
        if (visited[i])
            continue;
        visited[i] = true;
        dfs(i, i, num_sim_nbrs, sim_nbrs, visited, pivots, parent);
    }
}

int main(int argc, char** argv) {
//    std::string file_path = argv[1];
    float epsilon = std::stof(argv[1]);
    int mu = std::stoi(argv[2]);
    int num_threads = std::atoi(argv[3]);
    
    long thread;
    pthread_t* thread_handles = (pthread_t*) malloc(num_threads*sizeof(pthread_t));
    pthread_rwlock_init(&rwlock, NULL);
    pthread_mutex_init(&union_mutex, NULL);

    for (thread = 0; thread < num_threads; thread++) {
        pthread_create(&thread_handles[thread], NULL, threadWork, (void *) new AllThings(num_threads, thread));
    }
    
    for (thread = 0; thread < num_threads; thread++) {
        pthread_join(thread_handles[thread], NULL);
    }
    
    pthread_rwlock_destroy(&rwlock);
    pthread_mutex_destroy(&union_mutex);
    free(thread_handles);

    return 0;
}
