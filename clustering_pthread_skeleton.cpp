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

int global_num_vs, global_num_es, *global_nbr_offs = nullptr, *global_nbrs = nullptr;

float global_epsilon;
int global_mu;
bool *pivots = nullptr;
int *num_sim_nbrs = nullptr;
int **sim_nbrs = nullptr;

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
    
    // hints: union-found, set prioriteis, drawer principles
    // two stages:
    // stage 1: find the cores/pivots
    int local_num_vs = global_num_vs/all->num_threads + 1;
    int start = all->my_rank*local_num_vs;
    int end = (all->my_rank + 1)*local_num_vs;
    if (end > global_num_vs)
        end = global_num_vs;
    
    for (int i = start; i < end; ++i) {
        int *left_start = &global_nbrs[global_nbr_offs[i]];
        int *left_end = &global_nbrs[global_nbr_offs[i + 1]];
        int left_size = left_end - left_start;
        
        sim_nbrs[i] = new int[left_size];
        // loop over all neighbors of i
        for (int *j = left_start; j < left_end; j++) {
            int nbr_id = *j;
            
            int *right_start = &global_nbrs[global_nbr_offs[nbr_id]];
            int *right_end = &global_nbrs[global_nbr_offs[nbr_id + 1]];
            int right_size = right_end - right_start;
            
            // compute the similarity
            int num_com_nbrs = get_num_com_nbrs(left_start, left_end, right_start, right_end);
            
            float sim = (num_com_nbrs + 2) / std::sqrt((left_size + 1.0) * (right_size + 1.0));
            
            if (sim > global_epsilon) {
                sim_nbrs[i][num_sim_nbrs[i]] = nbr_id;
                num_sim_nbrs[i]++;
            }
        }
        if (num_sim_nbrs[i] > global_mu) pivots[i] = true;
    }
        
    // stage 2: expand the clusters from cores/pivots by DFS or BFS
    
    return 0;
}

int *scan(float epsilon, int mu, int num_threads, int num_vs, int num_es, int *nbr_offs, int *nbrs){
    // assign these values to the global variables:
    global_num_vs = num_vs;
    global_num_es = num_es;
    global_nbr_offs = nbr_offs;
    global_nbrs = nbrs;
    global_epsilon = epsilon;
    global_mu = mu;
    
    pivots = (bool*)malloc(num_vs*sizeof(bool));
    num_sim_nbrs = (int*)malloc(num_vs*sizeof(int));
    sim_nbrs = (int**)malloc(num_vs*sizeof(int));
    
    long thread;
    pthread_t* thread_handles = (pthread_t*) malloc(num_threads*sizeof(pthread_t));
    int *cluster_result = new int[num_vs];
    
    for (thread = 0; thread < num_threads; thread++)
    pthread_create(&thread_handles[thread], NULL, parallel, (void *) new AllThings(num_threads, thread));
    
    for (thread=0; thread < num_threads; thread++)
    pthread_join(thread_handles[thread], NULL);
    
#if DEBUG
    if (global_num_vs <= 50) {
        for (int i = 0; i < global_num_vs; ++i) {
            std::cout << pivots[i] << " ";
        }
        cout << endl;
    }
#endif
    return cluster_result;
}



