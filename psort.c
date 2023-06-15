#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <pthread.h>
#include <sys/time.h>

typedef struct records
{
    int key;
    int record[24];
} key_value;
struct merge_sort_s
{
    key_value *start;
    key_value *end;
};
pthread_mutex_t lock = PTHREAD_MUTEX_INITIALIZER;
// Merges two sorted sub-arrays start to mid and mid+1 to end
void merge(key_value *start, key_value *mid, key_value *end)
{
    // pthread_mutex_lock(&lock);
    int left_size = mid - start + 1;
    int right_size = end - mid;
    key_value *left = (key_value *)malloc(left_size * sizeof(key_value));
    key_value *right = (key_value *)malloc(right_size * sizeof(key_value));
    memcpy(left, start, left_size * sizeof(key_value));
    memcpy(right, mid + 1, right_size * sizeof(key_value));

    int i = 0, j = 0, k = 0;
    while (i < left_size && j < right_size)
    {
        if (left[i].key <= right[j].key)
        {
            start[k] = left[i];
            memcpy(&start[k].record, &left[i].record, sizeof(left[i].record));
            i++;
        }
        else
        {
            start[k] = right[j];
            memcpy(&start[k].record, &right[j].record, sizeof(right[j].record));
            j++;
        }
        k++;
    }

    while (i < left_size)
    {
        start[k] = left[i];
        memcpy(&start[k].record, &left[i].record, sizeof(left[i].record));
        i++;
        k++;
    }

    while (j < right_size)
    {
        start[k] = right[j];
        memcpy(&start[k].record, &right[j].record, sizeof(right[j].record));
        j++;
        k++;
    }

    free(left);
    free(right);
    // pthread_mutex_unlock(&lock);
}

// Recursively sort the array using Merge Sort
void merge_sort(key_value *start, key_value *end)
{
    if (start < end)
    {
        key_value *mid = start + (end - start) / 2;
        merge_sort(start, mid);
        merge_sort(mid + 1, end);
        merge(start, mid, end);
    }
}

void *thread_func(void *arg)
{
    struct merge_sort_s *params = arg;
    key_value *start = params->start;
    key_value *end = params->end;
    merge_sort(start, end - 1);
    key_value *sorted = (key_value *)malloc((end - start) * sizeof(key_value));
    memcpy(sorted, start, (end - start) * sizeof(key_value));
    pthread_exit(NULL);
}

// stdin = 0, stdout = 1, stderr = 2
int main(int argc, char *argv[])
{

    if (argc != 4)
    {
        printf("Incorrect use of arguments! Input File, Output File, Num Threads\n");
        return 1;
    }

    FILE *inputFile = fopen(argv[1], "r");
    if (inputFile == NULL)
    {
        fprintf(stderr, "Error: failed to open input file %s\n", argv[1]);
        return 1;
    }

    // get number of records
    fseek(inputFile, 0, SEEK_END);
    int numRecords = ftell(inputFile) / 100;
    rewind(inputFile);
    // dictionary: {key: record}
    key_value *records = malloc(numRecords * sizeof(key_value));
    key_value *tmp = records;

    // each record into keys called records
    while (fread(records, 100, 1, inputFile))
    {
        records++;
    }
    key_value *tmp_end = records;
    records = tmp;
    // for (int i = 0; i < numRecords; i++)
    // {
    //     printf("BEFORE %d\n", records[i].key);
    // }
    int numThreads = atoi(argv[3]);
    if (numThreads <= 0)
    {
        fprintf(stderr, "Error: invalid number of threads %s\n", argv[3]);
        return 1;
    }

    fclose(inputFile);

    struct timeval start, end;
    gettimeofday(&start, NULL);
    // Create thread
    if (numThreads >= numRecords)
        numThreads = numRecords - 1;
    pthread_t *threads;
    threads = malloc(numThreads * sizeof(pthread_t));
    struct merge_sort_s *params;
    params = malloc(numThreads * sizeof(struct merge_sort_s));

    if (numThreads >= numRecords)
        numThreads = numRecords - 1;

    for (int i = 0; i < numThreads; i++)
    {
        params[i].start = records + i * (numRecords / numThreads);
        if (i == numThreads - 1)
        {
            params[i].end = records + numRecords; // start == end
        }
        else
        {
            params[i].end = records + (i + 1) * (numRecords / numThreads);
        }
        pthread_create(&threads[i], NULL, thread_func, &params[i]);
    }
    // Wait for threads to finish
    for (int i = 0; i < numThreads; i++)
        pthread_join(threads[i], NULL);

    int mergesize = numRecords/numThreads;

    for(int i = 1; i < numThreads; i++){
    //printf("hehe\n");
    if(i==numThreads-1){
        merge(tmp, tmp+mergesize*i, tmp_end);
    }
    else{
    merge(tmp, tmp+mergesize* i, tmp+(i + 1) * mergesize);
    }
}

    gettimeofday(&end, NULL);

    double execution_time = (double)(end.tv_sec - start.tv_sec) + (double)(end.tv_usec - start.tv_usec) / 1000000;
    printf("Execution time: %f seconds\n", execution_time);

    for (int i = 0; i < numRecords; i++)
    {
        // to print full record, include another loop to go through array
        printf("AFTER %d\n", records[i].key);
    }

    FILE *outputFile = fopen(argv[2], "w");
    if (outputFile == NULL)
    {
        fprintf(stderr, "Error: failed to open output file %s\n", argv[2]);
        return 1;
    }

    for (int i = 0; i < numRecords; i++)
    {
        fwrite(&records[i], 100, 1, outputFile);

    }

    fclose(outputFile);
    free(records);
    free(threads);
    free(params);

    return 0;
}
